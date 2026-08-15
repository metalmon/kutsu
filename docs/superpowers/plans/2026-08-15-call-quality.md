# Call quality (prefill pacer + metrics + abort) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop downlink audio dropouts by giving the pacer an adaptive prefill jitter buffer (ported from the proven voice-cloud client), measure per-call audio quality (underruns / starved-ms / inter-chunk gap), surface it in the record + metrics, and abort calls whose audio is clearly broken.

**Architecture:** `pace::Downlink` gains a prefill state machine + underrun counters (pure logic). The bridge arms an `expecting` flag from Gemini turn events, tracks the max inter-chunk gap, and publishes live counters into a shared `Arc<QualityShared>`. The engine snapshots that on a 1 s tick → `CallRecord.quality`, aborts on a configurable underrun threshold, and exposes cumulative counters via `/metrics`.

**Tech Stack:** Rust (edition 2024), tokio, existing hand-rolled DSP (`Downsampler`). No new crates.

**Spec:** `docs/superpowers/specs/2026-08-15-call-quality-design.md`

## Global Constraints

- Build/test MUST use `cargo test --features vendor-openssl -p kutsu <t>` / `cargo build --features vendor-openssl`. Clean build, no new warnings.
- Ported tuning (do not change without cause): prebuffer **140 ms**, resume **60 ms**; abort default **40** cumulative underruns (`0` = never). 24 kHz → 24 samples/ms; a frame is 480 in-samples (20 ms) → 160 out-samples (8 kHz).
- The uplink stays transparent/never-gated (out of scope here). Only the downlink pacer changes.
- TDD: failing test → run(fail) → minimal impl → run(pass) → commit.

## File structure

- `src/bridge/pace.rs` (modify) — prefill state machine + counters.
- `src/bridge/mod.rs` (modify) — `QualityShared`, `BridgePorts` fields, expecting/gap wiring, publish counters.
- `src/state.rs` (modify) — `CallQuality` + `CallRecord.quality` + `CallStore::set_quality`.
- `src/config.rs` (modify) — `QualityConfig` + `ServerConfig.quality`.
- `src/engine.rs` (modify) — build `QualityShared`, tick → set_quality + abort gate, counters + `MetricsSnapshot` fields, log line.
- `src/main_support.rs` + `src/main.rs` (modify) — env for `QualityConfig`.
- `src/mcp.rs` + `src/mcp_http.rs` (modify) — expose quality.

---

### Task 1: Prefill jitter-buffer pacer (`pace::Downlink`)

**Files:**
- Modify: `src/bridge/pace.rs`
- Test: `src/bridge/pace.rs` tests

**Interfaces:**
- Produces: `Downlink::new(prebuffer_ms: u32, resume_ms: u32)`, `set_expecting(&mut self, bool)`, `underruns(&self) -> u64`, `starved_ms(&self) -> u64`. `push`/`clear`/`next_frame` keep their signatures; `next_frame` still returns exactly 160 samples/frame.

- [ ] **Step 1: Write the failing tests**

Add to `src/bridge/pace.rs` tests (the existing `underrun_yields_silence_frame` etc. call `Downlink::new()` with no args — update them to `Downlink::new(0, 0)` so prefill is disabled and their current assertions still hold, EXCEPT `buffered_audio_plays_out` which must now prefill: change it to `Downlink::new(0, 0)` too so 40 ms plays immediately as before). Then add:

```rust
#[test]
fn prefill_holds_playout_until_target_met() {
    let mut d = Downlink::new(140, 60); // prebuffer 140 ms = 3360 samples
    d.set_expecting(true);
    d.push(&[8000i16; 480 * 3]); // 60 ms < 140 ms target
    // Under target -> silence, and it's counted as starvation while expecting.
    let f = d.next_frame();
    assert!(f.iter().all(|&s| s.abs() < 16), "should hold (silence) under prefill target");
    // Top up past 140 ms and it starts playing.
    d.push(&[8000i16; 480 * 5]); // now 160 ms buffered
    let f = d.next_frame();
    assert!(f[80..].iter().any(|&s| s.abs() > 2000), "should play once prefill met");
}

#[test]
fn underrun_counted_only_while_expecting() {
    // Expecting: drain past the buffer -> one underrun + starved time.
    let mut d = Downlink::new(0, 60); // no initial prebuffer
    d.set_expecting(true);
    d.push(&[8000i16; 480]); // exactly one frame
    let _ = d.next_frame();  // plays it, buffer now empty
    let _ = d.next_frame();  // underrun (empty while expecting)
    assert_eq!(d.underruns(), 1);
    assert!(d.starved_ms() >= 20);

    // Not expecting: same drain counts nothing.
    let mut d2 = Downlink::new(0, 60);
    d2.push(&[8000i16; 480]);
    let _ = d2.next_frame();
    let _ = d2.next_frame(); // empty, but !expecting
    assert_eq!(d2.underruns(), 0);
}

#[test]
fn clear_rearms_prefill() {
    let mut d = Downlink::new(140, 60);
    d.set_expecting(true);
    d.push(&[8000i16; 480 * 8]); // 160 ms
    assert!(d.next_frame()[80..].iter().any(|&s| s.abs() > 2000)); // playing
    d.clear();
    d.push(&[8000i16; 480 * 3]); // 60 ms < 140 ms -> must re-prefill
    let f = d.next_frame();
    assert!(f.iter().all(|&s| s.abs() < 16), "clear must re-arm the 140 ms prefill");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --features vendor-openssl -p kutsu bridge::pace`
Expected: FAIL — `new` takes no args / no `set_expecting`/`underruns`.

- [ ] **Step 3: Implement the state machine**

Replace the `Downlink` struct + impl in `src/bridge/pace.rs`:

```rust
const IN_PER_FRAME: usize = 480;     // 20 ms @ 24 kHz
const SAMPLES_PER_MS: usize = 24;    // 24 kHz

pub struct Downlink {
    buf: VecDeque<i16>,
    down: Downsampler,
    prebuffer: usize,  // samples to buffer before (re)starting playout
    resume: usize,     // samples to buffer before resuming after a mid-turn underrun
    fill_target: usize,
    playing: bool,
    expecting: bool,
    underruns: u64,
    starved_ms: u64,
}

impl Downlink {
    pub fn new(prebuffer_ms: u32, resume_ms: u32) -> Self {
        let prebuffer = prebuffer_ms as usize * SAMPLES_PER_MS;
        let resume = resume_ms as usize * SAMPLES_PER_MS;
        Self {
            buf: VecDeque::new(), down: Downsampler::new(),
            prebuffer, resume, fill_target: prebuffer,
            playing: false, expecting: false, underruns: 0, starved_ms: 0,
        }
    }

    pub fn push(&mut self, samples: &[i16]) { self.buf.extend(samples.iter().copied()); }

    /// Barge-in: drop buffered audio and re-arm a full prefill for the next turn.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.playing = false;
        self.fill_target = self.prebuffer;
    }

    /// Arm/disarm underrun accounting for an active model turn. A rising edge
    /// (false -> true) re-arms a fresh prefill so each turn's audio is buffered
    /// before playout (absorbs Gemini-path jitter within the turn).
    pub fn set_expecting(&mut self, expecting: bool) {
        if expecting && !self.expecting {
            self.playing = false;
            self.fill_target = self.prebuffer;
        }
        self.expecting = expecting;
    }

    pub fn underruns(&self) -> u64 { self.underruns }
    pub fn starved_ms(&self) -> u64 { self.starved_ms }

    /// Produce one 20 ms frame (160 samples @ 8 kHz).
    pub fn next_frame(&mut self) -> Vec<i16> {
        if !self.playing {
            if self.buf.len() >= self.fill_target.max(IN_PER_FRAME) {
                self.playing = true;
            } else {
                if self.expecting { self.starved_ms += 20; }
                return self.silence_frame();
            }
        }
        if self.buf.len() >= IN_PER_FRAME {
            let mut block = [0i16; IN_PER_FRAME];
            for slot in block.iter_mut() {
                *slot = self.buf.pop_front().unwrap();
            }
            self.down.process(&block)
        } else {
            // Mid-turn underrun.
            if self.expecting {
                self.underruns += 1;
                self.starved_ms += 20;
            }
            self.playing = false;
            self.fill_target = self.resume;
            self.silence_frame()
        }
    }

    /// Feed 480 zeros so the downsampler's FIR state stays continuous.
    fn silence_frame(&mut self) -> Vec<i16> {
        self.down.process(&[0i16; IN_PER_FRAME])
    }
}
```

Note: `fill_target.max(IN_PER_FRAME)` makes `new(0,0)` behave like the old no-prefill pacer (plays as soon as one frame is available), so the updated legacy tests pass.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --features vendor-openssl -p kutsu bridge::pace`
Expected: PASS (new + updated legacy tests). Then `cargo build --features vendor-openssl` — the `Downlink::new()` call in `bridge/mod.rs` now needs args; it's fixed in Task 4, so a bin build may fail until then — that's expected; only the `pace` unit tests must pass here.

- [ ] **Step 5: Commit**

```bash
git add src/bridge/pace.rs
git commit -m "feat(bridge): prefill jitter buffer + underrun/starved metrics in pacer"
```

### Task 2: `CallQuality` on the record (`state.rs`)

**Files:**
- Modify: `src/state.rs`
- Test: `src/state.rs` tests

**Interfaces:**
- Produces: `pub struct CallQuality { pub underruns: u64, pub starved_ms: u64, pub max_gap_ms: u64 }` (Clone, Copy, Debug, Default, Serialize); `CallRecord.quality: CallQuality`; `CallStore::set_quality(&self, call_id: &str, q: CallQuality)`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn set_quality_updates_record() {
    let store = CallStore::new();
    store.insert(rec("c1"));
    store.set_quality("c1", CallQuality { underruns: 3, starved_ms: 60, max_gap_ms: 220 });
    let got = store.get("c1").unwrap();
    assert_eq!(got.quality.underruns, 3);
    assert_eq!(got.quality.starved_ms, 60);
    assert_eq!(got.quality.max_gap_ms, 220);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu state::tests::set_quality_updates_record`
Expected: FAIL — no `CallQuality` / `quality` field / `set_quality`.

- [ ] **Step 3: Implement**

Add the struct + field + method to `src/state.rs`:

```rust
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CallQuality {
    pub underruns: u64,
    pub starved_ms: u64,
    pub max_gap_ms: u64,
}
```

Add `pub quality: CallQuality,` to `CallRecord`. Every `CallRecord { .. }` literal (engine `place_call`, the `rec()` test helper, any other) sets `quality: CallQuality::default()`. Add:

```rust
pub fn set_quality(&self, call_id: &str, q: CallQuality) {
    if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
        r.quality = q;
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --features vendor-openssl -p kutsu state::`
Expected: PASS. Fix any `CallRecord { .. }` literal the compiler flags (add `quality: CallQuality::default()`).

- [ ] **Step 5: Commit**

```bash
git add src/state.rs src/engine.rs
git commit -m "feat(state): CallQuality on CallRecord + set_quality"
```

### Task 3: `QualityConfig` (`config.rs` + env)

**Files:**
- Modify: `src/config.rs`, `src/main_support.rs`, `src/main.rs`, and every `ServerConfig { .. }` construction site.
- Test: `src/config.rs` tests.

**Interfaces:**
- Produces: `pub struct QualityConfig { pub prebuffer_ms: u32, pub resume_ms: u32, pub abort_underruns: u32 }` with `Default` = `{ 140, 60, 40 }`; `ServerConfig.quality: QualityConfig`; env `KUTSU_QUALITY_PREBUFFER_MS`/`KUTSU_QUALITY_RESUME_MS`/`KUTSU_QUALITY_ABORT_UNDERRUNS`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn quality_config_defaults() {
    let q = QualityConfig::default();
    assert_eq!(q.prebuffer_ms, 140);
    assert_eq!(q.resume_ms, 60);
    assert_eq!(q.abort_underruns, 40);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu config::tests::quality_config_defaults`
Expected: FAIL — no `QualityConfig`.

- [ ] **Step 3: Implement**

In `src/config.rs`:

```rust
#[derive(Clone, Copy, Debug)]
pub struct QualityConfig {
    /// Samples-buffered target (ms) before (re)starting downlink playout.
    pub prebuffer_ms: u32,
    /// Faster re-arm target (ms) after a mid-turn underrun.
    pub resume_ms: u32,
    /// Cumulative underruns in one call that abort it as unusable; 0 = never.
    pub abort_underruns: u32,
}
impl Default for QualityConfig {
    fn default() -> Self { Self { prebuffer_ms: 140, resume_ms: 60, abort_underruns: 40 } }
}
```

Add `pub quality: QualityConfig,` to `ServerConfig`. In `main_support::configs_from_env`, parse (helper: `fn env_u32(k, d) { env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d) }`):

```rust
quality: QualityConfig {
    prebuffer_ms: env_u32("KUTSU_QUALITY_PREBUFFER_MS", 140),
    resume_ms: env_u32("KUTSU_QUALITY_RESUME_MS", 60),
    abort_underruns: env_u32("KUTSU_QUALITY_ABORT_UNDERRUNS", 40),
},
```

Every other `ServerConfig { .. }` site (main.rs `run_live`, and the test helpers in engine.rs/mcp.rs/mcp_http.rs/proto.rs/config.rs/gemini_live.rs) gets `quality: QualityConfig::default(),`.

- [ ] **Step 4: Run to verify pass + build**

Run: `cargo test --features vendor-openssl -p kutsu config::` then `cargo build --features vendor-openssl --tests`; fix each `missing field quality` the compiler reports with `quality: QualityConfig::default(),` (import `QualityConfig` where needed, or use `crate::config::QualityConfig::default()`).

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/main_support.rs src/main.rs src/engine.rs src/mcp.rs src/mcp_http.rs src/proto.rs src/gemini_live.rs
git commit -m "feat(config): QualityConfig (prebuffer/resume/abort) + env"
```

### Task 4: Bridge wiring — QualityShared, expecting, gap, publish

**Files:**
- Modify: `src/bridge/mod.rs`
- Test: `src/bridge/mod.rs` tests

**Interfaces:**
- Consumes: `Downlink::new/set_expecting/underruns/starved_ms` (Task 1), `CallQuality` (Task 2).
- Produces:
  - `pub struct QualityShared { underruns: AtomicU64, starved_ms: AtomicU64, max_gap_ms: AtomicU64 }` with `pub fn new() -> Arc<Self>`, `pub fn snapshot(&self) -> crate::state::CallQuality`.
  - `BridgePorts` gains `pub prebuffer_ms: u32`, `pub resume_ms: u32`, `pub quality: Arc<QualityShared>`.

- [ ] **Step 1: Read the current downlink loop**

Read `src/bridge/mod.rs` `run()`: the `select!` with the `uplink`, `gemini_events.recv()` (handles `OutputAudio`/`Interrupted`/`other`/`None`), and `ticker.tick()` arms. You are threading quality through this loop without changing its exit/abort structure.

- [ ] **Step 2: Write the failing test**

```rust
#[tokio::test(start_paused = true)]
async fn quality_shared_records_underruns_while_expecting() {
    let q = super::QualityShared::new();
    let (mut ports, mut ends) = wire(G711Kind::Ulaw);
    ports.prebuffer_ms = 0; // disable prefill so the test drains deterministically
    ports.resume_ms = 0;
    ports.quality = q.clone();
    let h = tokio::spawn(run(ports));
    // Model turn starts, one 20 ms chunk, then goes quiet mid-turn (no TurnComplete).
    ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480])).await.unwrap();
    tokio::task::yield_now().await;
    // Drain several ticks: after the one buffered frame, empty-while-expecting = underruns.
    for _ in 0..4 { tokio::time::advance(std::time::Duration::from_millis(20)).await; let _ = ends.phone_out_rx.recv().await; }
    assert!(q.snapshot().underruns >= 1, "expected underruns recorded while expecting");
    drop(ends); let _ = h.await;
}
```

(Adjust the wire helper to default `prebuffer_ms`/`resume_ms`/`quality`; see Step 3.)

- [ ] **Step 3: Implement**

Add near the top of `src/bridge/mod.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct QualityShared {
    underruns: AtomicU64,
    starved_ms: AtomicU64,
    max_gap_ms: AtomicU64,
}
impl QualityShared {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }
    pub fn snapshot(&self) -> crate::state::CallQuality {
        crate::state::CallQuality {
            underruns: self.underruns.load(Ordering::Relaxed),
            starved_ms: self.starved_ms.load(Ordering::Relaxed),
            max_gap_ms: self.max_gap_ms.load(Ordering::Relaxed),
        }
    }
}
```

`BridgePorts`: add `pub prebuffer_ms: u32, pub resume_ms: u32, pub quality: Arc<QualityShared>`. Update the `wire()` test helper + `BridgePorts { .. }` literals to set them (`prebuffer_ms: 140, resume_ms: 60, quality: QualityShared::new()` by default, overridden per test).

In `run()`: destructure the new fields; `let mut downlink = pace::Downlink::new(prebuffer_ms, resume_ms);`. Add a gap clock: `let mut last_audio: Option<tokio::time::Instant> = None; let mut max_gap_ms = 0u64;`. In the event arm:

```rust
Some(Event::OutputAudio(pcm24)) => {
    downlink.set_expecting(true);
    let now = tokio::time::Instant::now();
    if let Some(prev) = last_audio {
        let gap = now.duration_since(prev).as_millis() as u64;
        if gap > max_gap_ms { max_gap_ms = gap; }
    }
    last_audio = Some(now);
    downlink.push(&pcm24);
}
Some(Event::Interrupted) => { downlink.set_expecting(false); last_audio = None; downlink.clear(); }
Some(Event::TurnComplete) => {
    downlink.set_expecting(false);
    last_audio = None;
    let _ = events_out.send(Event::TurnComplete).await; // still forward it
}
Some(other) => { let _ = events_out.send(other).await; }
None => break BridgeEnd::GeminiClosed,
```

In the `ticker.tick()` arm, after producing the frame, publish counters:

```rust
quality.underruns.store(downlink.underruns(), Ordering::Relaxed);
quality.starved_ms.store(downlink.starved_ms(), Ordering::Relaxed);
quality.max_gap_ms.store(max_gap_ms, Ordering::Relaxed);
```

- [ ] **Step 4: Run tests + build**

Run: `cargo test --features vendor-openssl -p kutsu bridge::` then `cargo build --features vendor-openssl --tests`.
Expected: PASS. The engine's `BridgePorts { .. }` construction now needs the new fields — fixed in Task 5; a bin build may fail until then.

- [ ] **Step 5: Commit**

```bash
git add src/bridge/mod.rs
git commit -m "feat(bridge): QualityShared + expecting gating + inter-chunk gap"
```

### Task 5: Engine — abort gate, quality plumbing, metrics

**Files:**
- Modify: `src/engine.rs`
- Test: `src/engine.rs` tests

**Interfaces:**
- Consumes: `QualityShared` (Task 4), `CallStore::set_quality` (Task 2), `ServerConfig.quality` (Task 3).
- Produces: `MetricsSnapshot` gains `underruns_total: u64`, `starved_ms_total: u64`, `quality_aborted_total: u64`.

- [ ] **Step 1: Read run_call**

Read `run_call` in `src/engine.rs`: the `BridgePorts { .. }` construction (step 6), the `select!` orchestration loop (`events_out_rx` / `sip_events` / `bridge_task` / `deadline` arms), and the teardown + finalize (steps 8-9). You add a `QualityShared`, a `BridgePorts` field set, a 1 s tick arm, and cumulative counters.

- [ ] **Step 2: Write the failing test**

```rust
#[tokio::test]
async fn metrics_snapshot_has_quality_fields() {
    let (server, sip_cfg) = test_configs(1);
    let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
    let m = engine.metrics_snapshot();
    assert_eq!(m.underruns_total, 0);
    assert_eq!(m.starved_ms_total, 0);
    assert_eq!(m.quality_aborted_total, 0);
    engine.shutdown().await;
}
```

- [ ] **Step 3: Implement**

1. `MetricsSnapshot`: add `pub underruns_total: u64, pub starved_ms_total: u64, pub quality_aborted_total: u64`. Add matching `Arc<AtomicU64>` to the engine's `Counters` bundle; `metrics_snapshot` reads them.
2. In `run_call`, before building the bridge: `let quality = crate::bridge::QualityShared::new();`. Set the new `BridgePorts` fields: `prebuffer_ms: server.quality.prebuffer_ms, resume_ms: server.quality.resume_ms, quality: quality.clone()`.
3. Add a tick to the orchestration `select!`: `let mut qtick = tokio::time::interval(std::time::Duration::from_secs(1));` (pinned/created before the loop), arm:

```rust
_ = qtick.tick() => {
    let q = quality.snapshot();
    store.set_quality(&call_id, q);
    let cap = server.quality.abort_underruns;
    if cap > 0 && q.underruns >= cap as u64 {
        counters.quality_aborted.fetch_add(1, Ordering::Relaxed);
        break CallState::Failed;  // teardown below; error set in finalize
    }
}
```

4. Teardown/finalize: snapshot quality once more (`let q = quality.snapshot(); store.set_quality(&call_id, q);`), add `counters.underruns` / `counters.starved_ms` cumulative adds (`fetch_add(q.underruns, ..)` — add the per-call totals once at finalize, not per tick). If the loop broke via the abort arm, set `error = Some(format!("aborted: audio quality degraded ({} underruns, {} ms silence)", q.underruns, q.starved_ms))` on the `finalize` call (thread a small `abort_reason: Option<String>` from the abort arm). Log: `tracing::info!(%call_id, underruns = q.underruns, starved_ms = q.starved_ms, max_gap_ms = q.max_gap_ms, "call audio quality")`.

- [ ] **Step 4: Run tests + build**

Run: `cargo test --features vendor-openssl -p kutsu engine::` then `cargo build --features vendor-openssl`.
Expected: PASS + clean bin build (all `BridgePorts`/`MetricsSnapshot` sites now complete).

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): per-call quality + abort gate + cumulative audio metrics"
```

### Task 6: Expose quality (`mcp.rs`, `mcp_http.rs`)

**Files:**
- Modify: `src/mcp.rs` (`get_call_status`, `get_call_transcript`), `src/mcp_http.rs` (`render_prometheus`)
- Test: `src/mcp_http.rs` tests

**Interfaces:**
- Consumes: `CallRecord.quality` (Task 2), the three new `MetricsSnapshot` fields (Task 5).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn prometheus_has_quality_series() {
    let m = MetricsSnapshot {
        active: 0, queued: 0, placed_total: 0, completed_total: 0, failed_total: 0,
        cancelled_total: 0, channels_cap: 3,
        underruns_total: 7, starved_ms_total: 140, quality_aborted_total: 2,
    };
    let s = render_prometheus(&m);
    for line in ["kutsu_audio_underruns_total 7", "kutsu_audio_starved_ms_total 140", "kutsu_calls_quality_aborted_total 2"] {
        assert!(s.contains(line), "missing: {line}");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu mcp_http::tests::prometheus_has_quality_series`
Expected: FAIL — `MetricsSnapshot` literal missing the new fields / series absent.

- [ ] **Step 3: Implement**

`render_prometheus`: append three series — `kutsu_audio_underruns_total` (counter), `kutsu_audio_starved_ms_total` (counter), `kutsu_calls_quality_aborted_total` (counter) — from the snapshot fields. Update the existing `prometheus_has_all_series` test's `MetricsSnapshot` literal + the `graceful_teardown`/`test_engine` if any construct `MetricsSnapshot` — to include the new fields.

`src/mcp.rs`: in `get_call_status` and `get_call_transcript` JSON bodies add `"quality": rec.quality` (it's `Serialize`).

- [ ] **Step 4: Run tests + build**

Run: `cargo test --features vendor-openssl -p kutsu` then `cargo build --features vendor-openssl`.
Expected: PASS + clean build.

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs src/mcp_http.rs
git commit -m "feat(mcp): expose call quality in status + /metrics"
```

---

## Self-review

**Spec coverage:** prefill pacer (Task 1), CallQuality/store (Task 2), QualityConfig/env (Task 3), bridge QualityShared+expecting+gap (Task 4), engine tick+abort+metrics+log (Task 5), mcp/metrics exposure (Task 6). All spec components map to a task.

**Confirm-at-impl (reconnaissance, not placeholders):** Task 4 & 5 begin by reading the current `run()` loop / `run_call` (their exact structure drives the edits); `CallRecord`/`MetricsSnapshot`/`BridgePorts` literal sites are found by the compiler (each task's Step 4 fixes them).

**Cross-task field additions:** `CallRecord.quality` (T2), `ServerConfig.quality` (T3), `BridgePorts.{prebuffer_ms,resume_ms,quality}` (T4), `MetricsSnapshot.{underruns_total,starved_ms_total,quality_aborted_total}` (T5) — each task's build step fixes every construction site the compiler flags, so later tasks compile.

**Out of scope (spec):** rubato uplink, echo gating, RTP-layer metrics, rate/window abort — none in this plan.

## Execution

After approval, execute via superpowers:subagent-driven-development (fresh subagent per task + two-stage review), same as phase 5. Live confirmation (a real call now has ~zero underruns / smooth audio) is manual, done by the human after the branch lands.
