# Uplink Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure uplink (phone → Gemini) RTP loss/reorder and dump the incoming audio, so the next Linphone-over-WiFi call proves whether the ASR regression is network packet loss.

**Architecture:** A pure `UplinkStats` does RFC 3550 loss accounting on RTP sequence numbers in the SIP receive loop; it publishes into an `Arc<UplinkQualityShared>` (mirror of the existing `QualityShared`) that crosses the SIP-thread → engine boundary via `SipCallParts`. The engine merges the snapshot into `CallQuality` (status JSON + Prometheus). A separate env-gated dump writes the pre- and post-resample audio to WAV in the bridge uplink task.

**Tech Stack:** Rust 2024, tokio, ezk-sip/ezk-rtc (`RtpPacket.sequence_number`), existing `Pcm16Writer`.

**Spec:** `docs/superpowers/specs/2026-08-15-uplink-observability-design.md`

## Global Constraints

- Build/test with the vendored OpenSSL feature: `cargo test --lib --features vendor-openssl`. Plain `cargo test` fails to link OpenSSL on this Windows host.
- All in-repo text (code, comments, logs) is English.
- Loss accounting derives from `first_ext`/`max_ext`/`received` only — the exact extended value of reordered/duplicate packets never affects `lost` (they only bump `received` and their own classification counter). Keep the wrap logic minimal accordingly.
- New cumulative counters bump **once at finalize**, never per-tick (matches `underruns`/`starved_ms` totals).
- `#[allow(dead_code)]` is not used to silence "unused" on code wired in the same branch — wire it so it is actually used.

---

### Task 1: `UplinkStats` — pure RFC 3550 loss accounting

**Files:**
- Create: `src/sip/uplink.rs`
- Modify: `src/sip/mod.rs` (add `mod uplink;` and re-export `pub use uplink::{UplinkStats, UplinkQuality};`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `struct UplinkQuality { pub received: u64, pub lost: u64, pub reordered: u64 }` (derives `Clone, Copy, Debug, Default, PartialEq, Eq`)
  - `struct UplinkStats` with `fn new() -> Self`, `fn observe(&mut self, seq: u16)`, `fn snapshot(&self) -> UplinkQuality`.

- [ ] **Step 1: Write the failing tests**

Create `src/sip/uplink.rs` with the type stubs plus this test module:

```rust
//! Uplink RTP quality: loss/reorder accounting (RFC 3550-style) over the
//! 16-bit sequence space. Pure, single-writer; the SIP receive loop feeds it
//! each arriving sequence number.

/// A point-in-time uplink quality snapshot (phone -> us).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UplinkQuality {
    pub received: u64,
    pub lost: u64,
    pub reordered: u64,
}

/// Accumulates RTP sequence numbers into loss/reorder counts. `lost` is
/// derived as `(max_ext - first_ext + 1) - received`, correct under
/// reordering (a late packet raises `received`, not the span).
#[derive(Debug)]
pub struct UplinkStats {
    started: bool,
    cycles: u64,
    max16: u16,
    first_ext: u64,
    received: u64,
    reordered: u64,
    duplicated: u64,
}

impl UplinkStats {
    pub fn new() -> Self {
        Self { started: false, cycles: 0, max16: 0, first_ext: 0, received: 0, reordered: 0, duplicated: 0 }
    }

    pub fn observe(&mut self, _seq: u16) {
        unimplemented!()
    }

    pub fn snapshot(&self) -> UplinkQuality {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(seqs: &[u16]) -> UplinkQuality {
        let mut s = UplinkStats::new();
        for &q in seqs { s.observe(q); }
        s.snapshot()
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(run(&[]), UplinkQuality::default());
    }

    #[test]
    fn in_order_has_no_loss() {
        let q = run(&[100, 101, 102, 103, 104]);
        assert_eq!(q, UplinkQuality { received: 5, lost: 0, reordered: 0 });
    }

    #[test]
    fn single_gap_counts_one_lost() {
        // 102 missing.
        let q = run(&[100, 101, 103, 104]);
        assert_eq!(q, UplinkQuality { received: 4, lost: 1, reordered: 0 });
    }

    #[test]
    fn wider_gap_counts_all_lost() {
        // 101..104 missing (4 lost).
        let q = run(&[100, 105]);
        assert_eq!(q, UplinkQuality { received: 2, lost: 4, reordered: 0 });
    }

    #[test]
    fn reorder_does_not_inflate_loss() {
        // 102 arrives late, after 103.
        let q = run(&[100, 101, 103, 102, 104]);
        assert_eq!(q, UplinkQuality { received: 5, lost: 0, reordered: 1 });
    }

    #[test]
    fn duplicate_is_counted_not_lost() {
        let q = run(&[100, 101, 101, 102]);
        // received 4, span 100..102 = 3, saturating -> lost 0.
        assert_eq!(q.lost, 0);
        assert_eq!(q.received, 4);
    }

    #[test]
    fn wraparound_has_no_loss() {
        let q = run(&[65534, 65535, 0, 1]);
        assert_eq!(q, UplinkQuality { received: 4, lost: 0, reordered: 0 });
    }
}
```

Add to `src/sip/mod.rs` near the other `mod`/`pub use` lines:

```rust
mod uplink;
pub use uplink::{UplinkQuality, UplinkStats};
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib --features vendor-openssl sip::uplink`
Expected: FAIL (panics from `unimplemented!()`).

- [ ] **Step 3: Implement `observe` and `snapshot`**

Replace the two method bodies:

```rust
    pub fn observe(&mut self, seq: u16) {
        if !self.started {
            self.started = true;
            self.max16 = seq;
            self.first_ext = seq as u64; // cycle 0 baseline
            self.received = 1;
            return;
        }
        self.received += 1;
        let forward = seq.wrapping_sub(self.max16); // distance max -> seq, forward
        if forward == 0 {
            self.duplicated += 1;
        } else if forward < 0x8000 {
            // Forward progress, possibly across a 16-bit wrap.
            if seq < self.max16 {
                self.cycles += 1;
            }
            self.max16 = seq;
        } else {
            // seq is behind max -> a reordered (late) packet.
            self.reordered += 1;
        }
    }

    pub fn snapshot(&self) -> UplinkQuality {
        if !self.started {
            return UplinkQuality::default();
        }
        let max_ext = self.cycles * 65_536 + self.max16 as u64;
        let span = max_ext - self.first_ext + 1;
        UplinkQuality {
            received: self.received,
            lost: span.saturating_sub(self.received),
            reordered: self.reordered,
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features vendor-openssl sip::uplink`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/sip/uplink.rs src/sip/mod.rs
git commit -m "feat(sip): UplinkStats RFC 3550 loss/reorder accounting"
```

---

### Task 2: `UplinkQualityShared` — cross-thread handle

**Files:**
- Modify: `src/sip/uplink.rs` (add the shared type + test)

**Interfaces:**
- Consumes: `UplinkQuality` (Task 1).
- Produces: `struct UplinkQualityShared` with `fn new() -> Arc<Self>`, `fn publish(&self, q: &UplinkQuality)`, `fn snapshot(&self) -> UplinkQuality`.

- [ ] **Step 1: Write the failing test**

Append to `src/sip/uplink.rs` (above `#[cfg(test)]` add the imports; inside the type area add the struct):

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free uplink counters published by the SIP receive loop and read by
/// the engine, mirroring `bridge::QualityShared`. Single writer, single reader.
#[derive(Default)]
pub struct UplinkQualityShared {
    received: AtomicU64,
    lost: AtomicU64,
    reordered: AtomicU64,
}

impl UplinkQualityShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn publish(&self, q: &UplinkQuality) {
        self.received.store(q.received, Ordering::Relaxed);
        self.lost.store(q.lost, Ordering::Relaxed);
        self.reordered.store(q.reordered, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> UplinkQuality {
        UplinkQuality {
            received: self.received.load(Ordering::Relaxed),
            lost: self.lost.load(Ordering::Relaxed),
            reordered: self.reordered.load(Ordering::Relaxed),
        }
    }
}
```

Add this test to the `tests` module:

```rust
    #[test]
    fn shared_roundtrips_published_snapshot() {
        let shared = UplinkQualityShared::new();
        assert_eq!(shared.snapshot(), UplinkQuality::default());
        let q = UplinkQuality { received: 50, lost: 3, reordered: 2 };
        shared.publish(&q);
        assert_eq!(shared.snapshot(), q);
    }
```

Update the re-export in `src/sip/mod.rs`:

```rust
pub use uplink::{UplinkQuality, UplinkQualityShared, UplinkStats};
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl sip::uplink::tests::shared_roundtrips`
Expected: FAIL to compile first (until the struct is added), then PASS once added — if it compiles and passes immediately that is fine (the type is new code).

- [ ] **Step 3: (implementation already written in Step 1)**

No separate impl step — the struct above is the implementation.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl sip::uplink`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/sip/uplink.rs src/sip/mod.rs
git commit -m "feat(sip): UplinkQualityShared lock-free cross-thread handle"
```

---

### Task 3: Thread the handle through the SIP call and feed it from the receive loop

**Files:**
- Modify: `src/sip/mod.rs` (`SipCall` field, `from_parts` param, `SipCallParts` field, `split`, the mod-level unit test that builds parts around line 369)
- Modify: `src/sip/call.rs` (create the handle, pass to `from_parts`, feed the receive loop)

**Interfaces:**
- Consumes: `UplinkQualityShared`, `UplinkStats` (Tasks 1-2).
- Produces: `SipCallParts.uplink_quality: Arc<UplinkQualityShared>` (read by the engine in Task 6).

- [ ] **Step 1: Add the field to `SipCall`, `from_parts`, `SipCallParts`, and `split`**

In `src/sip/mod.rs`:

`SipCall` struct — add field:
```rust
    hangup: Option<oneshot::Sender<()>>,
    uplink_quality: std::sync::Arc<UplinkQualityShared>,
```

`split` — carry it:
```rust
        SipCallParts {
            call_id: self.call_id,
            events: self.events,
            audio_in: self.rtp_in,
            audio_out: self.rtp_out,
            hangup,
            uplink_quality: self.uplink_quality,
        }
```

`from_parts` — new param (keep it last):
```rust
    pub(crate) fn from_parts(
        call_id: String,
        events: mpsc::Receiver<SipEvent>,
        rtp_in: mpsc::Receiver<Bytes>,
        rtp_out: mpsc::Sender<Bytes>,
        hangup: oneshot::Sender<()>,
        uplink_quality: std::sync::Arc<UplinkQualityShared>,
    ) -> Self {
        Self { call_id, events, rtp_in, rtp_out, hangup: Some(hangup), uplink_quality }
    }
```

`SipCallParts` struct — add field:
```rust
    pub hangup: oneshot::Sender<()>,
    pub uplink_quality: std::sync::Arc<UplinkQualityShared>,
```

- [ ] **Step 2: Update the SIP unit test that builds a call/parts (mod.rs ~line 369)**

That test constructs channels and calls `from_parts` / builds a `SipCall`. Add a shared handle and pass it. Find the `from_parts(...)` (or `SipCall { ... }`) call in the test and add `UplinkQualityShared::new()` as the final argument / field. Example edit for a `from_parts` call:

```rust
        let call = SipCall::from_parts(
            "c1".into(), ev_rx, in_rx, out_tx, hup_tx,
            UplinkQualityShared::new(),
        );
```

- [ ] **Step 3: Wire `call.rs` — create the handle and feed the receive loop**

In `src/sip/call.rs`, import the types (add to the existing `use super::{...}` / crate imports):
```rust
use super::{UplinkQualityShared, UplinkStats};
```

Where the channels are created (around line 129-133), create the shared handle and pass it to `from_parts`:
```rust
    let uplink_quality = UplinkQualityShared::new();
    let handle = SipCall::from_parts(call_id, ev_rx, in_rx, out_tx, hup_tx, uplink_quality.clone());
```

Just before the main media loop (near the `let reason = loop {`), create the stats accumulator:
```rust
    let mut uplink_stats = UplinkStats::new();
```

In the receive arm (currently `Some(rtp) = receiver.recv() => { let _ = in_tx.try_send(rtp.payload); }`), account before forwarding:
```rust
            Some(rtp) = receiver.recv() => {
                uplink_stats.observe(rtp.sequence_number.0);
                uplink_quality.publish(&uplink_stats.snapshot());
                // Drop-on-full: never block the media loop on a stalled bridge.
                let _ = in_tx.try_send(rtp.payload);
            }
```

Note: `rtp.sequence_number.0` is the raw `u16` (ezk `SequenceNumber(u16)`).

- [ ] **Step 4: Build and run SIP tests**

Run: `cargo test --lib --features vendor-openssl sip::`
Expected: PASS (existing sip tests still green; crate compiles with the new field threaded).

- [ ] **Step 5: Commit**

```bash
git add src/sip/mod.rs src/sip/call.rs
git commit -m "feat(sip): feed UplinkStats from the RTP receive loop, expose on SipCallParts"
```

---

### Task 4: Add uplink fields to `CallQuality`

**Files:**
- Modify: `src/state.rs` (`CallQuality` struct + the `set_quality_updates_record` test)
- Modify: `src/bridge/mod.rs` (`QualityShared::snapshot` constructs `CallQuality` — fill uplink fields with `..Default::default()`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `CallQuality { .., pub uplink_received: u64, pub uplink_lost: u64, pub uplink_reordered: u64 }`.

- [ ] **Step 1: Extend the test first**

In `src/state.rs`, update `set_quality_updates_record` (around line 237-242) to set and assert the new fields:

```rust
    #[test]
    fn set_quality_updates_record() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_quality("c1", CallQuality {
            underruns: 3, starved_ms: 60, max_gap_ms: 220,
            uplink_received: 500, uplink_lost: 7, uplink_reordered: 1,
        });
        let got = store.get("c1").unwrap();
        assert_eq!(got.quality.underruns, 3);
        assert_eq!(got.quality.uplink_lost, 7);
        assert_eq!(got.quality.uplink_received, 500);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl state::tests::set_quality_updates_record`
Expected: FAIL to compile ("missing fields uplink_received ...").

- [ ] **Step 3: Add the fields**

In `src/state.rs`, `CallQuality`:
```rust
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CallQuality {
    pub underruns: u64,
    pub starved_ms: u64,
    pub max_gap_ms: u64,
    /// Uplink (phone -> us) RTP packets received.
    pub uplink_received: u64,
    /// Uplink RTP packets lost (RFC 3550 span - received).
    pub uplink_lost: u64,
    /// Uplink RTP packets that arrived late/out-of-order.
    pub uplink_reordered: u64,
}
```

In `src/bridge/mod.rs`, `QualityShared::snapshot` builds a `CallQuality` with only the three downlink fields — add the spread so it still compiles:
```rust
    pub fn snapshot(&self) -> crate::state::CallQuality {
        crate::state::CallQuality {
            underruns: self.underruns.load(Ordering::Relaxed),
            starved_ms: self.starved_ms.load(Ordering::Relaxed),
            max_gap_ms: self.max_gap_ms.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl state:: bridge::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/state.rs src/bridge/mod.rs
git commit -m "feat(state): uplink_received/lost/reordered on CallQuality"
```

---

### Task 5: Config — `dump_uplink_dir` env seam

**Files:**
- Modify: `src/config.rs` (`ServerConfig.dump_uplink_dir` field)
- Modify: `src/main_support.rs` (read `KUTSU_DUMP_UPLINK_DIR`)
- Modify: every `ServerConfig { .. }` literal the compiler flags (mirror how `transcript_dir` is set — the existing literals that already set `transcript_dir` all need the new field): `src/main.rs`, `src/engine.rs` (test literal ~line 455), `tests/live_smoke.rs`, and any test in `config.rs`.

**Interfaces:**
- Consumes: nothing.
- Produces: `ServerConfig.dump_uplink_dir: Option<std::path::PathBuf>`.

- [ ] **Step 1: Add the field**

In `src/config.rs`, next to `transcript_dir`:
```rust
    /// Directory for per-call uplink audio dumps (WAV). `None` disables it.
    pub dump_uplink_dir: Option<std::path::PathBuf>,
```

- [ ] **Step 2: Read it from env**

In `src/main_support.rs`, next to the `transcript_dir` line (~41):
```rust
        dump_uplink_dir: non_empty("KUTSU_DUMP_UPLINK_DIR").map(std::path::PathBuf::from),
```

- [ ] **Step 3: Fix every other `ServerConfig` literal**

Add `dump_uplink_dir: None,` to each literal the compiler flags. Known sites (all already set `transcript_dir: None`): `src/main.rs:234`, `src/engine.rs` (~line 455 test), `tests/live_smoke.rs` (~line 30). Build to find any others:

Run: `cargo build --lib --features vendor-openssl` and add `dump_uplink_dir: None,` wherever it errors "missing field".

- [ ] **Step 4: Verify build + tests**

Run: `cargo test --lib --features vendor-openssl config::`
Expected: PASS. Also `cargo build --tests --features "vendor-openssl live-tests"` compiles `live_smoke.rs`.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/main_support.rs src/main.rs src/engine.rs tests/live_smoke.rs
git commit -m "feat(config): KUTSU_DUMP_UPLINK_DIR -> ServerConfig.dump_uplink_dir"
```

---

### Task 6: Engine — merge uplink snapshot into `CallQuality` + cumulative counters

**Files:**
- Modify: `src/engine.rs` (`Counters`, `MetricsSnapshot`, `metrics_snapshot`, `run_call` merge in `qtick` + finalize)
- Modify: `src/mcp_http.rs` test literals that build `MetricsSnapshot` (add the two new fields — compiler-flagged)

**Interfaces:**
- Consumes: `SipCallParts.uplink_quality` (Task 3), `CallQuality` uplink fields (Task 4).
- Produces: `MetricsSnapshot { .., pub uplink_received_total: u64, pub uplink_lost_total: u64 }` (read by Task 8).

- [ ] **Step 1: Write the failing test**

In `src/engine.rs` tests, extend the existing metrics test (the one asserting `m.channels_cap`) or add a new one asserting the snapshot exposes uplink totals (they are zero with no calls, but the fields must exist):

```rust
    #[tokio::test]
    async fn metrics_snapshot_exposes_uplink_totals() {
        let engine = test_engine().await; // use the same constructor the neighbouring metrics test uses
        let m = engine.metrics_snapshot();
        assert_eq!(m.uplink_received_total, 0);
        assert_eq!(m.uplink_lost_total, 0);
        engine.shutdown().await;
    }
```

(If the neighbouring test builds the engine inline rather than via a helper, copy that construction here instead of `test_engine()`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl engine::tests::metrics_snapshot_exposes_uplink_totals`
Expected: FAIL to compile ("no field `uplink_received_total`").

- [ ] **Step 3: Add counters, snapshot fields, and the merge**

In `src/engine.rs`:

`Counters` — add fields:
```rust
    uplink_received: AtomicU64,
    uplink_lost: AtomicU64,
```

`MetricsSnapshot` — add fields:
```rust
    pub uplink_received_total: u64,
    pub uplink_lost_total: u64,
```

`metrics_snapshot()` — populate them:
```rust
            uplink_received_total: self.counters.uplink_received.load(Ordering::Relaxed),
            uplink_lost_total: self.counters.uplink_lost.load(Ordering::Relaxed),
```

In `run_call`, destructure `uplink_quality` from the parts (the `SipCallParts { .. }` binding around line 265):
```rust
    let SipCallParts { events: mut sip_events, audio_in, audio_out, hangup: sip_hangup, uplink_quality, .. } = call.split();
```

Add a small helper that merges a downlink `CallQuality` snapshot with the uplink snapshot (keeps the merge in one place, used by both the tick and finalize):
```rust
fn merge_quality(mut q: CallQuality, u: crate::sip::UplinkQuality) -> CallQuality {
    q.uplink_received = u.received;
    q.uplink_lost = u.lost;
    q.uplink_reordered = u.reordered;
    q
}
```

In the `qtick` arm, replace `let q = quality.snapshot();` / `store.set_quality(&call_id, q);` with:
```rust
                let q = merge_quality(quality.snapshot(), uplink_quality.snapshot());
                store.set_quality(&call_id, q);
```
(Keep the existing `should_abort(&server.quality, q.underruns)` check below it — `q.underruns` is unchanged by the merge.)

In the finalize block, where the final snapshot is taken (`let q = quality.snapshot();` ~line 389), merge and bump the cumulative uplink counters (once, here):
```rust
    let q = merge_quality(quality.snapshot(), uplink_quality.snapshot());
    store.set_quality(&call_id, q);
    counters.underruns.fetch_add(q.underruns, Ordering::Relaxed);
    counters.starved_ms.fetch_add(q.starved_ms, Ordering::Relaxed);
    counters.uplink_received.fetch_add(q.uplink_received, Ordering::Relaxed);
    counters.uplink_lost.fetch_add(q.uplink_lost, Ordering::Relaxed);
    tracing::info!(
        %call_id, codec = ?codec.kind,
        uplink_received = q.uplink_received, uplink_lost = q.uplink_lost,
        uplink_reordered = q.uplink_reordered,
        underruns = q.underruns, starved_ms = q.starved_ms, max_gap_ms = q.max_gap_ms,
        "call audio quality"
    );
```
(Remove the old `tracing::info!(.. "call audio quality")` line this replaces, and the old two `fetch_add` lines it now supersedes — do not double-count.)

In `src/mcp_http.rs`, add `uplink_received_total: 0,` and `uplink_lost_total: 0,` (and non-zero values where a test asserts specific numbers) to every `MetricsSnapshot { .. }` literal the compiler flags.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl engine:: mcp_http::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/mcp_http.rs
git commit -m "feat(engine): merge uplink loss into CallQuality + cumulative totals + log codec"
```

---

### Task 7: Bridge — dual WAV dump of uplink audio

**Files:**
- Modify: `src/bridge/mod.rs` (`BridgePorts` gains `call_id` + `uplink_dump`; uplink task writes two WAVs)
- Modify: `src/engine.rs` (fill the two new `BridgePorts` fields)

**Interfaces:**
- Consumes: `ServerConfig.dump_uplink_dir` (Task 5), `Pcm16Writer` (`src/audio_file.rs`).
- Produces: two WAV files per call when the env dir is set.

- [ ] **Step 1: Write the failing test**

In `src/bridge/mod.rs` tests, add a test that runs the uplink path with a dump dir and asserts both files exist and are readable PCM16. Use the existing test scaffolding that pushes `phone_in` frames (see `downlink_paces_frames_to_phone` for how ports/tasks are built); the minimal shape:

```rust
    #[tokio::test]
    async fn uplink_dump_writes_both_wavs() {
        let dir = std::env::temp_dir().join(format!("kutsu-uplink-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Build ports with uplink_dump = Some(dir.clone()), call_id "c1", codec Ulaw,
        // feeding a few 160-byte G.711 frames into phone_in, then close it.
        // (Reuse the port-construction helper used by the other bridge tests.)
        // ... run bridge::run(ports) to completion ...
        let eight = dir.join("c1-uplink-8k.wav");
        let sixteen = dir.join("c1-uplink-16k.wav");
        assert!(eight.exists(), "8k dump missing");
        assert!(sixteen.exists(), "16k dump missing");
        assert!(!crate::audio_file::read_pcm16(&eight, 8000).unwrap().is_empty());
        assert!(!crate::audio_file::read_pcm16(&sixteen, 16000).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
```

Note for the implementer: mirror the exact port construction from the neighbouring bridge test; only `call_id` and `uplink_dump` are new fields to set.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl bridge::tests::uplink_dump_writes_both_wavs`
Expected: FAIL to compile ("missing fields `call_id`, `uplink_dump`").

- [ ] **Step 3: Add the fields and the dump**

In `src/bridge/mod.rs`, `BridgePorts`:
```rust
    /// Call id, used to name uplink dump files.
    pub call_id: String,
    /// If set, directory to write per-call uplink WAV dumps into.
    pub uplink_dump: Option<std::path::PathBuf>,
```

Destructure them in `run` (the `let BridgePorts { .. } = ports;` around line 82-90): add `call_id, uplink_dump,`.

In the uplink task (around line 94-102), open writers before the loop and write in it:
```rust
    let uplink = tokio::spawn(async move {
        let mut dump8 = uplink_dump.as_ref().and_then(|dir| {
            crate::audio_file::Pcm16Writer::create(&dir.join(format!("{call_id}-uplink-8k.wav")), 8000).ok()
        });
        let mut dump16 = uplink_dump.as_ref().and_then(|dir| {
            crate::audio_file::Pcm16Writer::create(&dir.join(format!("{call_id}-uplink-16k.wav")), 16000).ok()
        });
        while let Some(payload) = phone_in.recv().await {
            let pcm8 = g711::decode(codec, &payload);
            let pcm16 = resample::up_8k_16k(&pcm8);
            if let Some(w) = dump8.as_mut() { let _ = w.write(&pcm8); }
            if let Some(w) = dump16.as_mut() { let _ = w.write(&pcm16); }
            if gemini_in.send(pcm16).await.is_err() {
                break; // gemini sink closed
            }
        }
        if let Some(w) = dump8.take() { let _ = w.finalize(); }
        if let Some(w) = dump16.take() { let _ = w.finalize(); }
    });
```

In `src/engine.rs`, fill the new `BridgePorts` fields (the `BridgePorts { .. }` literal ~line 303):
```rust
        call_id: call_id.clone(),
        uplink_dump: server.dump_uplink_dir.clone(),
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl bridge::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/bridge/mod.rs src/engine.rs
git commit -m "feat(bridge): env-gated dual WAV dump of uplink audio (8k arrived + 16k to Gemini)"
```

---

### Task 8: Prometheus — uplink series

**Files:**
- Modify: `src/mcp_http.rs` (`render_prometheus` + a `uplink_series_present` test)

**Interfaces:**
- Consumes: `MetricsSnapshot.uplink_received_total` / `uplink_lost_total` (Task 6).
- Produces: two `/metrics` series.

- [ ] **Step 1: Write the failing test**

In `src/mcp_http.rs` tests, mirroring `prometheus_has_quality_series`:

```rust
    #[test]
    fn uplink_series_present() {
        let m = MetricsSnapshot {
            // reuse the same base literal the neighbouring test uses, with:
            uplink_received_total: 900, uplink_lost_total: 12,
            ..base_metrics() // or inline the full literal as the sibling test does
        };
        let s = render_prometheus(&m);
        for line in ["kutsu_uplink_received_total 900", "kutsu_uplink_lost_total 12"] {
            assert!(s.contains(line), "missing: {line}\n{s}");
        }
    }
```

If there is no `base_metrics()` helper, copy the full `MetricsSnapshot { .. }` literal from `prometheus_has_quality_series` and set the two uplink fields.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl mcp_http::tests::uplink_series_present`
Expected: FAIL (series not rendered).

- [ ] **Step 3: Render the series**

In `render_prometheus`, after the `kutsu_audio_starved_ms_total` line (~63):
```rust
    g(&mut s, "kutsu_uplink_received_total", "Uplink RTP packets received since start.", "counter", m.uplink_received_total.to_string());
    g(&mut s, "kutsu_uplink_lost_total", "Uplink RTP packets lost since start.", "counter", m.uplink_lost_total.to_string());
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl mcp_http::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mcp_http.rs
git commit -m "feat(mcp): expose uplink received/lost totals in /metrics"
```

---

### Task 9: Whole-feature verification

**Files:** none (verification only).

- [ ] **Step 1: Full lib test run**

Run: `cargo test --lib --features vendor-openssl`
Expected: PASS (all prior tests + new uplink tests).

- [ ] **Step 2: Live-tests compile check**

Run: `cargo test --no-run --features "vendor-openssl live-tests" --test live_smoke`
Expected: compiles (the `ServerConfig` literal now has `dump_uplink_dir`).

- [ ] **Step 3: Manual smoke note (no commit)**

Record in the run notes: to capture the next real call, run the binary with `KUTSU_DUMP_UPLINK_DIR=<dir>` and stderr redirected to a file; after a Linphone call, inspect the `call audio quality` log line (uplink_lost / codec) and listen to `*-uplink-8k.wav` vs `*-uplink-16k.wav`.

- [ ] **Step 4: Commit (if any verification fixups were needed)**

```bash
git add -A
git commit -m "test(uplink): whole-feature verification fixups"
```
