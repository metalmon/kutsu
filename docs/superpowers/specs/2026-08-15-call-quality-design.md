# Call quality: prefill jitter-buffer + quality metrics + abort gate — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-15
**Scope:** `src/bridge/pace.rs` (prefill + underrun instrumentation), `src/bridge/mod.rs` (expecting gating + inter-chunk gap + quality reporting), `src/config.rs` (`QualityConfig`), `src/state.rs` (`CallQuality` on `CallRecord`), `src/engine.rs` (consume quality events, abort gate, metrics), `src/mcp_http.rs` + `src/mcp.rs` (expose quality), `src/main_support.rs` (env).

## Problem (diagnosed on a live call)

The downlink (Gemini → phone) audio dropped out — "half the words swallowed." Root cause is explicit in `pace.rs`: the pacer has **no pre-buffer**; it plays whatever is queued and inserts silence on every empty-buffer frame. Under real Gemini-path jitter (audio arrives in bursts through a remote proxy), each gap becomes inserted silence. The prototype (`e:/voice-cloud`, `audio_io.py`) solved this with an adaptive prefill jitter buffer (the "long-debugged" part). The pre-call `net_check` never catches this: it probes the **Gemini WSS RTT** once before the call, not the mid-call **playout-buffer starvation** where the dropouts occur.

There is no RTP-layer quality measurement to port — the prototype is WebSocket-only. All quality is measured at the **playout-buffer (pacer) layer**: underruns, silence-inserted ms, inter-chunk gaps. We reproduce those.

## Decisions

- **Prefill values ported from the prototype** (tuned): prebuffer **140 ms**, resume **60 ms**, both configurable. Cost: ~140 ms added latency before the agent's first audio — accepted (smoothness > latency).
- **Abort on degradation is enabled** (user-confirmed), configurable, off by `0`. The prototype only warns; we go further and hang up a call whose audio is clearly broken, marking it low-quality.
- **Quality is measured at the pacer + downlink-task layer**, not RTP. `net_check` stays as-is (Gemini pre-call probe); it was not the fault here.
- Metrics flow to `CallRecord.quality`, the `/metrics` endpoint (aggregate), `get_call_status`, and a per-call log line.

## Component 1 — Prefill jitter-buffer pacer (`pace::Downlink`)

Today `Downlink { buf: VecDeque<i16>, down: Downsampler }` and `next_frame()` pulls 480 samples (zero-padding on empty), always emits a 160-sample 8 kHz frame. Add an adaptive prefill state machine (24 kHz sample math: 1 ms = 24 samples).

New fields:
- `prebuffer_samples: usize` = `prebuffer_ms * 24` (start/refill target; default 140 ms → 3360).
- `resume_samples: usize` = `resume_ms * 24` (fast re-arm after a mid-speech underrun; default 60 ms → 1440).
- `playing: bool` — true while emitting real audio, false while holding for (re)prefill.
- `fill_target: usize` — current threshold to (re)start playout (`prebuffer_samples` initially, `resume_samples` after an underrun).
- `expecting: bool` — armed while a model turn is active. Underruns are only counted while `expecting`, so the legitimate buffer drain at turn end is NOT a false dropout.
- `underruns: u64`, `starved_ms: u64` — counters.

`next_frame()` (still returns exactly one 160-sample 8 kHz frame, every 20 ms):

1. **Holding (`!playing`)**: if `buf.len() >= fill_target`, set `playing = true` (playout (re)starts this frame). Otherwise emit a silence block (feed 480 zeros to the downsampler to keep its filter state continuous) and, if `expecting`, add 20 ms to `starved_ms`. Return.
2. **Playing**: if `buf.len() >= 480`, pop 480 real samples → downsampler → return. If `buf.len() < 480` (underrun): if `expecting`, increment `underruns`, add 20 ms `starved_ms`; set `playing = false`, `fill_target = resume_samples` (fast re-arm). Emit a silence block this frame. (After turn end — `!expecting` — the same empty-buffer path emits silence without counting, and `fill_target` resets to `prebuffer_samples` on the next `set_expecting(true)`.)

Methods: `push`, `clear` (barge-in: also reset `playing=false`, `fill_target=prebuffer_samples` so the next turn re-prefills), `set_expecting(bool)` (on `true` after a false→true edge, reset `fill_target=prebuffer_samples`, `playing=false` — a new turn prefills fresh), `underruns()`, `starved_ms()`.

`Downlink::new` takes `prebuffer_ms` + `resume_ms` (from config).

**Testable pure logic** (unit tests, no IO): prefill delays playout until the target is buffered; a mid-turn gap while `expecting` counts one underrun + 20 ms starved and re-arms at the resume target; the same gap while `!expecting` counts nothing; `clear` re-arms prefill.

## Component 2 — Expecting gating + inter-chunk gap + quality reporting (`bridge`)

The downlink task in `src/bridge/mod.rs` owns the `Downlink` and consumes Gemini events. Wire:
- On `Event::OutputAudio` (model producing audio): `downlink.set_expecting(true)`, and track `max_gap_ms` = max wall-clock ms between consecutive OutputAudio chunks *within a turn* (reset the gap clock at turn start). This proxies Gemini-path jitter.
- On `Event::TurnComplete` (and `Interrupted`): `downlink.set_expecting(false)` (Interrupted also `clear()`s, as today).
- Quality surface: a shared `Arc<QualityShared>` (three `AtomicU64`: `underruns`, `starved_ms`, `max_gap_ms`) is created by the engine and passed into `BridgePorts`; the downlink task updates it (relaxed stores) as the pacer counts. This keeps the `gemini_live::Event` enum clean (Quality is bridge-originated, not a Gemini protocol event) and lets the engine snapshot it live for the abort gate. `QualityShared::snapshot() -> CallQuality`.

## Component 3 — `CallQuality` on the record + config

`src/state.rs`: add
```rust
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CallQuality { pub underruns: u64, pub starved_ms: u64, pub max_gap_ms: u64 }
```
Add `pub quality: CallQuality` to `CallRecord` (default on insert); a `CallStore::set_quality(call_id, CallQuality)`.

`src/config.rs`: add
```rust
#[derive(Clone, Copy, Debug)]
pub struct QualityConfig {
    pub prebuffer_ms: u32,      // default 140
    pub resume_ms: u32,         // default 60
    pub abort_underruns: u32,   // cumulative underruns that abort the call; 0 = never
}
```
Default `{ prebuffer_ms: 140, resume_ms: 60, abort_underruns: 40 }`. Add `pub quality: QualityConfig` to `ServerConfig`. `main_support::configs_from_env` reads `KUTSU_QUALITY_PREBUFFER_MS`/`KUTSU_QUALITY_RESUME_MS`/`KUTSU_QUALITY_ABORT_UNDERRUNS`. All `ServerConfig` construction sites get the field.

**Abort threshold default:** `abort_underruns = 40`. Rationale: the prototype flags "unstable" at ≥3 underruns per 0.5 s (transient); 40 cumulative underruns in one call = ~0.8 s of dropped speech spread across the call = clearly unusable. Conservative enough not to kill a call over a couple of blips, low enough to end a genuinely broken one. `0` disables. (Future: a rate/window instead of cumulative — noted as scope, not built now.)

## Component 4 — Engine: consume quality, abort gate, metrics (`engine.rs`)

`run_call` creates the `Arc<QualityShared>`, passes a clone into `BridgePorts`, and keeps one. Add a periodic tick (`tokio::time::interval(1s)`) as a new arm in the select-loop:
- on tick → `let q = quality.snapshot(); store.set_quality(&call_id, q);` If `abort_underruns > 0 && q.underruns >= abort_underruns as u64` → break the loop with a new terminal reason `QualityAbort(q)`: teardown as usual, finalize `CallState::Failed` with `error = format!("aborted: audio quality degraded ({} underruns, {} ms silence)", q.underruns, q.starved_ms)`, and bump the `quality_aborted` counter. Also snapshot once more at teardown so a short call still records its quality.
- Extend `MetricsSnapshot` with `quality_aborted_total: u64` and gauges `underruns_last`/`starved_ms_last` are NOT global — instead expose a cumulative `underruns_total` counter (sum across calls) so `/metrics` shows fleet audio health: add `kutsu_audio_underruns_total`, `kutsu_audio_starved_ms_total`, `kutsu_calls_quality_aborted_total`.
- Log a per-call summary at finalize: `tracing::info!(%call_id, underruns, starved_ms, max_gap_ms, "call audio quality")`.

## Component 5 — Expose quality (`mcp.rs`, `mcp_http.rs`)

- `get_call_status` / `get_call_transcript` JSON: include `"quality": { underruns, starved_ms, max_gap_ms }` from the record.
- `render_prometheus`: add the three new series (Component 4).

## Out of scope (named, not tails)

- **Uplink resampling upgrade (`rubato`)** — the downlink was clean on Linphone, so resampling is not the dropout cause; the linear uplink upsampler is a separate ASR-quality nicety, deferred.
- **Uplink echo gating** (prototype `live_mic_gate`) — the acoustic echo seen in tests is a speakerphone artifact; production handsets don't have it. Conflicts with kutsu's "never gate the uplink" rule. Deferred as a design question.
- **RTP-layer quality** (jitter/loss/RTT on the phone leg) — not measured by the prototype either; the pacer-layer metrics are the proven signal. Future.
- **Rate/window abort** instead of cumulative — future refinement.
- Uplink `net_check` change — stays as the Gemini pre-call probe.

## Testing

- `pace.rs`: prefill-delays-playout; underrun-counted-only-while-expecting; starved_ms accrual; resume re-arm; clear re-arms prefill. Pure, table/step tests.
- `state.rs`: `set_quality` round-trip; `CallQuality` serializes.
- `config.rs`: `QualityConfig` env parsing + defaults.
- `engine.rs`: a `Quality` event updates the record; underruns ≥ threshold aborts (Failed + error); below threshold does not.
- `mcp_http.rs`: `render_prometheus` includes the new series.
- Live `#[ignore]`: unchanged; manual confirm that a real call now has few/zero underruns (smooth audio) and the per-call quality line prints.
