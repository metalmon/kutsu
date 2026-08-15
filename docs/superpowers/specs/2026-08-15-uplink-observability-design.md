# Uplink observability: RTP loss/reorder metrics + audio dump — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-15
**Scope:** `src/sip/call.rs` (per-packet RTP accounting), `src/sip/mod.rs` (`UplinkQualityShared`, `SipCallParts` field), `src/bridge/mod.rs` (uplink WAV dump), `src/engine.rs` (merge uplink into `CallQuality`, cumulative counters), `src/state.rs` (`CallQuality` uplink fields), `src/config.rs` + `src/main_support.rs` (`dump_uplink_dir` env), `src/mcp_http.rs` (Prometheus series).

## Problem (diagnosed by symptom, not yet by metric)

Recognition (Gemini ASR) degraded after switching the phone side from MicroSIP-on-host to Linphone-from-a-phone-over-WiFi. The resampler code is identical across both softphones, so it cannot explain a regression that appeared only on the switch — `rubato`/resampling is a dead end here. The plausible cause is **uplink network impairment** (packet loss / jitter / reorder over WiFi) that the clean localhost path never had: the uplink is a "transparent, continuous, never-manipulated forward" (`bridge/mod.rs:92`) with **no jitter buffer, no RTP-sequence reordering, no PLC**, so lost/late/out-of-order RTP becomes choppy audio that Gemini hears and mis-transcribes.

Two observability gaps block confirmation:
1. **Logs are not persisted** — `tracing` goes to stderr, `transcript_dir` defaults to `None`; past calls left nothing to inspect.
2. **The existing quality metrics (`underruns`/`starved_ms`/`max_gap_ms`) measure only the downlink pacer** — they are blind to uplink loss.

This feature closes the uplink measurement gap so the **next** Linphone call yields proof, and captures the incoming audio for offline listening. It is **diagnostic only** — no jitter buffer / PLC / RTCP is built here; those are treated as a possible *fix* to be decided after the data confirms the cause.

## Decisions

- Feasibility confirmed: `ezk_rtc::RtpPacket` (`rtp/rtp_packet.rs:6`) exposes public `sequence_number`, `timestamp`, `marker`, `payload`. Today `call.rs:242` reads only `.payload`; the sequence number is available and discarded. No ezk change needed.
- **RFC 3550 loss accounting**, not naive gap counting: track an extended (wrap-aware) sequence number, `first_ext`, `max_ext`, `received`. Loss is derived as `(max_ext − first_ext + 1) − received`. This is correct under reordering (a late packet does not inflate loss) where a "count the hole" approach over-reports.
- **Surface: logs + status JSON + Prometheus** (user choice). Uplink stats cross the SIP-thread → engine boundary via an `Arc` shared handle mirroring the existing `QualityShared` (single-writer in the SIP receive loop, single-reader in the engine `qtick`).
- **Audio dump gated by env** `KUTSU_DUMP_UPLINK_DIR`, mirroring the existing `transcript_dir` config seam. Dump is decoded **PCM16 @ 8 kHz** (openable in any player), not raw µ-law.
- The per-call summary log line includes the **negotiated codec (µ-law/A-law)** and loss %, so it simultaneously answers the secondary hypothesis (a µ-law-vs-A-law path difference between the two softphones).

## Component 1 — `UplinkStats` (pure accounting)

A plain struct, no atomics, no I/O; the SIP receive loop is its sole writer. Handles the 16-bit RTP sequence space with explicit cycle tracking.

Fields:
- `first_ext: Option<u64>` — extended seq of the first observed packet (baseline).
- `max_ext: u64` — highest extended seq seen.
- `cycles: u64`, `prev_seq: u16` — wrap tracking to extend a raw `u16` to `u64`.
- `received: u64` — packets actually delivered.
- `reordered: u64` — packets whose seq is ≤ a previously seen max (late/out-of-order).
- `duplicated: u64` — exact repeat of the last-seen seq.

`observe(seq: u16)`:
1. First packet: set baseline (`first_ext = max_ext = extend(seq)`, `received = 1`, `prev_seq = seq`). Return.
2. Extend `seq` to `ext` via cycle tracking: if `seq < prev_seq` and the drop looks like a forward wrap (not a small backward step), increment `cycles`. Compute `ext = cycles * 2^16 + seq` with the standard wrap heuristic.
3. `received += 1`. If `ext > max_ext`: forward progress, `max_ext = ext`. If `ext == max_ext` (or `== prev`): `duplicated += 1`. If `ext < max_ext`: `reordered += 1`.
4. `prev_seq = seq`.

`snapshot() -> UplinkQuality { received, lost, reordered }` where `lost = (max_ext − first_ext + 1).saturating_sub(received)`; `0` before the first packet.

**Testable pure logic** (unit tests, no IO): in-order stream → `lost == 0`; a single gap of N → `lost == N`; a reordered late packet after a gap → `lost` not inflated, `reordered == 1`; a duplicate → `duplicated == 1`, `lost` unchanged; wraparound 65534→65535→0→1 → `lost == 0`; empty → all zero.

## Component 2 — `UplinkQualityShared` + wiring (`sip`)

Mirror of `bridge::QualityShared`: an `Arc` of three `AtomicU64` (`received`, `lost`, `reordered`) with `snapshot() -> UplinkQuality` and a `publish(&UplinkQuality)` (relaxed stores). Created at call setup in `sip/mod.rs`, cloned into the receive task, and exposed on `SipCallParts` as `uplink_quality: Arc<UplinkQualityShared>`.

Receive loop (`call.rs:240`): before forwarding, `stats.observe(rtp.sequence_number.0); uplink_quality.publish(&stats.snapshot());` then the existing `in_tx.try_send(rtp.payload)`. `UplinkStats` is owned locally by the loop (single-threaded mutation); only the derived snapshot is published across the boundary.

## Component 3 — `CallQuality` extension + engine merge

`state::CallQuality` gains three flat fields (matching the existing `underruns`/`starved_ms`/`max_gap_ms` style): `uplink_received: u64`, `uplink_lost: u64`, `uplink_reordered: u64`. Loss % is derived at render time, not stored.

`engine::run_call`: take `uplink_quality` from `SipCallParts`. In the existing `qtick` arm, snapshot it alongside the downlink `quality` and write both into the record via `set_quality` (the `CallQuality` it builds now carries uplink fields too). At finalize, the same once-per-call snapshot fills the final record and bumps new cumulative counters.

`engine::Counters` gains `uplink_received: AtomicU64`, `uplink_lost: AtomicU64`, bumped **once at finalize** (consistent with `underruns`/`starved_ms` totals, not per-tick). `MetricsSnapshot` gains `uplink_received_total`, `uplink_lost_total`.

## Component 4 — Uplink audio dump (`bridge`)

`ServerConfig` gains `dump_uplink_dir: Option<PathBuf>` (mirrors `transcript_dir`), read in `main_support.rs` from `KUTSU_DUMP_UPLINK_DIR`. The engine passes a resolved per-call path into `BridgePorts` as `uplink_dump: Option<PathBuf>` = `dir.join(format!("{call_id}-uplink.wav"))`.

The bridge uplink task (`mod.rs:94`), which already decodes `pcm8` at line 96, opens a `Pcm16Writer::create(path, 8000)` when `uplink_dump` is `Some`, writes each decoded `pcm8` block, and `finalize()`s on task exit. When `None` (default), the task is byte-for-byte unchanged. The dump is the *pre-resample* 8 kHz phone audio — exactly what arrived, so gaps/artifacts are audible as the phone side sent them.

## Component 5 — Prometheus (`mcp_http`)

`render_prometheus` gains two series mirroring the downlink ones: `kutsu_uplink_received_total` and `kutsu_uplink_lost_total` (from `MetricsSnapshot`). `get_call_status` / transcript JSON already serialize `CallQuality`, so the new per-call fields appear there automatically.

## Error handling & edge cases

- First packet is the baseline; loss is not counted against it.
- Wraparound 65535→0 handled by cycle tracking; the backward-vs-wrap decision uses the standard RFC 3550 heuristic (a large negative raw delta is a forward wrap, a small one is reorder).
- Reorder and duplicate are counted separately and do **not** inflate `lost` (that is the point of the RFC 3550 derivation).
- `try_send` drop-on-full stays as-is (a stalled-bridge backstop); such a drop is a *local* loss not visible in RTP seq, so it is out of scope for this metric and noted as a known limitation.
- WAV dump I/O errors are logged and swallowed — dumping never affects the call or the uplink forward path.

## Testing

- `UplinkStats`: in-order / single gap / reorder-after-gap / duplicate / wraparound / empty (Component 1 list).
- `UplinkQualityShared`: `publish` then `snapshot` round-trips the values.
- Prometheus: a `uplink_series_present` test asserts both new series render (mirrors `prometheus_has_quality_series`).
- WAV dump: with `uplink_dump = Some(path)`, the bridge uplink task creates a readable PCM16 file; with `None`, no file is written.

## Extension seams (explicitly out of scope now)

- **Uplink jitter buffer / PLC** — the likely *fix* if loss is confirmed; deliberately not built until the data justifies it.
- **RTCP receiver reports** — richer loss/jitter, not needed for the local diagnosis.
- **RTP timestamp-gap analysis** — sequence-based loss is sufficient here.
