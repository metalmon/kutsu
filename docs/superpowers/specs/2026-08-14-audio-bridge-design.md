# Design: `src/bridge` — audio bridge (phone G.711 ⇄ Gemini PCM16)

Status: approved for implementation planning
Date: 2026-08-14
Depends on: `src/sip` (`SipCall`, merged) and `src/gemini_live` (`Session`, merged).

## 1. Purpose & boundaries

`bridge` connects one live phone call (`SipCall`, G.711 8 kHz) to one Gemini Live
`Session` (PCM16 16 kHz in / 24 kHz out): it converts codecs and sample rates,
paces the phone-bound audio, and handles barge-in — until either side ends.

Transport/DSP only. Explicit non-responsibilities:
- **Call orchestration** (dial → bridge → hang up → finalize, transcript
  persistence) — owned by `engine` (phase 4).
- **SIP/RTP** — owned by `sip`. **Gemini protocol** — owned by `gemini_live`.

## 2. Hard constraint #1 — the uplink to Gemini must never be manipulated

Field experience (kutsu Python prototype, see [[voice-cloud-reference]]): **any
manipulation of the audio stream *to* Gemini — a gap, a stop/restart, gating,
reframing that introduces discontinuity — drops the Gemini Live session.**

Therefore the **uplink (phone → Gemini) is a transparent, continuous passthrough**:
- Decode G.711 → resample 8 k→16 k → forward **every** inbound RTP frame, in
  order, at the phone's natural 20 ms cadence.
- **No** VAD/silence-gating, **no** noise processing, **no** dropping frames,
  **no** coalescing that creates gaps, **no** pausing/restarting the stream.
- The uplink runs continuously from answer to call end; it stops only when the
  call ends.
- On backpressure (Gemini's `audio_in` channel full) the uplink **awaits** — it
  MUST NOT drop uplink frames (a drop = a gap = a session-drop risk). Persistent
  backpressure is surfaced as a warning, never papered over by dropping.

All buffering, flushing, and barge-in logic lives **only on the downlink**
(Gemini → phone). If inbound phone RTP itself underruns (genuine packet loss —
rare with Asterisk/real trunks that send continuous RTP), the mitigation is a
20 ms uplink pacer that emits silence to preserve continuity; this is a
documented fallback, not built in the first cut (reactive forward is the default).

## 3. Modules & file layout

- `src/bridge/mod.rs` — public `run(...)` loop + `BridgeEnd`; wires the two
  directions and barge-in.
- `src/bridge/g711.rs` — G.711 µ-law/a-law encode/decode (pure).
- `src/bridge/resample.rs` — `up_8k_16k`, `down_24k_8k` (pure), each behind a
  plain function boundary so a higher-quality impl (e.g. the `rubato` crate) can
  replace it later without touching callers.
- `src/bridge/pace.rs` — the downlink jitter buffer + 20 ms pacer (pure logic:
  push 24 k samples, pull one 8 k/20 ms frame, silence on underrun).

Split so each unit has one job and is independently testable.

## 4. Data flow

Frame math: phone = G.711 8 kHz, 20 ms = **160 samples/frame**. Gemini in =
16 kHz (320 samples/20 ms). Gemini out = 24 kHz (480 samples/20 ms).

### Uplink — phone → Gemini (transparent, reactive, per §2)
```
SipCall.audio_in (Bytes, ~160B G.711)
  → g711::decode(kind)         -> [i16; 160] @ 8 kHz
  → resample::up_8k_16k        -> [i16; 320] @ 16 kHz
  → gemini_audio_in.send(Vec<i16>)   (await on backpressure; never drop)
```
Loop ends when `SipCall.audio_in` closes (call gone) or the Gemini sink closes.

### Downlink — Gemini → phone (buffered + paced + barge-in)
```
gemini_events.recv():
  Event::OutputAudio(Vec<i16> @ 24 kHz)  -> append to downlink sample buffer
  Event::Interrupted (barge-in)          -> CLEAR the downlink buffer (stop agent voice now)
  Event::{Transcript,TurnComplete,EndCall,Warning} -> forward to `events_out` (engine)

20 ms pacer tick:
  take 480 samples @24 kHz from buffer (or silence if <480 -> underrun)
    → resample::down_24k_8k    -> [i16; 160] @ 8 kHz
    → g711::encode(kind)       -> [u8; 160]
    → SipCall.audio_out.send(Bytes)
```
The pacer always emits a frame every 20 ms so RTP never stalls; underrun sends
G.711 silence (`0xFF` µ-law / `0xD5` a-law). Barge-in clears the buffer so the
next ticks send silence until fresh `OutputAudio` arrives — the caller stops
hearing the agent immediately.

## 5. Public interface

Channel-based (not the concrete `Session`/`SipCall` wrappers) so the run-loop is
testable with plain in-memory channels — no ezk, no WebSocket. The engine adapts
`SipCall`/`Session` to these handles in phase 4.

```rust
use crate::gemini_live::Event;
use crate::sip::G711Kind;

/// Everything the bridge needs for one call. Engine builds this from a
/// SipCall + a gemini Session.
pub struct BridgePorts {
    pub codec: G711Kind,                         // from SipCall's Answered event
    // phone side (from SipCall)
    pub phone_in:  mpsc::Receiver<Bytes>,        // inbound G.711 (remote -> us)
    pub phone_out: mpsc::Sender<Bytes>,          // outbound G.711 (us -> remote)
    // gemini side (from Session)
    pub gemini_in:     mpsc::Sender<Vec<i16>>,   // uplink sink (PCM16 16 kHz)
    pub gemini_events: mpsc::Receiver<Event>,    // downlink + control
    pub events_out:    mpsc::Sender<Event>,      // non-audio events -> engine
}

/// Why the bridge stopped.
pub enum BridgeEnd { PhoneClosed, GeminiClosed }

/// Run both directions until one side ends. Cancel-safe at the await points.
pub async fn run(ports: BridgePorts) -> BridgeEnd;
```

Task structure (all handles are `Send`, so the bridge runs on the engine's
normal multi-thread runtime — no dedicated thread/`LocalSet` like `sip`):
- **Uplink task** (its own `tokio::spawn`): loops `phone_in.recv()` →
  decode/resample → `gemini_in.send().await`. It **awaits** on backpressure, so
  it must be a separate task — it must never stall the downlink.
- **Downlink task** (one `select!` over the shared buffer): `gemini_events.recv()`
  (push `OutputAudio` / clear on `Interrupted` / forward the rest) raced with the
  20 ms pacer tick (drain 480 → resample → encode → `phone_out.send()`).

`run` starts both and returns `PhoneClosed` when `phone_in` closes or `phone_out`
send fails, `GeminiClosed` when `gemini_events` closes (aborting the sibling
task). It does **not** hang up or join either side — the engine owns lifecycle.

## 6. Conversions (the pure units)

- **G.711** (`g711.rs`): standard ITU tables. `decode_ulaw(u8)->i16`,
  `encode_ulaw(i16)->u8`, `decode_alaw`, `encode_alaw`; frame helpers
  `decode(kind, &[u8]) -> Vec<i16>` / `encode(kind, &[i16]) -> Vec<u8>`.
- **Upsample 8→16 kHz** (`resample.rs`): linear interpolation (insert one
  interpolated sample between each pair). Adds no bandwidth (source is
  narrowband) — cheap and correct for the uplink.
- **Downsample 24→8 kHz** (`resample.rs`): fixed windowed-sinc FIR low-pass
  (cutoff ~3.4 kHz, telephony band) applied before ÷3 decimation, coefficients
  in a `const` table computed once. Anti-aliasing is the point — without it the
  agent's voice is gritty. Behind `down_24k_8k(&[i16]) -> Vec<i16>` so it can be
  swapped for `rubato` if our ears demand it.

## 7. Testing

Pure units (deterministic, no I/O):
- `g711`: assert against ITU reference vectors; `encode(decode(x))` round-trips
  within G.711 quantization; silence bytes map correctly.
- `resample::up_8k_16k`: output length `2n-1`/`2n`, interpolated values are the
  pairwise means; a constant-DC input stays constant.
- `resample::down_24k_8k`: a <3 kHz tone passes with little attenuation; a
  >4 kHz tone is strongly attenuated (anti-alias proof); output length ≈ n/3.
- `pace`: pushing 480 samples yields exactly one 160-sample 8 kHz frame; underrun
  yields a silence frame; barge-in clear drops buffered audio.

Run-loop (`run`) with fake in-memory channels (no ezk/WebSocket):
- Uplink transparency: feed N G.711 frames into `phone_in`, assert N PCM16
  16 kHz frames arrive on `gemini_in` in order, none dropped.
- Downlink pacing: push `OutputAudio` bursts into `gemini_events`, assert
  `phone_out` receives steady 160-byte frames at the pace (drive time with
  `tokio::time` pause/advance).
- Barge-in: after buffering audio, send `Interrupted`, assert subsequent frames
  are silence until new `OutputAudio`.
- Event forwarding: `Transcript`/`EndCall` arrive on `events_out`.
- End conditions: closing `phone_in` → `PhoneClosed`; closing `gemini_events` →
  `GeminiClosed`.

## 8. Out of scope / deferred (documented seams, not built now)

- Acoustic echo cancellation, AGC, noise suppression (would be uplink
  manipulation — explicitly avoided per §2).
- Comfort-noise generation / DTX; adaptive jitter buffer (fixed small buffer for
  now). The uplink silence-pacer fallback (§2) if uplink underruns are observed.
- `rubato` (or other) high-quality resampler — the function boundaries in
  `resample.rs` make it a localized swap.
- DTMF audio detection (belongs with the sip DTMF seam).
