# Audio Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `src/bridge` — convert audio between one phone call (`SipCall`, G.711 8 kHz) and one Gemini `Session` (PCM16 16 kHz in / 24 kHz out), pace the phone-bound stream, handle barge-in.

**Architecture:** Pure DSP units (`g711`, `resample`, `pace`) + an all-`Send` `run()` loop that runs on the engine's normal runtime (no dedicated thread — unlike `sip`). Two directions: a transparent continuous uplink task (phone→Gemini, never manipulated), and a buffered/paced downlink (Gemini→phone) driven by a 20 ms ticker with barge-in flush.

**Tech Stack:** Rust 2024, tokio (`mpsc`, `time`), `bytes`. **No new dependencies** — DSP is hand-rolled. Resamplers sit behind function/struct boundaries so `rubato` can replace them later.

**Spec:** `docs/superpowers/specs/2026-08-14-audio-bridge-design.md`

## Global Constraints

- **Every `cargo` build/test command MUST include `--features vendor-openssl`** (the whole crate links OpenSSL via ezk-rtc, even though `bridge` doesn't use it). Toolchain already configured.
- **Uplink to Gemini is NEVER manipulated** (spec §2): decode + resample + forward every inbound frame, in order, no gating/dropping/gaps/pauses. All buffering/flushing lives only on the downlink.
- English only in all code/comments.
- No new crates. Frame math: phone 20 ms = **160** G.711 bytes / 160 samples @8 kHz; Gemini in = **320** samples @16 kHz; Gemini out = **480** samples @24 kHz.
- `bridge` is DSP/transport only — no call orchestration, no SIP/Gemini protocol, no state persistence (those are `engine`/`sip`/`gemini_live`).

## File Structure

- `src/bridge.rs` → **split** into:
  - `src/bridge/mod.rs` — module doc + `mod` declarations + (Task 4) `BridgePorts`, `BridgeEnd`, `run()`.
  - `src/bridge/g711.rs` — G.711 µ-law/a-law codec (pure).
  - `src/bridge/resample.rs` — `up_8k_16k` (pure) + `Downsampler` (stateful).
  - `src/bridge/pace.rs` — `Downlink` (24 kHz jitter buffer + downsample to 8 kHz PCM frames).
- `src/lib.rs` — unchanged (`pub mod bridge;` resolves to `bridge/mod.rs`).

Dependencies between tasks: g711 ← nothing; resample ← nothing; pace ← resample (`Downsampler`); run ← g711 + resample + pace + `crate::sip::G711Kind` + `crate::gemini_live::Event`.

---

## Task 1: Module split + G.711 codec

**Files:**
- Move: `src/bridge.rs` → `src/bridge/mod.rs`
- Create: `src/bridge/g711.rs`
- Test: `src/bridge/g711.rs` (inline)

**Interfaces:**
- Consumes: `crate::sip::G711Kind` (`Ulaw`/`Alaw`).
- Produces: `bridge::g711::{decode_ulaw, encode_ulaw, decode_alaw, encode_alaw, decode, encode, silence_byte}`.

- [ ] **Step 1: Convert the module to a directory.**

```bash
mkdir src/bridge
git mv src/bridge.rs src/bridge/mod.rs
```

Add to the top of `src/bridge/mod.rs` (after the existing doc comment):

```rust
mod g711;
```

- [ ] **Step 2: Write the failing tests.** Create `src/bridge/g711.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sip::G711Kind;

    #[test]
    fn silence_anchors() {
        // Digital silence encodes to the canonical bytes and decodes back near zero.
        assert_eq!(encode_ulaw(0), 0xFF);
        assert_eq!(decode_ulaw(0xFF), 0);
        assert_eq!(silence_byte(G711Kind::Ulaw), 0xFF);

        assert_eq!(encode_alaw(0), 0xD5);
        assert!(decode_alaw(0xD5).abs() < 16); // a-law min step, ~silence
        assert_eq!(silence_byte(G711Kind::Alaw), 0xD5);
    }

    #[test]
    fn ulaw_decode_is_stable_under_reencode() {
        // decode->encode->decode must reproduce the decoded PCM for all 256 codes.
        for b in 0u8..=255 {
            let pcm = decode_ulaw(b);
            assert_eq!(decode_ulaw(encode_ulaw(pcm)), pcm, "ulaw byte {b:#04x}");
        }
    }

    #[test]
    fn alaw_decode_is_stable_under_reencode() {
        for b in 0u8..=255 {
            let pcm = decode_alaw(b);
            assert_eq!(decode_alaw(encode_alaw(pcm)), pcm, "alaw byte {b:#04x}");
        }
    }

    #[test]
    fn frame_helpers_roundtrip_length() {
        let pcm = [0i16, 100, -100, 5000, -5000];
        let enc = encode(G711Kind::Ulaw, &pcm);
        assert_eq!(enc.len(), pcm.len());
        let dec = decode(G711Kind::Ulaw, &enc);
        assert_eq!(dec.len(), pcm.len());
    }
}
```

- [ ] **Step 3: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib bridge::g711`
Expected: FAIL — functions not defined.

- [ ] **Step 4: Implement the codec.** Prepend to `src/bridge/g711.rs` (above the test module):

```rust
//! ITU-T G.711 companding (µ-law / a-law), ported from the public-domain
//! Sun/CCITT reference. Pure, no I/O.

use crate::sip::G711Kind;

const BIAS: i32 = 0x84;
const CLIP: i32 = 8159;
const SEG_UEND: [i32; 8] = [0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF];
const SEG_AEND: [i32; 8] = [0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF];

fn search(val: i32, table: &[i32; 8]) -> usize {
    for (i, &t) in table.iter().enumerate() {
        if val <= t {
            return i;
        }
    }
    table.len()
}

/// PCM16 -> µ-law.
pub fn encode_ulaw(pcm: i16) -> u8 {
    let mut pcm_val = (pcm as i32) >> 2; // 16-bit -> 14-bit
    let mask = if pcm_val < 0 {
        pcm_val = -pcm_val;
        0x7F
    } else {
        0xFF
    };
    if pcm_val > CLIP {
        pcm_val = CLIP;
    }
    pcm_val += BIAS >> 2;
    let seg = search(pcm_val, &SEG_UEND);
    if seg >= 8 {
        (0x7F ^ mask) as u8
    } else {
        let uval = (seg as i32) << 4 | ((pcm_val >> (seg + 1)) & 0xF);
        (uval ^ mask) as u8
    }
}

/// µ-law -> PCM16.
pub fn decode_ulaw(u: u8) -> i16 {
    let u = (!u) as i32;
    let mut t = ((u & 0x0F) << 3) + BIAS;
    t <<= (u & 0x70) >> 4;
    (if (u & 0x80) != 0 { BIAS - t } else { t - BIAS }) as i16
}

/// PCM16 -> a-law.
pub fn encode_alaw(pcm: i16) -> u8 {
    let mut pcm_val = (pcm as i32) >> 3; // 16-bit -> 13-bit
    let mask = if pcm_val >= 0 {
        0xD5
    } else {
        pcm_val = -pcm_val - 1;
        0x55
    };
    let seg = search(pcm_val, &SEG_AEND);
    if seg >= 8 {
        (0x7F ^ mask) as u8
    } else {
        let mut aval = (seg as i32) << 4;
        aval |= if seg < 2 {
            (pcm_val >> 1) & 0xF
        } else {
            (pcm_val >> seg) & 0xF
        };
        (aval ^ mask) as u8
    }
}

/// a-law -> PCM16.
pub fn decode_alaw(a: u8) -> i16 {
    let a = (a ^ 0x55) as i32;
    let mut t = (a & 0x0F) << 4;
    let seg = (a & 0x70) >> 4;
    match seg {
        0 => t += 8,
        1 => t += 0x108,
        _ => {
            t += 0x108;
            t <<= seg - 1;
        }
    }
    (if (a & 0x80) != 0 { t } else { -t }) as i16
}

/// Decode a G.711 payload to PCM16 samples.
pub fn decode(kind: G711Kind, payload: &[u8]) -> Vec<i16> {
    match kind {
        G711Kind::Ulaw => payload.iter().map(|&b| decode_ulaw(b)).collect(),
        G711Kind::Alaw => payload.iter().map(|&b| decode_alaw(b)).collect(),
    }
}

/// Encode PCM16 samples to a G.711 payload.
pub fn encode(kind: G711Kind, pcm: &[i16]) -> Vec<u8> {
    match kind {
        G711Kind::Ulaw => pcm.iter().map(|&s| encode_ulaw(s)).collect(),
        G711Kind::Alaw => pcm.iter().map(|&s| encode_alaw(s)).collect(),
    }
}

/// The byte that encodes digital silence for this codec.
pub fn silence_byte(kind: G711Kind) -> u8 {
    match kind {
        G711Kind::Ulaw => 0xFF,
        G711Kind::Alaw => 0xD5,
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib bridge::g711`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit.**

```bash
git add src/bridge/
git commit -m "feat(bridge): split module; add G.711 mu-law/a-law codec"
```

---

## Task 2: Resamplers (`up_8k_16k` + stateful `Downsampler`)

**Files:**
- Create: `src/bridge/resample.rs`
- Modify: `src/bridge/mod.rs` (add `mod resample;`)
- Test: `src/bridge/resample.rs` (inline)

**Interfaces:**
- Produces: `bridge::resample::up_8k_16k(&[i16]) -> Vec<i16>`; `bridge::resample::Downsampler` with `new()` and `process(&mut self, &[i16]) -> Vec<i16>`.

- [ ] **Step 1: Declare the module.** Add to `src/bridge/mod.rs`:

```rust
mod resample;
```

- [ ] **Step 2: Write the failing tests.** Create `src/bridge/resample.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsample_doubles_and_interpolates() {
        let out = up_8k_16k(&[0, 100]);
        // [a, mean(a,b), b, hold(b)]
        assert_eq!(out, vec![0, 50, 100, 100]);
        assert_eq!(up_8k_16k(&[0; 160]).len(), 320);
    }

    #[test]
    fn upsample_preserves_dc() {
        let out = up_8k_16k(&[1000; 10]);
        assert!(out.iter().all(|&s| s == 1000));
    }

    #[test]
    fn downsample_thirds_length_and_dc() {
        let mut d = Downsampler::new();
        let out = d.process(&[2000i16; 480]);
        assert_eq!(out.len(), 160);
        // After the filter warms up, DC passes through (gain ~1).
        let tail = &out[80..];
        assert!(tail.iter().all(|&s| (s - 2000).abs() < 60), "DC not preserved: {tail:?}");
    }

    fn tone(freq: f32, n: usize) -> Vec<i16> {
        (0..n)
            .map(|i| {
                let t = i as f32 / 24000.0;
                ((2.0 * std::f32::consts::PI * freq * t).sin() * 10000.0) as i16
            })
            .collect()
    }

    fn rms(s: &[i16]) -> f32 {
        (s.iter().map(|&x| (x as f32).powi(2)).sum::<f32>() / s.len() as f32).sqrt()
    }

    #[test]
    fn downsample_is_antialiasing() {
        // A 1 kHz tone (in-band) passes; a 6 kHz tone (above the 4 kHz Nyquist of
        // 8 kHz) is strongly attenuated.
        let mut d1 = Downsampler::new();
        let pass = d1.process(&tone(1000.0, 2400));
        let mut d2 = Downsampler::new();
        let block = d2.process(&tone(6000.0, 2400));
        // Compare steady-state RMS (skip warm-up).
        let pass_rms = rms(&pass[40..]);
        let block_rms = rms(&block[40..]);
        assert!(pass_rms > 3000.0, "in-band tone attenuated: {pass_rms}");
        assert!(block_rms < pass_rms * 0.2, "out-of-band not attenuated: pass={pass_rms} block={block_rms}");
    }

    #[test]
    fn downsample_is_continuous_across_blocks() {
        // Processing [A|B] in two calls must equal processing A++B in one call.
        let a = tone(1000.0, 480);
        let b = tone(1000.0, 480);
        let mut split = Downsampler::new();
        let mut out_split = split.process(&a);
        out_split.extend(split.process(&b));

        let mut whole = Downsampler::new();
        let combined: Vec<i16> = a.iter().chain(b.iter()).copied().collect();
        let out_whole = whole.process(&combined);

        assert_eq!(out_split, out_whole);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib bridge::resample`
Expected: FAIL — `up_8k_16k`/`Downsampler` not defined.

- [ ] **Step 4: Implement.** Prepend to `src/bridge/resample.rs`:

```rust
//! Sample-rate conversion. `up_8k_16k` is a pure per-frame linear upsampler (the
//! uplink to Gemini — a robust listener). `Downsampler` is stateful (carries FIR
//! history across the pacer's 20 ms blocks) so the human-audible downlink has no
//! periodic edge transient. Both sit behind these boundaries so a higher-quality
//! impl (e.g. `rubato`) can replace them without touching callers.

use std::sync::OnceLock;

/// Upsample 8 kHz -> 16 kHz by linear interpolation. Output length = 2 * input.
pub fn up_8k_16k(input: &[i16]) -> Vec<i16> {
    let mut out = Vec::with_capacity(input.len() * 2);
    for i in 0..input.len() {
        let cur = input[i] as i32;
        let next = if i + 1 < input.len() {
            input[i + 1] as i32
        } else {
            cur // hold the last sample (frame boundary)
        };
        out.push(cur as i16);
        out.push(((cur + next) / 2) as i16);
    }
    out
}

/// Windowed-sinc low-pass FIR (cutoff ~3.4 kHz at 24 kHz), computed once.
fn lowpass() -> &'static [f32] {
    static C: OnceLock<Vec<f32>> = OnceLock::new();
    C.get_or_init(|| {
        const N: usize = 23; // taps (odd)
        let fc_norm = 3400.0f32 / 24000.0;
        let mid = (N as f32 - 1.0) / 2.0;
        let pi = std::f32::consts::PI;
        let mut h = vec![0f32; N];
        let mut sum = 0f32;
        for i in 0..N {
            let x = i as f32 - mid;
            let sinc = if x == 0.0 {
                2.0 * fc_norm
            } else {
                (2.0 * pi * fc_norm * x).sin() / (pi * x)
            };
            let w = 0.54 - 0.46 * (2.0 * pi * i as f32 / (N as f32 - 1.0)).cos(); // Hamming
            h[i] = sinc * w;
            sum += h[i];
        }
        for v in &mut h {
            *v /= sum; // normalize DC gain to 1
        }
        h
    })
}

/// Stateful 24 kHz -> 8 kHz downsampler: FIR low-pass then /3 decimation.
pub struct Downsampler {
    win: Vec<i16>, // sliding window of the last `taps` samples
    phase: usize,  // input-sample counter mod 3; emit when 0
}

impl Downsampler {
    pub fn new() -> Self {
        Self {
            win: vec![0; lowpass().len()],
            phase: 0,
        }
    }

    /// Feed a block of 24 kHz samples; return the decimated 8 kHz samples.
    pub fn process(&mut self, block: &[i16]) -> Vec<i16> {
        let h = lowpass();
        let taps = h.len();
        let mut out = Vec::with_capacity(block.len() / 3 + 1);
        for &s in block {
            self.win.copy_within(1..taps, 0);
            self.win[taps - 1] = s;
            if self.phase == 0 {
                let mut acc = 0f32;
                for j in 0..taps {
                    acc += h[j] * self.win[j] as f32;
                }
                out.push(acc.round().clamp(-32768.0, 32767.0) as i16);
            }
            self.phase = (self.phase + 1) % 3;
        }
        out
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib bridge::resample`
Expected: PASS (5 tests). If `downsample_is_antialiasing` tolerances are marginal, the FIR is correct — adjust the assertion thresholds slightly, not the filter, and note it in the report.

- [ ] **Step 6: Commit.**

```bash
git add src/bridge/
git commit -m "feat(bridge): 8->16k upsampler + stateful anti-aliasing 24->8k downsampler"
```

---

## Task 3: Downlink jitter buffer + pacer (`pace.rs`)

**Files:**
- Create: `src/bridge/pace.rs`
- Modify: `src/bridge/mod.rs` (add `mod pace;`)
- Test: `src/bridge/pace.rs` (inline)

**Interfaces:**
- Consumes: `resample::Downsampler` (Task 2).
- Produces: `bridge::pace::Downlink` with `new()`, `push(&mut self, &[i16])`, `clear(&mut self)`, `next_frame(&mut self) -> Vec<i16>` (always 160 samples @8 kHz).

- [ ] **Step 1: Declare the module.** Add to `src/bridge/mod.rs`:

```rust
mod pace;
```

- [ ] **Step 2: Write the failing tests.** Create `src/bridge/pace.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underrun_yields_silence_frame() {
        let mut d = Downlink::new();
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        assert!(f.iter().all(|&s| s.abs() < 16), "empty buffer should be ~silence");
    }

    #[test]
    fn buffered_audio_plays_out() {
        let mut d = Downlink::new();
        d.push(&[8000i16; 480 * 2]); // 40 ms of loud DC @24k
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        // After warm-up the frame carries the signal, not silence.
        assert!(f[80..].iter().any(|&s| s.abs() > 2000), "buffered audio not played");
    }

    #[test]
    fn clear_flushes_pending_audio() {
        let mut d = Downlink::new();
        d.push(&[8000i16; 480 * 4]);
        let _ = d.next_frame(); // consume some
        d.clear(); // barge-in
        let f = d.next_frame();
        assert!(f.iter().all(|&s| s.abs() < 16), "after clear should be silence");
    }

    #[test]
    fn each_frame_consumes_20ms() {
        let mut d = Downlink::new();
        d.push(&[1000i16; 480 * 3]); // exactly 3 frames' worth
        for _ in 0..3 {
            assert_eq!(d.next_frame().len(), 160);
        }
        // Buffer now drained -> silence.
        let f = d.next_frame();
        assert!(f.iter().all(|&s| s.abs() < 16));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib bridge::pace`
Expected: FAIL — `Downlink` not defined.

- [ ] **Step 4: Implement.** Prepend to `src/bridge/pace.rs`:

```rust
//! Downlink (Gemini -> phone) jitter buffer + 20 ms pacer. Holds 24 kHz PCM,
//! feeds the stateful downsampler one 480-sample (20 ms) block per frame, and
//! emits 160-sample 8 kHz frames. On underrun the block is zero-padded (silence)
//! — the downsampler is always fed 480 samples so its filter state stays
//! continuous. `clear()` is barge-in: drop everything buffered.
//!
//! First cut has no pre-buffer/re-buffering (spec §8 seam): it plays whatever is
//! queued and fills the rest with silence. If we hear choppiness under real
//! Gemini bursts, add a small pre-buffer threshold here.

use std::collections::VecDeque;

use super::resample::Downsampler;

const IN_PER_FRAME: usize = 480; // 20 ms @ 24 kHz

pub struct Downlink {
    buf: VecDeque<i16>,
    down: Downsampler,
}

impl Downlink {
    pub fn new() -> Self {
        Self {
            buf: VecDeque::new(),
            down: Downsampler::new(),
        }
    }

    /// Append Gemini output (PCM16 @ 24 kHz).
    pub fn push(&mut self, samples: &[i16]) {
        self.buf.extend(samples.iter().copied());
    }

    /// Barge-in: drop all buffered audio so the agent stops talking now.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Produce one 20 ms frame (160 samples @ 8 kHz). Underrun -> zero-padded.
    pub fn next_frame(&mut self) -> Vec<i16> {
        let mut block = [0i16; IN_PER_FRAME];
        for slot in block.iter_mut() {
            if let Some(s) = self.buf.pop_front() {
                *slot = s;
            } // else leave 0 (silence)
        }
        self.down.process(&block)
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib bridge::pace`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit.**

```bash
git add src/bridge/
git commit -m "feat(bridge): downlink jitter buffer + 20ms pacer with barge-in"
```

---

## Task 4: The `run()` loop (`BridgePorts`, `BridgeEnd`)

**Files:**
- Modify: `src/bridge/mod.rs`
- Test: `src/bridge/mod.rs` (inline)

**Interfaces:**
- Consumes: `g711`, `resample::up_8k_16k`, `pace::Downlink` (Tasks 1–3), `crate::sip::G711Kind`, `crate::gemini_live::Event`.
- Produces: `bridge::{BridgePorts, BridgeEnd, run}`.

- [ ] **Step 1: Write the failing tests.** Add to `src/bridge/mod.rs` an inline test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::gemini_live::Event;
    use crate::sip::G711Kind;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    // Build ports + keep the far ends for the test to drive.
    struct Ends {
        phone_in_tx: mpsc::Sender<Bytes>,
        phone_out_rx: mpsc::Receiver<Bytes>,
        gemini_in_rx: mpsc::Receiver<Vec<i16>>,
        gemini_events_tx: mpsc::Sender<Event>,
        events_out_rx: mpsc::Receiver<Event>,
    }

    fn wire(codec: G711Kind) -> (BridgePorts, Ends) {
        let (phone_in_tx, phone_in) = mpsc::channel(64);
        let (phone_out, phone_out_rx) = mpsc::channel(64);
        let (gemini_in, gemini_in_rx) = mpsc::channel(64);
        let (gemini_events_tx, gemini_events) = mpsc::channel(64);
        let (events_out, events_out_rx) = mpsc::channel(64);
        (
            BridgePorts { codec, phone_in, phone_out, gemini_in, gemini_events, events_out },
            Ends { phone_in_tx, phone_out_rx, gemini_in_rx, gemini_events_tx, events_out_rx },
        )
    }

    #[tokio::test(start_paused = true)]
    async fn uplink_forwards_every_frame_transparently() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Send 5 frames of 160 G.711 silence bytes.
        for _ in 0..5 {
            ends.phone_in_tx.send(Bytes::from(vec![0xFFu8; 160])).await.unwrap();
        }
        // Each becomes one PCM16 16 kHz frame of 320 samples, in order, none dropped.
        for _ in 0..5 {
            let frame = ends.gemini_in_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 320);
        }
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn downlink_paces_frames_to_phone() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Push 60 ms of loud audio.
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 3])).await.unwrap();
        // Advance three 20 ms ticks; expect three 160-byte frames.
        for _ in 0..3 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let frame = ends.phone_out_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 160);
        }
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn barge_in_silences_the_phone() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 4])).await.unwrap();
        ends.gemini_events_tx.send(Event::Interrupted).await.unwrap();
        // Give the loop a moment to process both events before the first tick.
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        let frame = ends.phone_out_rx.recv().await.unwrap();
        // After barge-in the buffer is cleared -> silence.
        let pcm = g711::decode(G711Kind::Ulaw, &frame);
        assert!(pcm.iter().all(|&s| s.abs() < 64), "expected silence after barge-in");
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn non_audio_events_forwarded_to_engine() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        ends.gemini_events_tx.send(Event::TurnComplete).await.unwrap();
        let got = ends.events_out_rx.recv().await.unwrap();
        assert!(matches!(got, Event::TurnComplete));
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn ends_when_gemini_closes() {
        let (ports, ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Drop the gemini events sender -> gemini_events.recv() returns None.
        drop(ends.gemini_events_tx);
        let end = h.await.unwrap();
        assert!(matches!(end, BridgeEnd::GeminiClosed));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib bridge::tests`
Expected: FAIL — `BridgePorts`/`run`/`BridgeEnd` not defined.

- [ ] **Step 3: Implement `run()`.** Add to `src/bridge/mod.rs` (below the `mod` declarations; imports at the top of the file):

```rust
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::gemini_live::Event;
use crate::sip::G711Kind;

/// Everything the bridge needs for one call. The engine builds this from a
/// `SipCall` (phone side) and a gemini `Session` (gemini side).
pub struct BridgePorts {
    pub codec: G711Kind,
    /// Inbound G.711 payloads from the phone (remote -> us).
    pub phone_in: mpsc::Receiver<Bytes>,
    /// Outbound G.711 payloads to the phone (us -> remote).
    pub phone_out: mpsc::Sender<Bytes>,
    /// Uplink sink to Gemini (PCM16 @ 16 kHz).
    pub gemini_in: mpsc::Sender<Vec<i16>>,
    /// Downlink + control events from Gemini.
    pub gemini_events: mpsc::Receiver<Event>,
    /// Non-audio events forwarded to the engine.
    pub events_out: mpsc::Sender<Event>,
}

/// Why the bridge stopped.
#[derive(Debug)]
pub enum BridgeEnd {
    /// The phone side ended (RTP receiver closed, or send to phone failed).
    PhoneClosed,
    /// The Gemini side ended (event stream closed).
    GeminiClosed,
}

/// Bridge one call until a side ends. Does not hang up or join either side —
/// the engine owns lifecycle. Cancel-safe at its await points.
pub async fn run(ports: BridgePorts) -> BridgeEnd {
    let BridgePorts {
        codec,
        mut phone_in,
        phone_out,
        gemini_in,
        mut gemini_events,
        events_out,
    } = ports;

    // Uplink: transparent, continuous, never-manipulated forward (spec §2).
    // Separate task so its backpressure `await` can never stall the downlink.
    let uplink = tokio::spawn(async move {
        while let Some(payload) = phone_in.recv().await {
            let pcm8 = g711::decode(codec, &payload);
            let pcm16 = resample::up_8k_16k(&pcm8);
            if gemini_in.send(pcm16).await.is_err() {
                break; // gemini sink closed
            }
        }
    });
    tokio::pin!(uplink);

    // Downlink: 24 kHz buffer + 20 ms pacer + barge-in, on this task.
    let mut downlink = pace::Downlink::new();
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));

    loop {
        tokio::select! {
            _ = &mut uplink => {
                // Uplink task ended: the phone stopped feeding us (hang-up).
                return BridgeEnd::PhoneClosed;
            }
            ev = gemini_events.recv() => match ev {
                Some(Event::OutputAudio(pcm24)) => downlink.push(&pcm24),
                Some(Event::Interrupted) => downlink.clear(),
                Some(other) => {
                    if events_out.send(other).await.is_err() {
                        // Engine dropped its event receiver; keep bridging audio.
                    }
                }
                None => return BridgeEnd::GeminiClosed,
            },
            _ = ticker.tick() => {
                let pcm8 = downlink.next_frame();
                let payload = g711::encode(codec, &pcm8);
                if phone_out.send(Bytes::from(payload)).await.is_err() {
                    return BridgeEnd::PhoneClosed;
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib bridge::tests`
Expected: PASS (5 tests). Note: `tokio::time::interval`'s first `tick()` completes immediately; under `start_paused` the tests `advance` time to fire subsequent ticks — if the first frame arrives before an `advance`, that is the immediate first tick and is expected.

- [ ] **Step 5: Run the whole suite (no regressions).**

Run: `cargo test --features vendor-openssl --lib`
Expected: PASS (all prior tests + the new bridge tests).

- [ ] **Step 6: Commit.**

```bash
git add src/bridge/
git commit -m "feat(bridge): run() loop wiring uplink passthrough + paced downlink"
```

---

## Self-Review

**Spec coverage:**
- §1 boundaries (DSP/transport only) → all four tasks stay within conversion + pacing; no orchestration/state. ✔
- §2 uplink never manipulated → Task 4 uplink task is decode+resample+forward, no gating/drop; `await`s on backpressure. ✔
- §3 module layout (`mod`/`g711`/`resample`/`pace`) → Tasks 1–4. ✔
- §4 data-flow math (160/320/480) → g711 frame helpers, `up_8k_16k` (×2), `Downsampler` (÷3), pacer 480→160. ✔
- §5 interface (`BridgePorts`/`BridgeEnd`/`run`, channel-based) → Task 4; testable with fakes. ✔
- §6 conversions (G.711 tables; linear up; stateful anti-alias down behind a boundary) → Tasks 1–2. ✔
- §7 tests (ITU-ish anchors + stability; resampler length/DC/anti-alias/continuity; pace underrun/play/clear; run transparency/pacing/barge-in/forward/end) → each task's tests. ✔
- §8 deferred (AEC/AGC/NS avoided; pre-buffer noted as a seam in `pace.rs`; `rubato` swap point) → documented in code comments. ✔

**Placeholder scan:** No TBD/TODO/"add error handling"/uncoded steps. Every code step has complete code.

**Type consistency:** `g711::{decode,encode}(G711Kind, ...)`, `silence_byte(G711Kind)`, `resample::up_8k_16k(&[i16])->Vec<i16>`, `resample::Downsampler::{new,process}`, `pace::Downlink::{new,push,clear,next_frame}`, `bridge::{BridgePorts,BridgeEnd,run}` — names/signatures match across Task 4's use sites (`g711::decode`/`encode`, `resample::up_8k_16k`, `pace::Downlink`) and their definitions. `Event` variants used (`OutputAudio`, `Interrupted`, `TurnComplete`, `Transcript`) exist in `crate::gemini_live::Event` (verified: enum has `OutputAudio(Vec<i16>)`, `Transcript{..}`, `Interrupted`, `TurnComplete`, `EndCall`, `Warning`). `G711Kind` is `pub` in `crate::sip`.
