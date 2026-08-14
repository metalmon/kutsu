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
        d.clear(); // barge-in — drops buffered audio, leaves downsampler continuous
        // First post-clear frame carries filter ring-out (expected DSP behavior).
        let _ = d.next_frame();
        // Second post-clear frame should be silence as filter settles.
        let f = d.next_frame();
        assert!(f.iter().all(|&s| s.abs() < 16), "second post-clear frame should be silence");
    }

    #[test]
    fn each_frame_consumes_20ms() {
        let mut d = Downlink::new();
        d.push(&[1000i16; 480 * 3]); // exactly 3 frames' worth
        for _ in 0..3 {
            assert_eq!(d.next_frame().len(), 160);
        }
        // Buffer now drained. First post-drain frame carries filter ring-out (expected DSP behavior).
        let _ = d.next_frame();
        // Second post-drain frame should be silence as filter settles.
        let f = d.next_frame();
        assert!(f.iter().all(|&s| s.abs() < 16));
    }
}
