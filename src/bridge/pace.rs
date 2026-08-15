//! Downlink (Gemini -> phone) jitter buffer + 20 ms pacer. Holds 24 kHz PCM,
//! feeds the stateful downsampler one 480-sample (20 ms) block per frame, and
//! emits 160-sample 8 kHz frames. On underrun the block is zero-padded (silence)
//! — the downsampler is always fed 480 samples so its filter state stays
//! continuous. `clear()` is barge-in: drop everything buffered and re-arm a
//! fresh prefill for the next turn.
//!
//! Adaptive prefill state machine: holds playout (silence) until `prebuffer_ms`
//! of audio has accumulated, absorbing jitter on the Gemini path before a turn
//! starts speaking. If the buffer runs dry mid-turn, playout pauses again and
//! waits for a smaller `resume_ms` refill before resuming, so a brief stall
//! doesn't force a full re-prefill. `underruns()`/`starved_ms()` are metrics for
//! how often/long the pacer had to hold or insert silence while a turn was
//! expected (`set_expecting(true)`).

use std::collections::VecDeque;

use super::resample::Downsampler;

const IN_PER_FRAME: usize = 480; // 20 ms @ 24 kHz
const SAMPLES_PER_MS: usize = 24; // 24 kHz

pub struct Downlink {
    buf: VecDeque<i16>,
    down: Downsampler,
    prebuffer: usize, // samples to buffer before (re)starting playout
    resume: usize,    // samples to buffer before resuming after a mid-turn underrun
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
            buf: VecDeque::new(),
            down: Downsampler::new(),
            prebuffer,
            resume,
            fill_target: prebuffer,
            playing: false,
            expecting: false,
            underruns: 0,
            starved_ms: 0,
        }
    }

    /// Append Gemini output (PCM16 @ 24 kHz).
    pub fn push(&mut self, samples: &[i16]) {
        self.buf.extend(samples.iter().copied());
    }

    /// Barge-in: drop buffered audio, reset the downsampler's FIR state (so the
    /// phone hears silence immediately instead of a ring-out tail from whatever
    /// was playing), and re-arm a full prefill for the next turn.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.down = Downsampler::new();
        self.playing = false;
        self.fill_target = self.prebuffer;
    }

    /// Arm/disarm underrun accounting for an active model turn. A rising edge
    /// (false -> true) re-arms a fresh prefill so each turn's audio is buffered
    /// before playout (absorbs Gemini-path jitter within the turn).
    // Not yet called from `bridge::run` — wired in a later task alongside the
    // prebuffer/resume config; exercised directly by this module's unit tests.
    #[allow(dead_code)]
    pub fn set_expecting(&mut self, expecting: bool) {
        if expecting && !self.expecting {
            self.playing = false;
            self.fill_target = self.prebuffer;
        }
        self.expecting = expecting;
    }

    #[allow(dead_code)]
    pub fn underruns(&self) -> u64 {
        self.underruns
    }

    #[allow(dead_code)]
    pub fn starved_ms(&self) -> u64 {
        self.starved_ms
    }

    /// Produce one 20 ms frame (160 samples @ 8 kHz).
    pub fn next_frame(&mut self) -> Vec<i16> {
        if !self.playing {
            if self.buf.len() >= self.fill_target.max(IN_PER_FRAME) {
                self.playing = true;
            } else {
                if self.expecting {
                    self.starved_ms += 20;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underrun_yields_silence_frame() {
        let mut d = Downlink::new(0, 0);
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        assert!(f.iter().all(|&s| s.abs() < 16), "empty buffer should be ~silence");
    }

    #[test]
    fn buffered_audio_plays_out() {
        let mut d = Downlink::new(0, 0);
        d.push(&[8000i16; 480 * 2]); // 40 ms of loud DC @24k
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        // After warm-up the frame carries the signal, not silence.
        assert!(f[80..].iter().any(|&s| s.abs() > 2000), "buffered audio not played");
    }

    #[test]
    fn clear_flushes_pending_audio() {
        let mut d = Downlink::new(0, 0);
        d.push(&[8000i16; 480 * 4]);
        let _ = d.next_frame(); // consume some
        d.clear(); // barge-in — drops buffered audio and resets the downsampler's FIR state
        // Both post-clear frames are silence: clear() resets the filter, so there's
        // no ring-out tail from the audio that was playing before the barge-in.
        let _ = d.next_frame();
        let f = d.next_frame();
        assert!(f.iter().all(|&s| s.abs() < 16), "second post-clear frame should be silence");
    }

    #[test]
    fn each_frame_consumes_20ms() {
        let mut d = Downlink::new(0, 0);
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
        let _ = d.next_frame(); // plays it, buffer now empty
        let _ = d.next_frame(); // underrun (empty while expecting)
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
}
