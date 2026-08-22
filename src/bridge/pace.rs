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

    /// Barge-in with a short fade instead of a hard cut: ramp the next `fade_ms`
    /// of buffered audio linearly down to silence and drop everything after it,
    /// so the phone hears a smooth stop rather than a click on an abrupt
    /// `clear()`. The downsampler keeps its FIR state so the faded tail plays
    /// through it continuously (then rings out into silence). Re-arms a full
    /// prefill for the next turn. Falls back to `clear()` when nothing is
    /// buffered.
    pub fn interrupt_fade(&mut self, fade_ms: u32) {
        let n = ((fade_ms as usize) * SAMPLES_PER_MS).min(self.buf.len());
        if n == 0 {
            self.clear();
            return;
        }
        let mut faded: VecDeque<i16> = VecDeque::with_capacity(n);
        for (i, &s) in self.buf.iter().take(n).enumerate() {
            let gain = 1.0 - (i as f32 / n as f32); // 1.0 -> ~0.0 across the head
            faded.push_back((s as f32 * gain) as i16);
        }
        self.buf = faded;
        self.playing = true; // play the faded tail out immediately, no re-prefill
        self.fill_target = self.prebuffer;
    }

    /// Arm/disarm underrun accounting for an active model turn. A rising edge
    /// (false -> true) re-arms a fresh prefill so each turn's audio is buffered
    /// before playout (absorbs Gemini-path jitter within the turn). Called from
    /// `bridge::run` as the engine tracks whether a model turn is in flight.
    pub fn set_expecting(&mut self, expecting: bool) {
        if expecting && !self.expecting {
            self.playing = false;
            self.fill_target = self.prebuffer;
        }
        self.expecting = expecting;
    }

    /// Count of mid-turn underruns (buffer ran dry after playout had started).
    /// This is the metric used for the abort gate — see `starved_ms` for why
    /// it, not this count, includes the per-turn prefill hold.
    pub fn underruns(&self) -> u64 {
        self.underruns
    }

    /// Total silence (ms) inserted while a turn was expected — this includes
    /// the intentional per-turn prefill hold (~`prebuffer_ms` at the start of
    /// each turn, and ~`resume_ms` after a mid-turn underrun), not only true
    /// dropout silence. It's therefore an upper bound on lost audio, not a
    /// precise dropout measure; for gating/alerting on real dropouts use
    /// `underruns` (the count), not this.
    pub fn starved_ms(&self) -> u64 {
        self.starved_ms
    }

    /// True while buffered downlink audio remains to be played out. The engine
    /// polls this to let the model's closing audio drain to the phone before
    /// hanging up, so the goodbye isn't truncated by BYE + bridge teardown.
    pub fn pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// True once the prefill target has been met and real (buffered) audio is
    /// being emitted, false while holding for prefill/resume or after a
    /// mid-turn underrun. The bridge watches the false->true edge to time the
    /// per-turn prebuffer latency (audio received -> audio reaching the phone).
    pub fn playing(&self) -> bool {
        self.playing
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
    fn interrupt_fade_drops_buffered_bulk() {
        let mut d = Downlink::new(0, 0);
        d.push(&[8000i16; 480 * 10]); // 200 ms of loud audio
        let _ = d.next_frame(); // start playing
        d.interrupt_fade(25); // keep ~25 ms faded head, drop the rest
        // Only ~1-2 frames of (fading) signal remain, not ~10.
        let mut signal_frames = 0;
        for _ in 0..10 {
            if d.next_frame().iter().any(|&s| s.abs() > 500) {
                signal_frames += 1;
            }
        }
        assert!(
            signal_frames <= 2,
            "fade must drop the buffered bulk, got {signal_frames}"
        );
    }

    #[test]
    fn interrupt_fade_on_empty_buffer_is_a_clear() {
        let mut d = Downlink::new(140, 60);
        d.interrupt_fade(25); // nothing buffered -> behaves like clear()/re-arm
        assert!(!d.pending());
    }

    #[test]
    fn pending_reflects_buffered_audio() {
        let mut d = Downlink::new(0, 0);
        assert!(!d.pending(), "empty buffer is not pending");
        d.push(&[8000i16; 480]); // one 20 ms block
        assert!(d.pending(), "buffered audio is pending");
        let _ = d.next_frame(); // plays out the whole block
        assert!(!d.pending(), "drained buffer is not pending");
    }

    #[test]
    fn underrun_yields_silence_frame() {
        let mut d = Downlink::new(0, 0);
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        assert!(
            f.iter().all(|&s| s.abs() < 16),
            "empty buffer should be ~silence"
        );
    }

    #[test]
    fn buffered_audio_plays_out() {
        let mut d = Downlink::new(0, 0);
        d.push(&[8000i16; 480 * 2]); // 40 ms of loud DC @24k
        let f = d.next_frame();
        assert_eq!(f.len(), 160);
        // After warm-up the frame carries the signal, not silence.
        assert!(
            f[80..].iter().any(|&s| s.abs() > 2000),
            "buffered audio not played"
        );
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
        assert!(
            f.iter().all(|&s| s.abs() < 16),
            "second post-clear frame should be silence"
        );
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
        assert!(
            f.iter().all(|&s| s.abs() < 16),
            "should hold (silence) under prefill target"
        );
        // Top up past 140 ms and it starts playing.
        d.push(&[8000i16; 480 * 5]); // now 160 ms buffered
        let f = d.next_frame();
        assert!(
            f[80..].iter().any(|&s| s.abs() > 2000),
            "should play once prefill met"
        );
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
        assert!(
            f.iter().all(|&s| s.abs() < 16),
            "clear must re-arm the 140 ms prefill"
        );
    }
}
