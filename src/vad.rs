//! Pure adaptive-noise-floor energy VAD, ported from the voice-cloud prototype
//! (audio_io.py). Detects the onset of callee speech from incoming PCM16.
//!
//! Cold-start calibration matters: the discriminator between a steady line
//! background and genuine speech is temporal, not amplitude alone. The first
//! `warmup_frames` frames unconditionally train `noise_floor` to the line's
//! background level (no detection during that window); only after warmup does
//! the ratio-gated threshold from the voice-cloud prototype apply.
use crate::config::VadConfig;

pub struct Vad {
    cfg: VadConfig,
    noise_floor: f32,
    consec: u32,
    fired: bool,
    frames_seen: u32,
}

impl Vad {
    pub fn new(cfg: VadConfig) -> Self {
        Self { cfg, noise_floor: 0.0, consec: 0, fired: false, frames_seen: 0 }
    }

    /// Feed one incoming callee frame. Returns true exactly once — on the frame
    /// that confirms speech onset. Once fired, always returns false.
    pub fn observe(&mut self, frame: &[i16]) -> bool {
        if self.fired || frame.is_empty() { return false; }
        let sumsq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        let rms = (sumsq / frame.len() as f64).sqrt() as f32;

        if self.frames_seen < self.cfg.warmup_frames {
            // Calibration window: track the background unconditionally
            // (regardless of level) and never detect speech yet.
            self.frames_seen += 1;
            self.noise_floor = self.noise_floor * 0.9 + rms * 0.1;
            self.consec = 0;
            return false;
        }

        let threshold = (self.cfg.min_rms as f32).max(self.noise_floor * self.cfg.ratio);
        if rms >= threshold {
            self.consec += 1;
            if self.consec >= self.cfg.onset_frames {
                self.fired = true;
                return true;
            }
        } else {
            self.consec = 0;
            // Track background only on non-speech frames (EMA, a = 0.1).
            self.noise_floor = self.noise_floor * 0.9 + rms * 0.1;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(amp: i16, n: usize) -> Vec<i16> { vec![amp; n] }
    fn cfg() -> VadConfig {
        VadConfig { min_rms: 200, ratio: 3.0, onset_frames: 3, warmup_frames: 10 }
    }

    #[test]
    fn silence_never_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..50 { assert!(!v.observe(&frame(0, 320))); }
    }

    #[test]
    fn speech_after_warmup_fires_once() {
        let mut v = Vad::new(cfg());
        // Warm up on a low, steady background.
        for _ in 0..10 { assert!(!v.observe(&frame(100, 320))); }
        // Loud burst: fires exactly on the onset_frames-th frame, never again.
        assert!(!v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320)));
        assert!(v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320))); // already fired
    }

    #[test]
    fn single_click_does_not_fire() {
        let mut v = Vad::new(cfg());
        for _ in 0..10 { assert!(!v.observe(&frame(100, 320))); } // warmup
        assert!(!v.observe(&frame(4000, 320))); // one loud frame
        for _ in 0..10 { assert!(!v.observe(&frame(0, 320))); } // back to silence, consec resets
    }

    #[test]
    fn steady_loud_background_no_false_fire() {
        let mut v = Vad::new(cfg());
        // A steady, elevated background (well above min_rms but constant)
        // throughout, including warmup: the floor adapts up toward ~1500
        // (threshold ~4500), so the same-level input never reads as speech.
        for _ in 0..200 { assert!(!v.observe(&frame(1500, 320))); }
    }

    #[test]
    fn quiet_speech_above_low_background_fires() {
        let mut v = Vad::new(cfg());
        // Warm up on a quiet background well below min_rms.
        for _ in 0..10 { assert!(!v.observe(&frame(80, 320))); }
        // Quiet "Алло"-level speech (well below the old ratio*min_rms=600
        // trap, but real speech): must still be detected.
        let mut fired = false;
        for _ in 0..5 { if v.observe(&frame(400, 320)) { fired = true; } }
        assert!(fired);
    }
}
