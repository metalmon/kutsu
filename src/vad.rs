//! Pure adaptive-noise-floor energy VAD, ported from the voice-cloud prototype
//! (audio_io.py). Detects the onset of callee speech from incoming PCM16.
use crate::config::VadConfig;

pub struct Vad {
    cfg: VadConfig,
    noise_floor: f32,
    consec: u32,
    fired: bool,
}

impl Vad {
    pub fn new(cfg: VadConfig) -> Self {
        Self { cfg, noise_floor: 0.0, consec: 0, fired: false }
    }
    /// Feed one incoming callee frame. Returns true exactly once — on the frame
    /// that confirms speech onset. Once fired, always returns false.
    pub fn observe(&mut self, frame: &[i16]) -> bool {
        if self.fired || frame.is_empty() { return false; }
        let sumsq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        let rms = (sumsq / frame.len() as f64).sqrt() as f32;
        // Scale the ratio off whichever is larger — the absolute telephone
        // floor or the adapted background level — so a steady background
        // sitting above `min_rms` (e.g. 500 RMS line noise) still gets a
        // proportionally raised gate instead of leaking through the
        // un-scaled absolute floor while the EMA is catching up.
        let threshold = self.cfg.ratio * (self.cfg.min_rms as f32).max(self.noise_floor);
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
    fn cfg() -> VadConfig { VadConfig { min_rms: 200, ratio: 3.0, onset_frames: 3 } }

    #[test]
    fn silence_never_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..50 { assert!(!v.observe(&frame(0, 320))); }
    }
    #[test]
    fn sustained_speech_fires_once_after_onset_frames() {
        let mut v = Vad::new(cfg());
        // Two loud frames: not yet (onset_frames = 3).
        assert!(!v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320)));
        // Third confirms onset -> fires exactly once.
        assert!(v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320))); // already fired
    }
    #[test]
    fn single_click_does_not_fire() {
        let mut v = Vad::new(cfg());
        assert!(!v.observe(&frame(8000, 320))); // one loud frame
        for _ in 0..10 { assert!(!v.observe(&frame(0, 320))); } // back to silence, consec resets
    }
    #[test]
    fn adapts_to_steady_background_no_false_fire() {
        let mut v = Vad::new(cfg());
        // A steady moderate background (well above min_rms but constant) must
        // be tracked by the noise floor and NOT read as speech.
        for _ in 0..200 { assert!(!v.observe(&frame(500, 320))); }
    }
    #[test]
    fn speech_above_raised_floor_still_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..100 { v.observe(&frame(500, 320)); }   // floor adapts up toward ~500
        // Real speech well above floor*ratio still fires.
        let mut fired = false;
        for _ in 0..5 { if v.observe(&frame(4000, 320)) { fired = true; } }
        assert!(fired);
    }
}
