//! Pure adaptive-noise-floor energy VAD, ported from the voice-cloud prototype
//! (audio_io.py). Detects the onset of callee speech from incoming PCM16.
//!
//! Cold-start calibration matters: the discriminator between a steady line
//! background and genuine speech is a JUMP from an established baseline, not
//! absolute loudness. The very first frame ever observed has no prior
//! contrast to be judged against, so it unconditionally seeds `noise_floor`.
//! For the rest of the `warmup_frames` window, a frame that jumps above the
//! floor already established by that seed is treated as speech immediately —
//! so a "Allo" arriving right after pickup isn't silently averaged into the
//! background — while frames that stay near the seed keep training it.
//!
//! Residual ambiguity (not fixable, documented deliberately): speech that is
//! present and perfectly sustained from literally the first observed frame,
//! with no lead-in at all — not even one quieter frame — is indistinguishable
//! from a steady background at that same level, because the seed frame has
//! no contrast to be judged against. This is an inherent limit of
//! energy-only VAD, not a bug in this module. The integration layer (see
//! Task 3) also treats a user-role `Transcript` (ASR-driven) as a
//! `callee_active` signal, so this exact edge case is still caught — just
//! slightly later, once Gemini transcribes the first words.
use crate::config::VadConfig;

pub struct Vad {
    cfg: VadConfig,
    noise_floor: f32,
    last_rms: f32,
    consec: u32,
    fired: bool,
    frames_seen: u32,
}

impl Vad {
    pub fn new(cfg: VadConfig) -> Self {
        Self {
            cfg,
            noise_floor: 0.0,
            last_rms: 0.0,
            consec: 0,
            fired: false,
            frames_seen: 0,
        }
    }

    /// RMS of the most recently observed frame (for onset logging/tuning).
    pub fn last_rms(&self) -> f32 {
        self.last_rms
    }
    /// Current adaptive noise floor (for onset logging/tuning).
    pub fn noise_floor(&self) -> f32 {
        self.noise_floor
    }

    /// Whether the most recently observed frame is above the speech threshold
    /// (same rule as [`Vad::classify`], but a stateless read of the last frame).
    /// Used to profile utterance duration for answering-machine detection — a
    /// short first utterance ("allo") vs a long continuous greeting (voicemail).
    pub fn is_speech_frame(&self) -> bool {
        let threshold = (self.cfg.min_rms as f32).max(self.noise_floor * self.cfg.ratio);
        self.last_rms >= threshold
    }

    /// Feed one incoming callee frame. Returns true exactly once — on the frame
    /// that confirms speech onset. Once fired, always returns false.
    pub fn observe(&mut self, frame: &[i16]) -> bool {
        if frame.is_empty() {
            return false;
        }
        let sumsq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        let rms = (sumsq / frame.len() as f64).sqrt() as f32;
        self.last_rms = rms;
        // Onset fires only once, but keep `last_rms` fresh on every later frame
        // so `is_speech_frame()` (utterance profiling / AMD) tracks live energy
        // instead of freezing at the loud onset value.
        if self.fired {
            return false;
        }

        if self.frames_seen < self.cfg.warmup_frames {
            let is_seed_frame = self.frames_seen == 0;
            self.frames_seen += 1;
            if is_seed_frame {
                // No prior contrast exists yet: absorb unconditionally to
                // seed the floor (see module doc for the residual ambiguity
                // this implies).
                self.noise_floor = rms;
                self.consec = 0;
                return false;
            }
            // Jump-bail: a level well above the already-established floor is
            // speech even during warmup; only frames near the floor keep
            // training it.
            return self.classify(rms);
        }
        self.classify(rms)
    }

    /// Shared speech/background classification against the current floor.
    /// Used for both post-warmup detection and the warmup jump-bail.
    fn classify(&mut self, rms: f32) -> bool {
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
    fn frame(amp: i16, n: usize) -> Vec<i16> {
        vec![amp; n]
    }
    fn cfg() -> VadConfig {
        VadConfig {
            min_rms: 200,
            ratio: 3.0,
            onset_frames: 3,
            warmup_frames: 10,
        }
    }

    #[test]
    fn silence_never_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..50 {
            assert!(!v.observe(&frame(0, 320)));
        }
    }

    #[test]
    fn speech_after_warmup_fires_once() {
        let mut v = Vad::new(cfg());
        // Warm up on a low, steady background.
        for _ in 0..10 {
            assert!(!v.observe(&frame(100, 320)));
        }
        // Loud burst: fires exactly on the onset_frames-th frame, never again.
        assert!(!v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320)));
        assert!(v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320))); // already fired
    }

    #[test]
    fn single_click_does_not_fire() {
        let mut v = Vad::new(cfg());
        for _ in 0..10 {
            assert!(!v.observe(&frame(100, 320)));
        } // warmup
        assert!(!v.observe(&frame(4000, 320))); // one loud frame
        for _ in 0..10 {
            assert!(!v.observe(&frame(0, 320)));
        } // back to silence, consec resets
    }

    #[test]
    fn steady_loud_background_no_false_fire() {
        let mut v = Vad::new(cfg());
        // A steady, elevated background (well above min_rms but constant)
        // throughout, including warmup: the floor adapts up toward ~1500
        // (threshold ~4500), so the same-level input never reads as speech.
        for _ in 0..200 {
            assert!(!v.observe(&frame(1500, 320)));
        }
    }

    #[test]
    fn speech_jump_during_warmup_fires() {
        let mut v = Vad::new(cfg());
        // A few quiet baseline frames right after pickup establish the floor
        // (the first of these seeds it, since there's no prior contrast yet).
        for _ in 0..4 {
            assert!(!v.observe(&frame(80, 320)));
        }
        // The callee's "Allo" is a clear jump above that established floor —
        // still inside the warmup window (frames_seen well under
        // warmup_frames=10), but the jump-bail catches it immediately
        // instead of averaging it into the background.
        assert!(!v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320)));
        assert!(v.observe(&frame(4000, 320))); // 3rd burst frame confirms onset
        assert!(!v.observe(&frame(4000, 320))); // already fired
    }

    #[test]
    fn quiet_speech_above_low_background_fires() {
        let mut v = Vad::new(cfg());
        // Warm up on a quiet background well below min_rms.
        for _ in 0..10 {
            assert!(!v.observe(&frame(80, 320)));
        }
        // Quiet "Allo"-level speech (well below the old ratio*min_rms=600
        // trap, but real speech): must still be detected.
        let mut fired = false;
        for _ in 0..5 {
            if v.observe(&frame(400, 320)) {
                fired = true;
            }
        }
        assert!(fired);
    }
}
