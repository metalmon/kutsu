//! Per-frame speech classification backends. `EnergyFramer` wraps the existing
//! energy VAD; `SileroFramer` (feature `amd-silero`) is a neural backend.

use crate::amd::FrameClass;

/// Classifies fixed 20 ms PCM16 frames as speech / non-speech.
pub trait SpeechFramer {
    fn classify(&mut self, frame: &[i16]) -> FrameClass;
}

/// Energy-VAD backend: wraps [`crate::vad::Vad`]. No speech probability.
pub struct EnergyFramer {
    vad: crate::vad::Vad,
}

impl EnergyFramer {
    pub fn new(cfg: crate::config::VadConfig) -> Self {
        Self {
            vad: crate::vad::Vad::new(cfg),
        }
    }
}

impl SpeechFramer for EnergyFramer {
    fn classify(&mut self, frame: &[i16]) -> FrameClass {
        // `observe` updates the floor and returns onset-confirmed; the per-frame
        // speech state is `is_speech_frame()`.
        let _ = self.vad.observe(frame);
        FrameClass {
            speech: self.vad.is_speech_frame(),
            speech_prob: None,
            rms: self.vad.last_rms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VadConfig;

    #[test]
    fn energy_framer_flags_loud_frame_as_speech_after_quiet_seed() {
        let mut f = EnergyFramer::new(VadConfig::default());
        // Seed the noise floor with quiet frames, then a loud burst.
        let quiet = [30i16; 160];
        let loud = [4000i16; 160];
        for _ in 0..12 {
            let _ = f.classify(&quiet);
        }
        let fc = f.classify(&loud);
        assert!(fc.speech, "loud frame after a quiet seed must read as speech");
        assert!(fc.speech_prob.is_none(), "energy framer has no probability");
        assert!(fc.rms > 0.0);
    }
}
