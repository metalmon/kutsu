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

/// Neural VAD backend (feature `amd-silero`): accumulates incoming frames into
/// Silero's fixed 512-sample windows, running inference per full window and
/// holding the last probability between windows. Expects 16 kHz input.
#[cfg(feature = "amd-silero")]
pub struct SileroFramer {
    model: crate::amd::silero::SileroModel,
    window: usize,
    buf: Vec<i16>,
    last_prob: f32,
    threshold: f32,
}

#[cfg(feature = "amd-silero")]
impl SileroFramer {
    /// `sample_rate` must match the audio fed in (telephony is 8 kHz — use the
    /// native 8 kHz dump, not one upsampled to 16 kHz).
    pub fn new(sample_rate: u32) -> anyhow::Result<Self> {
        let window = crate::amd::silero::window_for(sample_rate);
        Ok(Self {
            model: crate::amd::silero::SileroModel::new(sample_rate)?,
            window,
            buf: Vec::with_capacity(window),
            last_prob: 0.0,
            threshold: 0.5,
        })
    }
}

#[cfg(feature = "amd-silero")]
impl SpeechFramer for SileroFramer {
    fn classify(&mut self, frame: &[i16]) -> FrameClass {
        self.buf.extend_from_slice(frame);
        while self.buf.len() >= self.window {
            let window: Vec<i16> = self.buf.drain(..self.window).collect();
            // Fail open: a model error holds the previous probability rather than
            // aborting the analysis of a whole recording/call.
            if let Ok(p) = self.model.infer(&window) {
                self.last_prob = p;
            }
        }
        let rms = {
            let sumsq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
            (sumsq / frame.len().max(1) as f64).sqrt() as f32
        };
        FrameClass {
            speech: self.last_prob > self.threshold,
            speech_prob: Some(self.last_prob),
            rms,
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
        assert!(
            fc.speech,
            "loud frame after a quiet seed must read as speech"
        );
        assert!(fc.speech_prob.is_none(), "energy framer has no probability");
        assert!(fc.rms > 0.0);
    }
}
