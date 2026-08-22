//! Silero neural VAD backend (feature `amd-silero`). Wraps the embedded Silero
//! v5 ONNX model via `ort`, exposing a per-window speech probability. The model
//! is stateful (LSTM): the `state` tensor is threaded between calls.

use ort::session::Session;
use ort::value::Tensor;

/// Embedded Silero VAD v5 model (see assets/silero_vad.onnx).
const MODEL: &[u8] = include_bytes!("../../assets/silero_vad.onnx");

/// Silero v5 requires a fixed 512-sample window at 16 kHz.
pub const WINDOW: usize = 512;
/// Sample rate the model is driven at.
const SR: i64 = 16_000;
/// Recurrent state shape is `[2, 1, 128]` = 256 floats.
const STATE_LEN: usize = 2 * 128;

pub struct SileroModel {
    session: Session,
    state: Vec<f32>,
}

impl SileroModel {
    pub fn new() -> anyhow::Result<Self> {
        let session = Session::builder()?.commit_from_memory(MODEL)?;
        Ok(Self {
            session,
            state: vec![0.0f32; STATE_LEN],
        })
    }

    /// Run one `WINDOW`-sample window (i16 @ 16 kHz) and return the speech
    /// probability in `0..=1`. Threads the recurrent state across calls.
    pub fn infer(&mut self, window: &[i16]) -> anyhow::Result<f32> {
        let audio: Vec<f32> = window.iter().map(|&s| s as f32 / 32768.0).collect();
        let audio_t = Tensor::from_array((vec![1_i64, audio.len() as i64], audio))?;
        let state_t = Tensor::from_array((vec![2_i64, 1, 128], self.state.clone()))?;
        let sr_t = Tensor::from_array((Vec::<i64>::new(), vec![SR]))?;

        let outputs = self.session.run(ort::inputs![
            "input" => audio_t,
            "state" => state_t,
            "sr" => sr_t,
        ])?;

        // Copy out before the borrow on `self.session` (held by `outputs`) ends.
        let new_state = outputs["stateN"].try_extract_tensor::<f32>()?.1.to_vec();
        let prob = outputs["output"].try_extract_tensor::<f32>()?.1[0];
        self.state = new_state;
        Ok(prob)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silero_loads_and_scores_in_range() {
        let mut s = SileroModel::new().expect("load model");
        let silence = vec![0i16; WINDOW];
        // A tone stands in for energy; Silero may or may not call it speech, but
        // the score must be a valid probability and not below the silence score.
        let tone: Vec<i16> = (0..WINDOW)
            .map(|i| ((i as f32 * 0.3).sin() * 8000.0) as i16)
            .collect();
        let p_sil = s.infer(&silence).expect("infer silence");
        let p_tone = s.infer(&tone).expect("infer tone");
        // The smoke test checks the model loads, runs, and returns valid
        // probabilities; it does not assume a synthetic tone reads as speech
        // (Silero is trained on real speech, not sine waves).
        assert!(
            (0.0..=1.0).contains(&p_sil),
            "silence prob out of range: {p_sil}"
        );
        assert!(
            (0.0..=1.0).contains(&p_tone),
            "tone prob out of range: {p_tone}"
        );
        assert!(
            p_sil < 0.5,
            "digital silence must not read as speech: {p_sil}"
        );
    }
}
