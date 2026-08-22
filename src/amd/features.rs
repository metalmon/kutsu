//! Callee-audio profile extracted from a stream of classified frames. Pure: no
//! I/O, no timing — `frame_ms` is the per-frame duration used to convert frame
//! counts to milliseconds.

use crate::amd::FrameClass;

/// ~500 ms of trailing silence ends the "first utterance".
const UTTERANCE_GAP_MS: u64 = 500;

/// Summary of the callee's audio over a call, the input to an
/// [`crate::amd::detector::AmdDetector`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalleeProfile {
    /// Time to the first speech frame; `None` if the callee never spoke.
    pub onset_ms: Option<u64>,
    /// Length of the first continuous speech run (ended by ~500 ms silence).
    pub first_utterance_ms: u64,
    /// Speech frames / total frames.
    pub speech_ratio: f32,
    /// Longest continuous speech run (a long run suggests a voicemail greeting).
    pub longest_speech_run_ms: u64,
    /// Longest continuous silence gap (a human yields the turn; a machine may not).
    pub longest_pause_ms: u64,
    /// Total analyzed duration.
    pub total_ms: u64,
    /// Mean neural speech probability, when the framer provides one.
    pub mean_speech_prob: Option<f32>,
    /// Fraction of frames that are loud but non-speech (music/ads/hold). Only
    /// meaningful with a probability-based framer; `None` otherwise.
    pub nonspeech_energy_ratio: Option<f32>,
}

pub fn extract_profile(
    frames: &[FrameClass],
    frame_ms: u64,
    nonspeech_rms_floor: f32,
) -> CalleeProfile {
    let total = frames.len() as u64;
    let mut speech_frames = 0u64;
    let mut onset_frame: Option<u64> = None;
    let mut first_utt_frames = 0u64;
    let mut first_utt_done = false;
    let mut cur_speech_run = 0u64;
    let mut longest_speech_run = 0u64;
    let mut cur_pause = 0u64;
    let mut longest_pause = 0u64;
    let mut trailing_sil_after_first = 0u64;

    let mut has_prob = false;
    let mut prob_sum = 0f32;
    let mut nonspeech_loud = 0u64;

    for (i, f) in frames.iter().enumerate() {
        if let Some(p) = f.speech_prob {
            has_prob = true;
            prob_sum += p;
            if !f.speech && f.rms >= nonspeech_rms_floor {
                nonspeech_loud += 1;
            }
        }
        if f.speech {
            speech_frames += 1;
            if onset_frame.is_none() {
                onset_frame = Some(i as u64);
            }
            cur_speech_run += 1;
            longest_speech_run = longest_speech_run.max(cur_speech_run);
            cur_pause = 0;
            // First-utterance accounting: grow while we are still in it.
            if onset_frame.is_some() && !first_utt_done {
                first_utt_frames += 1;
                trailing_sil_after_first = 0;
            }
        } else {
            cur_speech_run = 0;
            cur_pause += 1;
            longest_pause = longest_pause.max(cur_pause);
            if onset_frame.is_some() && !first_utt_done {
                trailing_sil_after_first += 1;
                if trailing_sil_after_first * frame_ms >= UTTERANCE_GAP_MS {
                    first_utt_done = true;
                }
            }
        }
    }

    CalleeProfile {
        onset_ms: onset_frame.map(|f| f * frame_ms),
        first_utterance_ms: first_utt_frames * frame_ms,
        speech_ratio: if total == 0 {
            0.0
        } else {
            speech_frames as f32 / total as f32
        },
        longest_speech_run_ms: longest_speech_run * frame_ms,
        longest_pause_ms: longest_pause * frame_ms,
        total_ms: total * frame_ms,
        mean_speech_prob: if has_prob && total > 0 {
            Some(prob_sum / total as f32)
        } else {
            None
        },
        nonspeech_energy_ratio: if has_prob && total > 0 {
            Some(nonspeech_loud as f32 / total as f32)
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amd::FrameClass;

    fn speech(rms: f32) -> FrameClass {
        FrameClass {
            speech: true,
            speech_prob: None,
            rms,
        }
    }
    fn sil(rms: f32) -> FrameClass {
        FrameClass {
            speech: false,
            speech_prob: None,
            rms,
        }
    }

    #[test]
    fn profile_measures_onset_first_utterance_and_runs() {
        // 2 silence, 5 speech, 30 silence (>500 ms ends the utterance), 3 speech.
        let mut frames = vec![sil(10.0); 2];
        frames.extend(vec![speech(2000.0); 5]);
        frames.extend(vec![sil(10.0); 30]);
        frames.extend(vec![speech(2000.0); 3]);
        let p = extract_profile(&frames, 20, 500.0);
        assert_eq!(p.onset_ms, Some(40)); // 2 silence frames * 20 ms
        assert_eq!(p.first_utterance_ms, 100); // 5 speech frames * 20 ms
        assert_eq!(p.longest_speech_run_ms, 100);
        assert_eq!(p.longest_pause_ms, 600); // 30 * 20 ms
        assert_eq!(p.total_ms, 40 * 20);
        assert!((p.speech_ratio - 8.0 / 40.0).abs() < 1e-6);
        assert!(p.mean_speech_prob.is_none());
        assert!(p.nonspeech_energy_ratio.is_none());
    }

    #[test]
    fn nonspeech_energy_ratio_present_only_with_probabilities() {
        // Silero-style frames: loud but non-speech = music/hold.
        let frames = vec![
            FrameClass {
                speech: false,
                speech_prob: Some(0.1),
                rms: 3000.0,
            },
            FrameClass {
                speech: false,
                speech_prob: Some(0.1),
                rms: 3000.0,
            },
            FrameClass {
                speech: true,
                speech_prob: Some(0.9),
                rms: 3000.0,
            },
            FrameClass {
                speech: false,
                speech_prob: Some(0.1),
                rms: 10.0,
            },
        ];
        let p = extract_profile(&frames, 20, 500.0);
        // 2 of 4 frames are loud (>=500) and non-speech.
        assert_eq!(p.nonspeech_energy_ratio, Some(0.5));
        assert!(p.mean_speech_prob.is_some());
    }

    #[test]
    fn empty_input_is_all_zero() {
        let p = extract_profile(&[], 20, 500.0);
        assert_eq!(p.onset_ms, None);
        assert_eq!(p.total_ms, 0);
        assert_eq!(p.first_utterance_ms, 0);
    }
}
