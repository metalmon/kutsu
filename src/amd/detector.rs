//! Verdict from a [`CalleeProfile`]. `HeuristicDetector` uses tunable thresholds
//! (the harness sweeps them); the trait leaves room for model / combined
//! detectors later.

use crate::amd::AmdClass;
use crate::amd::features::CalleeProfile;

/// A classification with a rough confidence (0..1).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AmdVerdict {
    pub class: AmdClass,
    pub confidence: f32,
}

/// Turns a callee profile into a verdict.
pub trait AmdDetector {
    fn classify(&self, p: &CalleeProfile) -> AmdVerdict;
}

/// Thresholds for [`HeuristicDetector`]. Tuned via the harness against a corpus.
#[derive(Clone, Copy, Debug)]
pub struct HeuristicParams {
    /// A first utterance / longest run at or above this reads as a machine
    /// greeting (a human's first "hello" is short).
    pub greeting_ms: u64,
    /// `nonspeech_energy_ratio` at or above this reads as hold music/ads.
    pub hold_nonspeech_ratio: f32,
}

impl Default for HeuristicParams {
    fn default() -> Self {
        // Starting points; the harness refines these against labeled data.
        Self {
            greeting_ms: 3000,
            hold_nonspeech_ratio: 0.4,
        }
    }
}

pub struct HeuristicDetector {
    params: HeuristicParams,
}

impl HeuristicDetector {
    pub fn new(params: HeuristicParams) -> Self {
        Self { params }
    }
}

impl AmdDetector for HeuristicDetector {
    fn classify(&self, p: &CalleeProfile) -> AmdVerdict {
        // Hold first: loud non-speech is unambiguous music/ads (Silero only).
        if let Some(r) = p.nonspeech_energy_ratio
            && r >= self.params.hold_nonspeech_ratio
        {
            return AmdVerdict {
                class: AmdClass::Hold,
                confidence: r.min(1.0),
            };
        }
        // A long continuous greeting with no turn-yielding reads as voicemail.
        if p.first_utterance_ms >= self.params.greeting_ms
            || p.longest_speech_run_ms >= self.params.greeting_ms
        {
            return AmdVerdict {
                class: AmdClass::Voicemail,
                confidence: 0.7,
            };
        }
        AmdVerdict {
            class: AmdClass::Human,
            confidence: 0.6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amd::AmdClass;
    use crate::amd::features::CalleeProfile;

    fn base() -> CalleeProfile {
        CalleeProfile {
            onset_ms: Some(300),
            first_utterance_ms: 500,
            speech_ratio: 0.3,
            longest_speech_run_ms: 500,
            longest_pause_ms: 1000,
            total_ms: 8000,
            mean_speech_prob: None,
            nonspeech_energy_ratio: None,
        }
    }

    #[test]
    fn short_utterance_then_pause_reads_human() {
        let d = HeuristicDetector::new(HeuristicParams::default());
        assert_eq!(d.classify(&base()).class, AmdClass::Human);
    }

    #[test]
    fn long_continuous_greeting_reads_voicemail() {
        let d = HeuristicDetector::new(HeuristicParams::default());
        let p = CalleeProfile {
            first_utterance_ms: 6000,
            longest_speech_run_ms: 6000,
            ..base()
        };
        assert_eq!(d.classify(&p).class, AmdClass::Voicemail);
    }

    #[test]
    fn loud_nonspeech_reads_hold() {
        let d = HeuristicDetector::new(HeuristicParams::default());
        let p = CalleeProfile {
            nonspeech_energy_ratio: Some(0.6),
            mean_speech_prob: Some(0.1),
            ..base()
        };
        assert_eq!(d.classify(&p).class, AmdClass::Hold);
    }
}
