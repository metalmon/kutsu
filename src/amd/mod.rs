//! Answering-machine / non-live detection: tell a live human from a machine
//! (voicemail, IVR, or PBX hold/transfer with background audio). The framer +
//! feature extractor + detector here are shared by the offline harness
//! (`amd-eval` feature) and the future runtime hook; the corpus/eval pieces are
//! gated behind `amd-eval`. See docs/superpowers/specs/2026-08-22-amd-harness-design.md.

pub mod detector;
pub mod features;
pub mod framer;

#[cfg(feature = "amd-eval")]
pub mod corpus;
#[cfg(feature = "amd-eval")]
pub mod eval;
#[cfg(feature = "amd-silero")]
pub mod silero;

/// What the far end is, from the callee-audio profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AmdClass {
    Human,
    Voicemail,
    Ivr,
    Hold,
}

impl AmdClass {
    /// All variants, in a stable order — the axes of the confusion matrix.
    pub const ALL: [AmdClass; 4] = [
        AmdClass::Human,
        AmdClass::Voicemail,
        AmdClass::Ivr,
        AmdClass::Hold,
    ];

    /// The actionable axis: anything that is not a live human is a machine.
    pub fn is_machine(&self) -> bool {
        !matches!(self, AmdClass::Human)
    }

    /// Position in [`AmdClass::ALL`] — the confusion-matrix index.
    pub fn index(self) -> usize {
        match self {
            AmdClass::Human => 0,
            AmdClass::Voicemail => 1,
            AmdClass::Ivr => 2,
            AmdClass::Hold => 3,
        }
    }

    /// Short lowercase label for reports.
    pub fn label(self) -> &'static str {
        match self {
            AmdClass::Human => "human",
            AmdClass::Voicemail => "voicemail",
            AmdClass::Ivr => "ivr",
            AmdClass::Hold => "hold",
        }
    }
}

/// One 20 ms frame classified by a [`framer::SpeechFramer`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrameClass {
    /// Whether this frame is speech.
    pub speech: bool,
    /// Neural speech probability (0..1) when the backend provides one; `None`
    /// for the energy backend.
    pub speech_prob: Option<f32>,
    /// Frame RMS amplitude (linear PCM16).
    pub rms: f32,
}
