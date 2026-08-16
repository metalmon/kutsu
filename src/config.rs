//! Server- and scenario-level configuration for the Gemini Live client.
//!
//! `ServerConfig` holds server-start defaults (credentials, model, voice, proxy,
//! network-check thresholds, concurrency cap). `ScenarioConfig` mirrors the
//! future `place_call` per-task arguments.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::PathBuf;

/// Signaling/media transport for the SIP trunk. Only `Udp` is implemented this
/// iteration; `Tls` is a documented extension seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SipTransportKind {
    #[default]
    Udp,
    Tls,
}

/// Outbound SIP trunk configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SipConfig {
    /// SIP server / trunk as `host:port`.
    pub server: String,
    /// Digest username (also the default caller identity).
    pub username: String,
    /// Digest password.
    pub password: String,
    /// From-header user-part. Defaults to `username` when absent.
    #[serde(default)]
    pub from_user: Option<String>,
    /// Local IP to bind + advertise in SDP. Auto-detected (route toward
    /// `server`) when absent.
    #[serde(default)]
    pub local_ip: Option<IpAddr>,

    // --- extension seams; parsed but not yet wired ---
    /// Send a REGISTER binding before calling. Not yet implemented.
    #[serde(default)]
    pub register: bool,
    /// Transport kind. Only `Udp` implemented. Not yet wired.
    #[serde(default)]
    pub transport: SipTransportKind,
}

impl SipConfig {
    /// Caller identity user-part: explicit `from_user`, else the digest username.
    pub fn from_user(&self) -> &str {
        self.from_user.as_deref().unwrap_or(&self.username)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Model {
    HalfCascade,
    NativeAudio,
}

impl Default for Model {
    fn default() -> Self {
        Model::HalfCascade
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Proxy {
    pub url: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NetCheckConfig {
    pub enabled: bool,
    pub samples: u32,
    pub max_rtt_ms: u32,
    pub max_jitter_ms: u32,
    pub max_loss_pct: f32,
}

impl Default for NetCheckConfig {
    fn default() -> Self {
        // Thresholds tuned for realtime voice (see spec).
        NetCheckConfig { enabled: true, samples: 10, max_rtt_ms: 300, max_jitter_ms: 50, max_loss_pct: 2.0 }
    }
}

/// Grammatical gender the agent uses when referring to itself, matched to the
/// configured [`ServerConfig::voice`]. Kept as an explicit setting rather than
/// derived from the provider-specific voice name, so the prompt layer stays
/// provider-independent (the operator sets it to match whatever voice they pick).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Gender {
    Male,
    #[default]
    Female,
    /// No grammatical-gender instruction (e.g. a genderless language or an
    /// androgynous voice).
    Neutral,
}

impl Gender {
    /// Parse a config/env string (case-insensitive). Unknown/empty falls back to
    /// `Female`, which matches the default voice (`Autonoe`).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "male" | "m" | "man" => Gender::Male,
            "neutral" | "none" | "n" => Gender::Neutral,
            _ => Gender::Female,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub api_key: String,
    pub proxy: Option<Proxy>,
    pub model: Model,
    pub voice: String,
    /// Grammatical gender the agent speaks in, matched to `voice`.
    pub voice_gender: Gender,
    pub language: String,
    pub net_check: NetCheckConfig,
    pub max_concurrent_channels: usize,
    /// Outbound greeting: if the callee produces nothing within this many ms,
    /// the agent greets first (words come from the system prompt). `0` disables
    /// the proactive greeting entirely (purely reactive — wait for the callee).
    pub greet_after_silence_ms: u64,
    /// Directory to write finalized CallRecord JSON to; None = skip persistence.
    pub transcript_dir: Option<PathBuf>,
    /// Directory for per-call uplink audio dumps (WAV). `None` disables it.
    pub dump_uplink_dir: Option<PathBuf>,
    /// Directory for per-call downlink audio dumps (WAV: Gemini 24k + phone 8k).
    /// `None` disables it.
    pub dump_downlink_dir: Option<PathBuf>,
    /// Safety cap on a single call's duration (seconds).
    pub max_call_secs: u64,
    /// Downlink audio-quality pacing (prebuffer/resume/abort thresholds).
    pub quality: QualityConfig,
    /// Retry policy for transient dial outcomes (busy, etc).
    pub retry: RetryConfig,
    /// Energy-VAD tuning for detecting that the callee has started speaking.
    pub vad: VadConfig,
    /// Instruction sent to the model when a session reconnects with lost context.
    pub resume_cue: String,
}

/// Downlink playout pacing thresholds. Tunes the tradeoff between latency
/// (lower prebuffer/resume = agent speaks sooner) and audible glitching
/// (higher = more cushion against jitter/underruns).
#[derive(Clone, Copy, Debug)]
pub struct QualityConfig {
    /// Samples-buffered target (ms) before (re)starting downlink playout.
    pub prebuffer_ms: u32,
    /// Faster re-arm target (ms) after a mid-turn underrun.
    pub resume_ms: u32,
    /// Cumulative underruns in one call that abort it as unusable; 0 = never.
    pub abort_underruns: u32,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self { prebuffer_ms: 140, resume_ms: 60, abort_underruns: 40 }
    }
}

/// Retry policy for transient dial outcomes.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Max dial attempts for a busy number (incl. the first). Default 3.
    pub busy_max_attempts: u32,
    /// Delay before a busy retry, ms. Default 300_000 (5 min).
    pub busy_retry_interval_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self { busy_max_attempts: 3, busy_retry_interval_ms: 300_000 }
    }
}

/// Energy-VAD tuning for detecting that the callee has started speaking.
#[derive(Debug, Clone, Copy)]
pub struct VadConfig {
    /// Absolute RMS floor: a frame below this is never speech, regardless of
    /// how low the adaptive noise floor has decayed. Telephone-calibrated
    /// against measured uplink levels (line background roughly 50-500 RMS,
    /// voiced "Алло" roughly 200-500 RMS), so genuine quiet callee speech
    /// still clears it once the floor has been calibrated (see
    /// `warmup_frames`).
    pub min_rms: u32,
    /// Speech = frame RMS >= max(min_rms, noise_floor * ratio). The very
    /// first observed frame seeds `noise_floor` unconditionally (there is no
    /// prior contrast to judge it against); from the second frame on, every
    /// frame — including during `warmup_frames` — is classified against this
    /// threshold, and the floor's EMA is updated only on frames that fall
    /// below it (non-speech).
    pub ratio: f32,
    /// Consecutive speech frames required to confirm onset (rejects clicks).
    pub onset_frames: u32,
    /// Bounds the startup window in which a sudden jump above the
    /// (already-seeded) noise floor can still confirm onset early —
    /// so an immediate loud onset shortly after a brief quiet baseline is
    /// caught rather than being averaged into the background. Detection is
    /// not disabled during this window; it uses the same
    /// `max(min_rms, noise_floor * ratio)` classification as afterward. At
    /// 20 ms/frame, 10 frames is about 200 ms.
    pub warmup_frames: u32,
}
impl Default for VadConfig {
    fn default() -> Self { Self { min_rms: 200, ratio: 3.0, onset_frames: 3, warmup_frames: 10 } }
}

/// Default wait before the agent greets a silent callee (see
/// [`ServerConfig::greet_after_silence_ms`]). With warm-start the Gemini
/// session is already connected at answer, so this is a natural conversational
/// beat, not dead air: ~1 s lets the callee get their "Алло?" in first (and,
/// if Gemini transcribes it in time, the agent responds reactively); if the
/// callee stays silent, the agent greets. Override per deployment with
/// `KUTSU_GREET_AFTER_SILENCE_MS` (0 disables the proactive greeting entirely).
pub const DEFAULT_GREET_AFTER_SILENCE_MS: u64 = 1000;

/// Default instruction sent to the model when a session reconnects with lost
/// context mid-exchange. An instruction, not a spoken phrase; the model speaks
/// it in the conversation's language. Override with `KUTSU_RESUME_CUE`.
pub const RESUME_CUE: &str = "The connection dropped briefly and the last thing \
    the other party said may be lost. Ask them to repeat what they just said, \
    replying in the same language you have been speaking.";

#[derive(Clone, Debug, Deserialize)]
pub struct ScenarioConfig {
    pub system_prompt: String,
    pub goal_schema: serde_json::Value,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_parses_from_json_and_model_defaults() {
        let json = r#"{
            "system_prompt": "You are an assistant.",
            "goal_schema": {"type":"object","required":["disposition"]},
            "context": {"name":"Ivan"}
        }"#;
        let sc: ScenarioConfig = serde_json::from_str(json).unwrap();
        assert_eq!(sc.system_prompt, "You are an assistant.");
        assert_eq!(sc.goal_schema["type"], "object");
        assert_eq!(sc.context.unwrap()["name"], "Ivan");

        assert!(matches!(Model::default(), Model::HalfCascade));
        let nc = NetCheckConfig::default();
        assert_eq!(nc.max_rtt_ms, 300);
        assert_eq!(nc.samples, 10);
        assert!(nc.enabled);
    }

    #[test]
    fn model_serde_uses_kebab_case() {
        assert_eq!(serde_json::to_string(&Model::NativeAudio).unwrap(), "\"native-audio\"");
        let m: Model = serde_json::from_str("\"half-cascade\"").unwrap();
        assert!(matches!(m, Model::HalfCascade));
    }

    #[test]
    fn sip_config_parses_and_from_user_defaults_to_username() {
        let json = r#"{"server":"192.168.88.243:5060","username":"kutsu","password":"kutsupw"}"#;
        let c: SipConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.server, "192.168.88.243:5060");
        assert_eq!(c.from_user(), "kutsu");
        assert_eq!(c.transport, SipTransportKind::Udp);
        assert!(!c.register);
        assert!(c.local_ip.is_none());
    }

    #[test]
    fn sip_config_from_user_override() {
        let json = r#"{"server":"s:5060","username":"u","password":"p","from_user":"caller"}"#;
        let c: SipConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.from_user(), "caller");
    }

    #[test]
    fn quality_config_defaults() {
        let q = QualityConfig::default();
        assert_eq!(q.prebuffer_ms, 140);
        assert_eq!(q.resume_ms, 60);
        assert_eq!(q.abort_underruns, 40);
    }

    #[test]
    fn server_config_has_engine_fields() {
        let c = ServerConfig {
            api_key: "k".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            voice_gender: crate::config::Gender::Female,
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: 3,
            greet_after_silence_ms: DEFAULT_GREET_AFTER_SILENCE_MS,
            transcript_dir: Some(std::path::PathBuf::from("/tmp/x")),
            dump_uplink_dir: None,
            dump_downlink_dir: None,
            max_call_secs: 600,
            quality: QualityConfig::default(),
            retry: RetryConfig::default(),
            vad: VadConfig::default(),
            resume_cue: RESUME_CUE.into(),
        };
        assert_eq!(c.max_call_secs, 600);
        assert!(c.transcript_dir.is_some());
    }

    #[test]
    fn default_greet_delay_is_1000ms() {
        assert_eq!(DEFAULT_GREET_AFTER_SILENCE_MS, 1000);
    }
}
