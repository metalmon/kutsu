//! Server- and scenario-level configuration for the Gemini Live client.
//!
//! `ServerConfig` holds server-start defaults (credentials, model, voice, proxy,
//! network-check thresholds, concurrency cap). `ScenarioConfig` mirrors the
//! future `place_call` per-task arguments.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

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

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub api_key: String,
    pub proxy: Option<Proxy>,
    pub model: Model,
    pub voice: String,
    pub language: String,
    pub net_check: NetCheckConfig,
    pub max_concurrent_channels: usize,
    /// Outbound greeting: if the callee produces nothing within this many ms,
    /// the agent greets first (words come from the system prompt). `0` disables
    /// the proactive greeting entirely (purely reactive — wait for the callee).
    pub greet_after_silence_ms: u64,
}

/// Default wait before the agent greets a silent callee (see
/// [`ServerConfig::greet_after_silence_ms`]).
pub const DEFAULT_GREET_AFTER_SILENCE_MS: u64 = 4000;

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
}
