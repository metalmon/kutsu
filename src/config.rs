//! Server- and scenario-level configuration for the Gemini Live client.
//!
//! `ServerConfig` holds server-start defaults (credentials, model, voice, proxy,
//! network-check thresholds, concurrency cap). `ScenarioConfig` mirrors the
//! future `place_call` per-task arguments.

use serde::{Deserialize, Serialize};

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
}

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
}
