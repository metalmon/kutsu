//! Shared config construction for the `kutsu call` CLI and its integration test.

use crate::config::{
    Gender, Model, NetCheckConfig, Proxy, ScenarioConfig, ServerConfig, SipConfig,
    SipTransportKind, DEFAULT_GREET_AFTER_SILENCE_MS,
};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

fn non_empty(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

/// Build (ServerConfig, SipConfig) from environment (mirrors `run_live`'s
/// ServerConfig construction). Errors if GEMINI_API_KEY is unset. SIP fields
/// come from KUTSU_SIP_*.
pub fn configs_from_env() -> anyhow::Result<(ServerConfig, SipConfig)> {
    let api_key =
        std::env::var("GEMINI_API_KEY").map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set"))?;
    let proxy = non_empty("PROXY_URL").map(|url| Proxy {
        url,
        user: non_empty("PROXY_USER"),
        password: non_empty("PROXY_PASSWORD"),
    });
    let server = ServerConfig {
        api_key,
        proxy,
        model: Model::HalfCascade,
        voice: env_or("KUTSU_VOICE", "Autonoe"),
        voice_gender: Gender::parse(&env_or("KUTSU_VOICE_GENDER", "female")),
        language: env_or("KUTSU_LANGUAGE", "en-US"),
        net_check: NetCheckConfig::default(),
        max_concurrent_channels: 3,
        greet_after_silence_ms: DEFAULT_GREET_AFTER_SILENCE_MS,
        transcript_dir: non_empty("KUTSU_TRANSCRIPT_DIR").map(std::path::PathBuf::from),
        max_call_secs: 600,
    };
    let sip = SipConfig {
        server: env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060"),
        username: env_or("KUTSU_SIP_USER", "kutsu"),
        password: env_or("KUTSU_SIP_PASS", "kutsupw"),
        from_user: None,
        local_ip: None,
        register: false,
        transport: SipTransportKind::Udp,
    };
    Ok((server, sip))
}

/// A minimal valid scenario (system prompt from KUTSU_SYSTEM_PROMPT or a default).
pub fn default_scenario() -> ScenarioConfig {
    ScenarioConfig {
        system_prompt: env_or(
            "KUTSU_SYSTEM_PROMPT",
            "You are a friendly assistant making a phone call. Greet the person warmly and have a short, natural conversation.",
        ),
        goal_schema: serde_json::json!({}),
        context: None,
    }
}
