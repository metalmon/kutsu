//! Shared config construction for the `kutsu call` CLI and its integration test.

use crate::config::{
    Gender, Model, NetCheckConfig, Proxy, QualityConfig, RetryConfig, ScenarioConfig, ServerConfig,
    SipConfig, SipTransportKind, VadConfig, DEFAULT_GREET_AFTER_SILENCE_MS, RESUME_CUE,
};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

fn non_empty(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

fn env_u32(k: &str, d: u32) -> u32 {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn env_f32(k: &str, d: f32) -> f32 {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn env_bool(k: &str, d: bool) -> bool {
    std::env::var(k)
        .ok()
        .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(d)
}

/// Build (ServerConfig, SipConfig) from environment. This is the single
/// source of env -> config for every entry point (`kutsu call`, its
/// integration test, and `run_live`, which starts from this and overrides
/// only its CLI args). Errors if GEMINI_API_KEY is unset. SIP fields come
/// from KUTSU_SIP_*.
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
        model: match env_or("KUTSU_MODEL", "native-audio").trim().to_ascii_lowercase().as_str() {
            "half-cascade" | "half" | "halfcascade" | "cascade" => Model::HalfCascade,
            _ => Model::NativeAudio,
        },
        voice: env_or("KUTSU_VOICE", "Autonoe"),
        voice_gender: Gender::parse(&env_or("KUTSU_VOICE_GENDER", "female")),
        language: env_or("KUTSU_LANGUAGE", "en-US"),
        net_check: NetCheckConfig {
            enabled: env_bool("KUTSU_NETCHECK_ENABLED", true),
            samples: env_u32("KUTSU_NETCHECK_SAMPLES", 10),
            max_rtt_ms: env_u32("KUTSU_NETCHECK_MAX_RTT_MS", 300),
            max_jitter_ms: env_u32("KUTSU_NETCHECK_MAX_JITTER_MS", 50),
            max_loss_pct: env_f32("KUTSU_NETCHECK_MAX_LOSS_PCT", 2.0),
        },
        max_concurrent_channels: env_u32("KUTSU_MAX_CONCURRENT_CHANNELS", 3) as usize,
        greet_after_silence_ms: env_u64("KUTSU_GREET_AFTER_SILENCE_MS", DEFAULT_GREET_AFTER_SILENCE_MS),
        transcript_dir: non_empty("KUTSU_TRANSCRIPT_DIR").map(std::path::PathBuf::from),
        dump_uplink_dir: non_empty("KUTSU_DUMP_UPLINK_DIR").map(std::path::PathBuf::from),
        dump_downlink_dir: non_empty("KUTSU_DUMP_DOWNLINK_DIR").map(std::path::PathBuf::from),
        max_call_secs: env_u64("KUTSU_MAX_CALL_SECS", 600),
        quality: QualityConfig {
            prebuffer_ms: env_u32("KUTSU_QUALITY_PREBUFFER_MS", 800),
            resume_ms: env_u32("KUTSU_QUALITY_RESUME_MS", 400),
            abort_underruns: env_u32("KUTSU_QUALITY_ABORT_UNDERRUNS", 40),
        },
        retry: RetryConfig {
            busy_max_attempts: env_u32("KUTSU_BUSY_MAX_ATTEMPTS", 3),
            busy_retry_interval_ms: env_u64("KUTSU_BUSY_RETRY_INTERVAL_MS", 300_000),
        },
        vad: VadConfig {
            min_rms: env_u32("KUTSU_VAD_MIN_RMS", 200),
            ratio: env_f32("KUTSU_VAD_RATIO", 3.0),
            onset_frames: env_u32("KUTSU_VAD_ONSET_FRAMES", 3),
            warmup_frames: env_u32("KUTSU_VAD_WARMUP_FRAMES", 10),
        },
        agc: crate::config::AgcConfig {
            enabled: env_bool("KUTSU_AGC_ENABLED", true),
            target_dbfs: env_f32("KUTSU_AGC_TARGET_DBFS", -18.0),
            max_gain_db: env_f32("KUTSU_AGC_MAX_GAIN_DB", 30.0),
            noise_floor_rms: env_f32("KUTSU_AGC_NOISE_FLOOR_RMS", 200.0),
        },
        resume_cue: env_or("KUTSU_RESUME_CUE", RESUME_CUE),
    };
    let sip = SipConfig {
        server: env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060"),
        username: env_or("KUTSU_SIP_USER", "kutsu"),
        password: env_or("KUTSU_SIP_PASS", "kutsupw"),
        from_user: non_empty("KUTSU_SIP_FROM_USER"),
        local_ip: non_empty("KUTSU_SIP_LOCAL_IP").and_then(|s| s.parse().ok()),
        local_port: non_empty("KUTSU_SIP_LOCAL_PORT").and_then(|s| s.parse().ok()),
        sip_domain: non_empty("KUTSU_SIP_DOMAIN"),
        register: env_bool("KUTSU_SIP_REGISTER", false),
        register_expiry_secs: non_empty("KUTSU_SIP_REGISTER_EXPIRY").and_then(|s| s.parse().ok()),
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
