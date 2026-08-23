//! Shared config construction for the `kutsu call` CLI and its integration test.
//!
//! Layering (low → high precedence): struct `Default` → `kutsu.toml` → `KUTSU_*`
//! environment overlay. Secrets (`GEMINI_API_KEY`, SIP password, proxy
//! credentials) are sourced ONLY from the environment, never from the committed
//! TOML. The config file is discovered at `KUTSU_CONFIG`, else `kutsu.toml` in
//! the working directory; a missing file is fine (all defaults apply).

use crate::config::{Config, Gender, Model, Proxy, ScenarioConfig, ServerConfig, SipConfig};

fn non_empty(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

/// Override a `String` field iff the env var is set (including empty — an empty
/// override is meaningful for some fields, so callers that want non-empty use
/// [`over_opt_str`]).
fn over_str(k: &str, cur: &mut String) {
    if let Ok(v) = std::env::var(k) {
        *cur = v;
    }
}

/// Override an `Option<String>` iff the env var is set and non-empty.
fn over_opt_str(k: &str, cur: &mut Option<String>) {
    if let Some(v) = non_empty(k) {
        *cur = Some(v);
    }
}

fn over_u32(k: &str, cur: &mut u32) {
    if let Some(v) = std::env::var(k).ok().and_then(|s| s.parse().ok()) {
        *cur = v;
    }
}

fn over_u64(k: &str, cur: &mut u64) {
    if let Some(v) = std::env::var(k).ok().and_then(|s| s.parse().ok()) {
        *cur = v;
    }
}

fn over_usize(k: &str, cur: &mut usize) {
    if let Some(v) = std::env::var(k).ok().and_then(|s| s.parse().ok()) {
        *cur = v;
    }
}

fn over_f32(k: &str, cur: &mut f32) {
    if let Some(v) = std::env::var(k).ok().and_then(|s| s.parse().ok()) {
        *cur = v;
    }
}

fn over_bool(k: &str, cur: &mut bool) {
    if let Some(v) =
        std::env::var(k)
            .ok()
            .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            })
    {
        *cur = v;
    }
}

fn over_path(k: &str, cur: &mut Option<std::path::PathBuf>) {
    if let Some(v) = non_empty(k) {
        *cur = Some(std::path::PathBuf::from(v));
    }
}

/// Path to the TOML config: `KUTSU_CONFIG` if set, else `kutsu.toml` in CWD.
fn config_path() -> std::path::PathBuf {
    std::env::var("KUTSU_CONFIG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("kutsu.toml"))
}

/// Build (ServerConfig, SipConfig) with the full layering: defaults, then
/// `kutsu.toml`, then the `KUTSU_*` env overlay. This is the single source of
/// config for every entry point (`kutsu call`, its integration test, and
/// `run_live`, which starts from this and overrides only its CLI args). Errors
/// if `GEMINI_API_KEY` is unset.
pub fn configs_from_env() -> anyhow::Result<(ServerConfig, SipConfig)> {
    let cfg = Config::load(&config_path())?;
    let (mut server, mut sip) = (cfg.server, cfg.sip);
    apply_env_overlay(&mut server, &mut sip)?;
    Ok((server, sip))
}

/// Apply the `KUTSU_*` (and secret) environment overlay on top of the loaded
/// file config, mutating in place. Only variables that are actually set override
/// a field, so the file/default value shows through otherwise.
fn apply_env_overlay(server: &mut ServerConfig, sip: &mut SipConfig) -> anyhow::Result<()> {
    // Secrets — env only, never from the committed TOML.
    server.api_key =
        std::env::var("GEMINI_API_KEY").map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set"))?;
    if let Some(url) = non_empty("PROXY_URL") {
        server.proxy = Some(Proxy {
            url,
            user: non_empty("PROXY_USER"),
            password: non_empty("PROXY_PASSWORD"),
        });
    }

    // Server knobs.
    if let Ok(m) = std::env::var("KUTSU_MODEL") {
        server.model = match m.trim().to_ascii_lowercase().as_str() {
            "half-cascade" | "half" | "halfcascade" | "cascade" => Model::HalfCascade,
            _ => Model::NativeAudio,
        };
    }
    over_str("KUTSU_VOICE", &mut server.voice);
    if let Ok(g) = std::env::var("KUTSU_VOICE_GENDER") {
        server.voice_gender = Gender::parse(&g);
    }
    over_str("KUTSU_LANGUAGE", &mut server.language);
    over_usize(
        "KUTSU_MAX_CONCURRENT_CHANNELS",
        &mut server.max_concurrent_channels,
    );
    over_u64(
        "KUTSU_GREET_AFTER_SILENCE_MS",
        &mut server.greet_after_silence_ms,
    );
    over_u64("KUTSU_MAX_CALL_SECS", &mut server.max_call_secs);
    over_path("KUTSU_TRANSCRIPT_DIR", &mut server.transcript_dir);
    over_path("KUTSU_DUMP_UPLINK_DIR", &mut server.dump_uplink_dir);
    over_path("KUTSU_DUMP_DOWNLINK_DIR", &mut server.dump_downlink_dir);

    // net_check.
    over_bool("KUTSU_NETCHECK_ENABLED", &mut server.net_check.enabled);
    over_u32("KUTSU_NETCHECK_SAMPLES", &mut server.net_check.samples);
    over_u32(
        "KUTSU_NETCHECK_MAX_RTT_MS",
        &mut server.net_check.max_rtt_ms,
    );
    over_u32(
        "KUTSU_NETCHECK_MAX_JITTER_MS",
        &mut server.net_check.max_jitter_ms,
    );
    over_f32(
        "KUTSU_NETCHECK_MAX_LOSS_PCT",
        &mut server.net_check.max_loss_pct,
    );
    over_f32(
        "KUTSU_UPLINK_LOSS_ABORT_PCT",
        &mut server.net_check.uplink_loss_abort_pct,
    );
    over_f32(
        "KUTSU_DOWNLINK_LOSS_ABORT_PCT",
        &mut server.net_check.downlink_loss_abort_pct,
    );

    // quality.
    over_u32(
        "KUTSU_QUALITY_PREBUFFER_MS",
        &mut server.quality.prebuffer_ms,
    );
    over_u32("KUTSU_QUALITY_RESUME_MS", &mut server.quality.resume_ms);
    over_u32(
        "KUTSU_QUALITY_ABORT_UNDERRUNS",
        &mut server.quality.abort_underruns,
    );

    // retry.
    over_u32(
        "KUTSU_BUSY_MAX_ATTEMPTS",
        &mut server.retry.busy_max_attempts,
    );
    over_u64(
        "KUTSU_BUSY_RETRY_INTERVAL_MS",
        &mut server.retry.busy_retry_interval_ms,
    );

    // vad.
    over_u32("KUTSU_VAD_MIN_RMS", &mut server.vad.min_rms);
    over_f32("KUTSU_VAD_RATIO", &mut server.vad.ratio);
    over_u32("KUTSU_VAD_ONSET_FRAMES", &mut server.vad.onset_frames);
    over_u32("KUTSU_VAD_WARMUP_FRAMES", &mut server.vad.warmup_frames);

    // agc.
    over_bool("KUTSU_AGC_ENABLED", &mut server.agc.enabled);
    over_f32("KUTSU_AGC_TARGET_DBFS", &mut server.agc.target_dbfs);
    over_f32("KUTSU_AGC_MAX_GAIN_DB", &mut server.agc.max_gain_db);
    over_f32("KUTSU_AGC_NOISE_FLOOR_RMS", &mut server.agc.noise_floor_rms);

    // prompts.
    over_str(
        "KUTSU_SYSTEM_PROMPT",
        &mut server.prompts.base_system_prompt,
    );
    over_str("KUTSU_GREET_CUE", &mut server.prompts.greet_cue);
    over_str("KUTSU_RESUME_CUE", &mut server.prompts.resume_cue);

    // SIP (password is a secret; the rest may come from TOML or env).
    over_str("KUTSU_SIP_SERVER", &mut sip.server);
    over_str("KUTSU_SIP_USER", &mut sip.username);
    over_str("KUTSU_SIP_PASS", &mut sip.password);
    over_opt_str("KUTSU_SIP_FROM_USER", &mut sip.from_user);
    over_opt_str("KUTSU_SIP_DOMAIN", &mut sip.sip_domain);
    if let Some(ip) = non_empty("KUTSU_SIP_LOCAL_IP").and_then(|s| s.parse().ok()) {
        sip.local_ip = Some(ip);
    }
    if let Some(port) = non_empty("KUTSU_SIP_LOCAL_PORT").and_then(|s| s.parse().ok()) {
        sip.local_port = Some(port);
    }
    over_bool("KUTSU_SIP_REGISTER", &mut sip.register);
    if let Some(exp) = non_empty("KUTSU_SIP_REGISTER_EXPIRY").and_then(|s| s.parse().ok()) {
        sip.register_expiry_secs = Some(exp);
    }

    Ok(())
}

/// A minimal valid scenario for the CLI: an empty object goal schema and no
/// context. The schema MUST be typed `object` — Gemini's setup validation
/// rejects a function-parameters schema that carries `properties` (which
/// [`augment_goal_schema`](crate::config::augment_goal_schema) always adds)
/// unless its type is OBJECT. The persona/base prompt comes from the config
/// (`[server.prompts]` / `KUTSU_SYSTEM_PROMPT`), not from here.
pub fn default_scenario() -> ScenarioConfig {
    ScenarioConfig {
        goal_schema: serde_json::json!({ "type": "object" }),
        context: None,
        prompt_override: None,
    }
}
