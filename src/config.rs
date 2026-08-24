//! Configuration for the SIP + Gemini Live call server.
//!
//! [`Config`] is the top-level file schema (`kutsu.toml`): `[server]` (with
//! nested `[server.net_check]`, `[server.quality]`, `[server.agc]`,
//! `[server.retry]`, `[server.vad]`, `[server.prompts]`) and `[sip]`. Every
//! field defaults, so a partial file is valid; secrets (`api_key`, SIP password,
//! proxy credentials) are never read from the file — they come from the
//! environment overlay in [`crate::main_support`].
//!
//! [`ServerConfig`] holds the deployment-stable defaults and [`PromptsConfig`]
//! the prompt text; [`ServerConfig::assemble_system_instruction`] composes a
//! call's system instruction. [`ScenarioConfig`] is the per-call layer (mirrors
//! `place_call`): the `goal_schema`, optional `context`, and an optional
//! `prompt_override`.

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
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
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
    /// Local UDP port to bind the SIP transport to. `None` (or `0`) lets the OS
    /// pick an ephemeral port. Set a fixed port for trunks that authorize by a
    /// static source `IP:port` (e.g. an IP-authorized SIP trunk): the
    /// outbound source port must then match the configured origination address.
    #[serde(default)]
    pub local_port: Option<u16>,

    /// SIP domain (host part) for the request/To/From URIs and the REGISTER
    /// registrar, e.g. `sip.example.com`. When set, outbound URIs carry this
    /// domain (resolved via DNS by the stack) instead of the numeric `server`
    /// address — required by trunks that route/authorize by SIP domain. When
    /// absent, the URIs use `server`'s host (backwards-compatible IP behaviour).
    #[serde(default)]
    pub sip_domain: Option<String>,
    /// Register a binding with the trunk before placing calls (REGISTER +
    /// digest, refreshed until shutdown). Required by registration-based trunks
    /// (login/password), e.g. a standard login-password SIP account.
    #[serde(default)]
    pub register: bool,
    /// Requested REGISTER binding expiry, seconds. `None` uses the stack
    /// default. Trunks often prefer a short value (often ~120).
    #[serde(default)]
    pub register_expiry_secs: Option<u64>,

    // --- extension seams; parsed but not yet wired ---
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
#[derive(Default)]
pub enum Model {
    #[default]
    HalfCascade,
    NativeAudio,
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
#[serde(default)]
pub struct NetCheckConfig {
    pub enabled: bool,
    pub samples: u32,
    pub max_rtt_ms: u32,
    pub max_jitter_ms: u32,
    pub max_loss_pct: f32,
    /// Mid-call abort threshold: rolling-window uplink RTP loss (%) over the last
    /// ~8 s that marks the callee/cellular leg as unusable. Distinct from
    /// `max_loss_pct` (which gates the pre-dial Gemini-leg ping loss).
    pub uplink_loss_abort_pct: f32,
    /// Mid-call abort threshold: downlink RTP loss (%) the callee reports back via
    /// RTCP receiver reports (our audio -> callee). Best-effort: only active when
    /// the carrier actually sends RR; otherwise no downlink signal and no abort.
    pub downlink_loss_abort_pct: f32,
    /// Warm-up pings sent (and discarded) before the measured `samples`, so a
    /// cold first RTT does not inflate jitter. Excluded from rtt/jitter/loss.
    pub warmup_pings: u32,
    /// Reconnect-cost gate: max `connect_ms + setup_ms` (WS establish + setup
    /// handshake). Exceeding it fails preflight. 0 disables this gate.
    pub max_setup_ms: u32,
    /// Per-call preflight attempts before failing the call (>=1).
    pub retry_max: u32,
    /// Exponential backoff base between preflight attempts:
    /// `base * 2^(attempt-1)` ms.
    pub retry_backoff_base_ms: u64,
    /// Consecutive preflight failures that trip the shared-channel breaker.
    /// 0 disables the breaker.
    pub breaker_threshold: u32,
    /// How long the breaker holds the queue once tripped, in ms.
    pub cooldown_ms: u64,
}

impl Default for NetCheckConfig {
    fn default() -> Self {
        // Thresholds tuned for realtime voice (see spec).
        NetCheckConfig {
            enabled: true,
            samples: 10,
            max_rtt_ms: 300,
            max_jitter_ms: 50,
            max_loss_pct: 2.0,
            uplink_loss_abort_pct: 10.0,
            downlink_loss_abort_pct: 10.0,
            warmup_pings: 1,
            max_setup_ms: 2500,
            retry_max: 3,
            retry_backoff_base_ms: 5000,
            breaker_threshold: 3,
            cooldown_ms: 60000,
        }
    }
}

/// Grammatical gender the agent uses when referring to itself, matched to the
/// configured [`ServerConfig::voice`]. Kept as an explicit setting rather than
/// derived from the provider-specific voice name, so the prompt layer stays
/// provider-independent (the operator sets it to match whatever voice they pick).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
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

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
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
    /// Silence on the line (no speech from either party) after which the model
    /// is nudged with `prompts.end_call_cue` to wrap up (ms).
    pub dead_air_nudge_ms: u64,
    /// After the dead-air nudge fires, how long the model gets to call
    /// `end_call` before the call is force-ended (ms); also the grace window
    /// given to a late `end_call` after the callee hangs up abruptly. `0`
    /// disables wrap-up entirely (both the dead-air nudge and the
    /// abrupt-hangup harvest).
    pub wrap_up_grace_ms: u64,
    /// Base suggested polling interval (ms) returned to task-capable MCP clients
    /// for `place_call` tasks (the `pollIntervalMs` hint). Adapted up at task
    /// creation when the call is queued behind busy channels, capped by
    /// `mcp_poll_interval_max_ms`.
    pub mcp_poll_interval_ms: u64,
    /// Upper bound for the adapted `place_call` task poll interval (ms).
    pub mcp_poll_interval_max_ms: u64,
    /// Downlink audio-quality pacing (prebuffer/resume/abort thresholds).
    pub quality: QualityConfig,
    /// Retry policy for transient dial outcomes (busy, etc).
    pub retry: RetryConfig,
    /// Energy-VAD tuning for detecting that the callee has started speaking.
    pub vad: VadConfig,
    /// Adaptive gain applied to the uplink (callee → Gemini).
    pub agc: AgcConfig,
    /// All prompt text (base persona, cues, closing, gender, language) used to
    /// assemble a call's system instruction and runtime cues.
    pub prompts: PromptsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            proxy: None,
            model: Model::default(),
            voice: "Autonoe".into(),
            voice_gender: Gender::default(),
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: 3,
            greet_after_silence_ms: DEFAULT_GREET_AFTER_SILENCE_MS,
            transcript_dir: None,
            dump_uplink_dir: None,
            dump_downlink_dir: None,
            max_call_secs: 600,
            dead_air_nudge_ms: 25_000,
            wrap_up_grace_ms: 15_000,
            mcp_poll_interval_ms: 5_000,
            mcp_poll_interval_max_ms: 30_000,
            quality: QualityConfig::default(),
            retry: RetryConfig::default(),
            vad: VadConfig::default(),
            agc: AgcConfig::default(),
            prompts: PromptsConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Assemble the full system instruction for a call: the base persona (or the
    /// call's `prompt_override`), the objective (goal-preamble + the per-call
    /// `goal_schema` JSON, so the model knows what to gather — not just how to
    /// shape `end_call`), optional contact context, then the standard protocol
    /// layer (voice-gender + closing + language). This is the text handed to the
    /// crate as `SetupConfig::system_instruction`.
    pub fn assemble_system_instruction(&self, scenario: &ScenarioConfig) -> String {
        let p = &self.prompts;
        let mut s = scenario
            .prompt_override
            .clone()
            .unwrap_or_else(|| p.base_system_prompt.clone());

        s.push_str(&p.goal_preamble);
        s.push_str(
            &serde_json::to_string_pretty(&scenario.goal_schema)
                .unwrap_or_else(|_| scenario.goal_schema.to_string()),
        );

        if let Some(ctx) = &scenario.context {
            s.push_str("\n\n# Contact context\n");
            s.push_str(&ctx.to_string());
        }

        s.push_str(match self.voice_gender {
            Gender::Female => &p.gender_female,
            Gender::Male => &p.gender_male,
            Gender::Neutral => "",
        });
        s.push_str(&p.closing);
        s.push_str(&p.amd_instruction);
        s.push_str(&p.language_template.replace("{language}", &self.language));
        s
    }
}

/// Inject kutsu's standard `amd` field into a caller-supplied `goal_schema` so
/// the model always reports what it reached via the single `end_call` tool. The
/// caller's business fields are untouched; kutsu adds a top-level `amd` enum
/// (and marks it required). A non-object schema is returned unchanged.
pub fn augment_goal_schema(schema: &serde_json::Value) -> serde_json::Value {
    // A client may deliver the schema as a JSON *string* — some tool-call
    // serializers (e.g. strict function-calling over the Responses API)
    // stringify a free-form object argument. Parse it to an object first, else
    // it would be forwarded verbatim as an invalid function-parameters value
    // (Gemini WS 1007 "Invalid value at …parameters"). Provider-agnostic.
    let owned;
    let schema = match schema {
        serde_json::Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => {
                owned = v;
                &owned
            }
            Err(_) => schema,
        },
        _ => schema,
    };
    let mut s = schema.clone();
    let Some(obj) = s.as_object_mut() else {
        return s;
    };
    // A top-level `amd` property makes this an OBJECT schema. Gemini's setup
    // validation rejects `properties` on a schema whose type is not OBJECT
    // (WS close 1007: "parameters.properties: only allowed for OBJECT type"),
    // so guarantee `type: object` even when the caller passed a bare `{}` (the
    // CLI default_scenario) or a schema with properties but no explicit type.
    obj.entry("type")
        .or_insert_with(|| serde_json::json!("object"));
    let props = obj
        .entry("properties")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(props) = props.as_object_mut() {
        props.insert(
            "amd".into(),
            serde_json::json!({
                "type": "string",
                "enum": ["live", "voicemail", "announcement", "ivr", "hold"],
                "description": "What the far end is: `live` for a real person you \
                    conversed with; otherwise the non-live kind you detected."
            }),
        );
    }
    // Mark `amd` required so the model always reports it.
    let req = obj
        .entry("required")
        .or_insert_with(|| serde_json::json!([]));
    if let Some(arr) = req.as_array_mut()
        && !arr.iter().any(|v| v == "amd")
    {
        arr.push(serde_json::json!("amd"));
    }
    s
}

/// JSON-Schema keywords not in Gemini's function-parameter `Schema` subset.
/// Removing them is safe: they are validation/metadata Gemini would reject
/// outright ("Unknown name …" → WS close 1007), not structural type info.
const GEMINI_UNSUPPORTED_SCHEMA_KEYS: &[&str] = &[
    "additionalProperties",
    "unevaluatedProperties",
    "patternProperties",
    "additionalItems",
    "unevaluatedItems",
    "$schema",
    "$id",
    "$defs",
    "definitions",
    "examples",
];

/// Recursively drop [`GEMINI_UNSUPPORTED_SCHEMA_KEYS`] from every object node of
/// a JSON Schema so it fits Gemini's function-parameter Schema subset.
///
/// Gemini-specific and applied only where the Gemini setup is built (see
/// `gemini_live::build_setup_config`) — deliberately NOT in the
/// provider-agnostic [`augment_goal_schema`], since another provider (e.g.
/// OpenAI strict tools) may *require* the very keys Gemini rejects.
pub fn sanitize_gemini_schema(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for k in GEMINI_UNSUPPORTED_SCHEMA_KEYS {
                map.remove(*k);
            }
            // Gemini's Schema wants a single `type` string. Strict-tool
            // serializers emit `type: [T, "null"]` for optional fields; fold to
            // `type: T` + `nullable: true` (both accepted by Gemini).
            if let Some(serde_json::Value::Array(types)) = map.get("type").cloned() {
                let has_null = types.iter().any(|t| t.as_str() == Some("null"));
                if let Some(first) = types.into_iter().find(|t| t.as_str() != Some("null")) {
                    map.insert("type".into(), first);
                }
                if has_null {
                    map.insert("nullable".into(), serde_json::json!(true));
                }
            }
            for child in map.values_mut() {
                sanitize_gemini_schema(child);
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr.iter_mut() {
                sanitize_gemini_schema(child);
            }
        }
        _ => {}
    }
}

/// Downlink playout pacing thresholds. Tunes the tradeoff between latency
/// (lower prebuffer/resume = agent speaks sooner) and audible glitching
/// (higher = more cushion against jitter/underruns).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
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
        // Tuned for a real trunk: Gemini delivers audio in bursts ≥ prebuffer
        // (so a large prebuffer builds a jitter cushion without adding turn-start
        // latency — measured onset stays ~0), and that cushion rides out the
        // multi-hundred-ms mid-turn gaps in Gemini's delivery over the network
        // that a smaller buffer rendered as dropouts. The earlier 180/60 was
        // tuned against a zero-jitter LAN PBX; on a live carrier→mobile call 800
        // dropped starvation from ~1460 ms to ~0 with no audible latency cost.
        Self {
            prebuffer_ms: 800,
            resume_ms: 400,
            abort_underruns: 40,
        }
    }
}

/// Adaptive gain control for the uplink (callee → Gemini). Real trunks arrive
/// quiet (~-32 dBFS), which under-triggers Gemini's speech detection; AGC lifts
/// soft speech toward `target_dbfs` so it is reliably detected, while holding
/// gain on near-silence (below `noise_floor_rms`) so background noise is not
/// amplified into false speech.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct AgcConfig {
    /// Master toggle. When false the uplink is passed through untouched.
    pub enabled: bool,
    /// Target output level in dBFS (0 = full scale). Sustained speech is driven
    /// toward this level.
    pub target_dbfs: f32,
    /// Ceiling on applied gain, in dB — bounds how far very quiet input is
    /// boosted (prevents blowing up a nearly-silent line).
    pub max_gain_db: f32,
    /// Frames with RMS below this are treated as silence/noise: gain is held
    /// (not driven up toward the target), so background is not amplified.
    pub noise_floor_rms: f32,
}

impl Default for AgcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_dbfs: -18.0,
            max_gain_db: 30.0,
            noise_floor_rms: 200.0,
        }
    }
}

/// Retry policy for transient dial outcomes.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    /// Max dial attempts for a busy number (incl. the first). Default 3.
    pub busy_max_attempts: u32,
    /// Delay before a busy retry, ms. Default 300_000 (5 min).
    pub busy_retry_interval_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            busy_max_attempts: 3,
            busy_retry_interval_ms: 300_000,
        }
    }
}

/// Energy-VAD tuning for detecting that the callee has started speaking.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct VadConfig {
    /// Absolute RMS floor: a frame below this is never speech, regardless of
    /// how low the adaptive noise floor has decayed. Telephone-calibrated
    /// against measured uplink levels (line background roughly 50-500 RMS,
    /// voiced "Allo" roughly 200-500 RMS), so genuine quiet callee speech
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
    fn default() -> Self {
        Self {
            min_rms: 200,
            ratio: 3.0,
            onset_frames: 3,
            warmup_frames: 10,
        }
    }
}

/// Default wait before the agent greets a silent callee (see
/// [`ServerConfig::greet_after_silence_ms`]). With warm-start the Gemini
/// session is already connected at answer, so this is a natural conversational
/// beat, not dead air: ~1 s lets the callee get their "Allo?" in first (and,
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

/// Default base persona/behaviour for the agent. A deployment-stable prompt
/// (English by repo convention); operators override it in `[server.prompts]` or
/// via `KUTSU_SYSTEM_PROMPT`. The per-call `goal_schema` + `context` carry what
/// varies between calls.
pub const BASE_SYSTEM_PROMPT: &str = "You are a helpful voice agent making a \
    phone call. Speak naturally and conversationally, keep your replies short, \
    and stay on task.";

/// Preamble that introduces the per-call `goal_schema` inside the system prompt
/// so the model knows what to steer the conversation toward and collect — not
/// only how to shape the final `end_call` payload. The schema JSON is appended
/// right after this text by [`ServerConfig::assemble_system_instruction`].
pub const GOAL_PREAMBLE: &str = "\n\n# Your objective\nDuring this call, pursue \
    and gather the information described by the schema below, steering the \
    conversation toward it naturally. When the objective is met (or clearly \
    cannot be), submit your findings by calling the `end_call` tool with a value \
    matching this JSON Schema:\n";

/// Call-closing directive appended to every system prompt so the model closes
/// deterministically via the tool (fixes the double-goodbye). English by repo
/// convention; the goodbye itself is spoken in the scenario's language.
pub const CLOSING_INSTRUCTION: &str = "\n\n# Ending the call\n\
    When the conversation is finished - you have said your goodbyes, agreed on a \
    next step, or the other party firmly refuses or is rude - say ONE short, \
    natural goodbye and, in the SAME turn, call the `end_call` tool. Say the \
    goodbye only once: the line is played in full and then the call is hung up, \
    so never add a second goodbye and do not keep talking after it.";

/// Kickoff cue sent as a user turn when the callee stays silent after answer. It
/// only hands the turn to the model — the greeting wording comes from the system
/// prompt. Override with `KUTSU_GREET_CUE`.
pub const GREET_CUE: &str = "The call has connected and the other party has not \
    spoken yet. Greet them now and begin the conversation as instructed.";

/// Default cue handed to the model when the line has gone quiet for
/// `dead_air_nudge_ms`, nudging it toward wrapping up via `end_call`. English by
/// repo convention; operators override it in `[server.prompts]` or via
/// `KUTSU_END_CALL_CUE`.
pub const END_CALL_CUE: &str = "The line has gone quiet. If the conversation is \
    finished, wrap up now and submit the result by calling the end_call tool.";

/// Cue injected when the callee has hung up (Stage-B harvest). Unlike
/// [`END_CALL_CUE`] (dead air, still connected), this is unconditional: the call
/// is already over, so the model must submit whatever it has via `end_call`
/// rather than deciding whether the conversation is "finished". English by repo
/// convention; override in `[server.prompts]` or via `KUTSU_HARVEST_CUE`.
pub const HARVEST_CUE: &str = "The other party has hung up; the call is over and \
    they can no longer hear you. Do not try to say anything else. Submit your \
    result now by calling the end_call tool with whatever information you \
    gathered so far. If the objective was not completed, still call end_call and \
    note in the summary that the callee hung up before it finished.";

/// Voice-gender directive for a female voice. English by repo convention and
/// deliberately language-neutral: operators add language-specific examples (e.g.
/// gendered verb endings) in their own `[server.prompts]` override.
pub const GENDER_FEMALE: &str = "\n\n# Your voice\nYou speak with a FEMALE voice. \
    Always refer to yourself using feminine grammatical forms (verbs, \
    adjectives, participles). Never use masculine self-reference.";

/// Voice-gender directive for a male voice (see [`GENDER_FEMALE`]).
pub const GENDER_MALE: &str = "\n\n# Your voice\nYou speak with a MALE voice. \
    Always refer to yourself using masculine grammatical forms (verbs, \
    adjectives, participles). Never use feminine self-reference.";

/// Language directive template. `{language}` is replaced with the configured
/// BCP-47 tag at assembly time. Pins the spoken language (essential on
/// native-audio, which ignores the structured `languageCode`).
pub const LANGUAGE_TEMPLATE: &str = "\n\n# Language\nSpeak ONLY in the language \
    with BCP-47 code `{language}`, always, from the very first word and in every \
    single reply. You may understand the other party when they use another \
    language, but you MUST always answer in `{language}`, pronouncing it cleanly \
    and naturally like a native speaker, with no foreign accent.";

/// Answering-machine / non-live detection, done inside the live session (no
/// separate classifier). Two rules: recognize obviously non-live audio, and —
/// for the hard case of background speech with no musical cue — use
/// interactivity (a live person answers you; background audio does not). The
/// model reports the outcome in the `amd` field of `end_call` (injected by
/// [`augment_goal_schema`]). English by repo convention.
pub const AMD_INSTRUCTION: &str = "\n\n# Detecting a non-live answer\n\
    You may reach something other than a live person ready to talk. Watch for:\n\
    - a voicemail / answering-machine greeting (set amd=voicemail),\n\
    - a carrier/operator recording such as \"the subscriber is unavailable or \
    switched off\" (set amd=announcement),\n\
    - an automated menu asking you to press or say options (set amd=ivr),\n\
    - hold music, ads, or voices/chatter in the background that are not \
    addressing you (set amd=hold).\n\
    Decisive test when audio alone is ambiguous (e.g. background speech with no \
    music): a LIVE person responds to you - they answer your greeting and your \
    questions and take turns. If, after you greet and ask a short question, the \
    other side does not engage with you (keeps talking without responding, talks \
    over you, or ignores your question), treat it as NOT live. As soon as you are \
    confident it is not a live person, do NOT continue the conversation: call \
    `end_call` immediately with the matching `amd` value. When you did have a \
    real back-and-forth with a person, set amd=live.";

/// All prompt text used to assemble a call's system instruction and the runtime
/// cues. Deployment-stable defaults live here (English-only); operators override
/// any field in `[server.prompts]` or via the matching `KUTSU_*` env var. The
/// per-call layer (`goal_schema`, `context`, optional `prompt_override`) is
/// [`ScenarioConfig`].
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct PromptsConfig {
    /// Base persona/behaviour, used when a call carries no `prompt_override`.
    pub base_system_prompt: String,
    /// Preamble printed before the per-call `goal_schema` in the prompt.
    pub goal_preamble: String,
    /// Call-closing (`end_call`) directive appended to every prompt.
    pub closing: String,
    /// Cue handed to the model to greet a silent callee.
    pub greet_cue: String,
    /// Cue handed to the model when the line has gone quiet for
    /// `dead_air_nudge_ms`, nudging it to wrap up via `end_call`.
    pub end_call_cue: String,
    /// Cue injected when the callee hangs up mid-call (Stage-B harvest): an
    /// unconditional instruction to submit the result via `end_call`.
    pub harvest_cue: String,
    /// Instruction sent to the model when a session reconnects with lost context.
    pub resume_cue: String,
    /// Voice directive for a female voice.
    pub gender_female: String,
    /// Voice directive for a male voice.
    pub gender_male: String,
    /// Language directive template; `{language}` is substituted at assembly.
    pub language_template: String,
    /// Non-live detection rule (voicemail/announcement/ivr/hold + interactivity).
    pub amd_instruction: String,
}

impl Default for PromptsConfig {
    fn default() -> Self {
        Self {
            base_system_prompt: BASE_SYSTEM_PROMPT.into(),
            goal_preamble: GOAL_PREAMBLE.into(),
            closing: CLOSING_INSTRUCTION.into(),
            greet_cue: GREET_CUE.into(),
            end_call_cue: END_CALL_CUE.into(),
            harvest_cue: HARVEST_CUE.into(),
            resume_cue: RESUME_CUE.into(),
            gender_female: GENDER_FEMALE.into(),
            gender_male: GENDER_MALE.into(),
            language_template: LANGUAGE_TEMPLATE.into(),
            amd_instruction: AMD_INSTRUCTION.into(),
        }
    }
}

/// Per-call arguments (mirrors `place_call`): what varies between calls. The
/// stable persona lives in [`PromptsConfig::base_system_prompt`]; a call may
/// replace it for this call only via `prompt_override`.
#[derive(Clone, Debug, Deserialize)]
pub struct ScenarioConfig {
    /// JSON Schema the agent fills and submits via `end_call`. Carries the call's
    /// objective (through its field descriptions) as well as the output shape.
    pub goal_schema: serde_json::Value,
    /// Optional lead/contact object injected into the prompt.
    #[serde(default)]
    pub context: Option<serde_json::Value>,
    /// Optional per-call persona override, replacing `base_system_prompt`.
    #[serde(default)]
    pub prompt_override: Option<String>,
}

/// Top-level file configuration: everything loadable from `kutsu.toml`. Secrets
/// (`api_key`, SIP password, proxy credentials) are NOT sourced here — they come
/// from the environment overlay. Every field defaults, so a partial file is
/// valid; `kutsu.toml` need only carry what a deployment overrides.
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub sip: SipConfig,
}

impl Config {
    /// Parse a TOML string over the defaults (missing fields keep their default).
    pub fn from_toml_str(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    /// Load from a TOML file. A missing file yields all-defaults (`Ok`); an error
    /// is returned only when the file exists but cannot be read or parsed.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Self::from_toml_str(&s)
                .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(anyhow::anyhow!("failed to read {}: {e}", path.display())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_parses_from_json_and_model_defaults() {
        let json = r#"{
            "goal_schema": {"type":"object","required":["disposition"]},
            "context": {"name":"Alex Carter"}
        }"#;
        let sc: ScenarioConfig = serde_json::from_str(json).unwrap();
        assert_eq!(sc.goal_schema["type"], "object");
        assert_eq!(sc.context.unwrap()["name"], "Alex Carter");
        assert!(sc.prompt_override.is_none());

        assert!(matches!(Model::default(), Model::HalfCascade));
        let nc = NetCheckConfig::default();
        assert_eq!(nc.max_rtt_ms, 300);
        assert_eq!(nc.samples, 10);
        assert!(nc.enabled);
        assert_eq!(nc.uplink_loss_abort_pct, 10.0);
        assert_eq!(nc.downlink_loss_abort_pct, 10.0);
    }

    #[test]
    fn net_check_downlink_threshold_parses_from_toml() {
        let cfg =
            Config::from_toml_str("[server.net_check]\ndownlink_loss_abort_pct = 25.0\n").unwrap();
        assert_eq!(cfg.server.net_check.downlink_loss_abort_pct, 25.0);
        // other net_check fields keep their defaults
        assert_eq!(cfg.server.net_check.uplink_loss_abort_pct, 10.0);
    }

    #[test]
    fn netcheck_resilience_defaults() {
        let nc = NetCheckConfig::default();
        assert_eq!(nc.warmup_pings, 1);
        assert_eq!(nc.max_setup_ms, 2500);
        assert_eq!(nc.retry_max, 3);
        assert_eq!(nc.retry_backoff_base_ms, 5000);
        assert_eq!(nc.breaker_threshold, 3);
        assert_eq!(nc.cooldown_ms, 60000);
    }

    #[test]
    fn netcheck_resilience_parses_from_toml() {
        let cfg = Config::from_toml_str(
            "[server.net_check]\nretry_max = 5\ncooldown_ms = 90000\n",
        )
        .unwrap();
        assert_eq!(cfg.server.net_check.retry_max, 5);
        assert_eq!(cfg.server.net_check.cooldown_ms, 90000);
        // untouched fields keep defaults
        assert_eq!(cfg.server.net_check.warmup_pings, 1);
    }

    #[test]
    fn model_serde_uses_kebab_case() {
        assert_eq!(
            serde_json::to_string(&Model::NativeAudio).unwrap(),
            "\"native-audio\""
        );
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
        assert!(c.local_port.is_none());
    }

    #[test]
    fn sip_config_parses_local_port() {
        let json = r#"{"server":"s:5060","username":"u","password":"p","local_port":5060}"#;
        let c: SipConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.local_port, Some(5060));
    }

    #[test]
    fn sip_config_register_defaults_off_and_parses() {
        let base = r#"{"server":"s:5060","username":"u","password":"p"}"#;
        let c: SipConfig = serde_json::from_str(base).unwrap();
        assert!(!c.register);
        assert!(c.sip_domain.is_none());
        assert!(c.register_expiry_secs.is_none());

        let json = r#"{"server":"37.139.38.224:5060","username":"06733","password":"p",
            "sip_domain":"sip.example.com","register":true,"register_expiry_secs":120}"#;
        let c: SipConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.sip_domain.as_deref(), Some("sip.example.com"));
        assert!(c.register);
        assert_eq!(c.register_expiry_secs, Some(120));
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
        assert_eq!(q.prebuffer_ms, 800);
        assert_eq!(q.resume_ms, 400);
        assert_eq!(q.abort_underruns, 40);
    }

    #[test]
    fn wrapup_defaults() {
        let s = ServerConfig::default();
        assert_eq!(s.dead_air_nudge_ms, 25_000);
        assert_eq!(s.wrap_up_grace_ms, 15_000);
        assert!(!PromptsConfig::default().end_call_cue.is_empty());
        assert!(!PromptsConfig::default().harvest_cue.is_empty());
    }

    #[test]
    fn server_config_has_engine_fields() {
        let c = ServerConfig {
            transcript_dir: Some(std::path::PathBuf::from("/tmp/x")),
            ..Default::default()
        };
        assert_eq!(c.max_call_secs, 600);
        assert!(c.transcript_dir.is_some());
        assert_eq!(c.prompts.resume_cue, RESUME_CUE);
    }

    fn scenario(goal: serde_json::Value) -> ScenarioConfig {
        ScenarioConfig {
            goal_schema: goal,
            context: None,
            prompt_override: None,
        }
    }

    #[test]
    fn assemble_injects_base_prompt_goal_schema_and_layers() {
        let cfg = ServerConfig::default(); // Female, en-US
        let sys = cfg.assemble_system_instruction(&scenario(serde_json::json!({
            "type": "object",
            "properties": {"disposition": {"type": "string", "description": "call outcome"}}
        })));
        // base persona first
        assert!(sys.starts_with(BASE_SYSTEM_PROMPT));
        // objective section carries the schema so the model knows what to gather
        assert!(sys.contains("# Your objective"));
        assert!(sys.contains("disposition"));
        assert!(sys.contains("call outcome"));
        // protocol layer
        assert!(sys.contains("FEMALE voice"));
        assert!(sys.contains("end_call"));
        assert!(sys.contains("# Language"));
        assert!(sys.contains("en-US"));
    }

    #[test]
    fn assemble_uses_prompt_override_and_context_and_male_gender() {
        let mut cfg = ServerConfig {
            voice_gender: Gender::Male,
            ..Default::default()
        };
        cfg.language = "ru-RU".into();
        let sc = ScenarioConfig {
            goal_schema: serde_json::json!({"type": "object"}),
            context: Some(serde_json::json!({"name": "Alex Carter"})),
            prompt_override: Some("You are a debt collector.".into()),
        };
        let sys = cfg.assemble_system_instruction(&sc);
        assert!(sys.starts_with("You are a debt collector."));
        assert!(!sys.contains(BASE_SYSTEM_PROMPT));
        assert!(sys.contains("# Contact context"));
        assert!(sys.contains("Alex Carter"));
        assert!(sys.contains("MALE voice"));
        assert!(sys.contains("ru-RU"));
    }

    #[test]
    fn default_prompts_are_english_only() {
        // Clean-OSS invariant: no Cyrillic in the shipped prompt defaults.
        let p = PromptsConfig::default();
        for s in [
            &p.base_system_prompt,
            &p.goal_preamble,
            &p.closing,
            &p.greet_cue,
            &p.resume_cue,
            &p.gender_female,
            &p.gender_male,
            &p.language_template,
            &p.amd_instruction,
        ] {
            assert!(
                s.is_ascii(),
                "prompt default must be ASCII/English-only: {s:?}"
            );
        }
    }

    #[test]
    fn augment_goal_schema_injects_amd_and_keeps_caller_fields() {
        let caller = serde_json::json!({
            "type": "object",
            "properties": { "disposition": { "type": "string" } },
            "required": ["disposition"]
        });
        let aug = augment_goal_schema(&caller);
        // caller field preserved
        assert_eq!(aug["properties"]["disposition"]["type"], "string");
        // amd injected + required
        assert_eq!(aug["properties"]["amd"]["type"], "string");
        let req: Vec<&str> = aug["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(req.contains(&"disposition"));
        assert!(req.contains(&"amd"));
        assert!(
            aug["properties"]["amd"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "hold")
        );
    }

    #[test]
    fn augment_goal_schema_leaves_non_object_unchanged() {
        let s = serde_json::json!("not an object");
        assert_eq!(augment_goal_schema(&s), s);
    }

    #[test]
    fn augment_goal_schema_forces_object_type() {
        // Gemini's setup validation rejects `properties` on a schema whose type
        // is not OBJECT (WS close 1007: "parameters.properties: only allowed
        // for OBJECT type"). augment always adds `properties`, so it MUST also
        // guarantee `type: object` — including for a bare `{}` (the CLI's
        // default_scenario) and a schema that had properties but no type.
        let aug = augment_goal_schema(&serde_json::json!({}));
        assert_eq!(aug["type"], "object");
        assert!(aug["properties"]["amd"].is_object());

        let aug2 = augment_goal_schema(&serde_json::json!({
            "properties": { "name": { "type": "string" } }
        }));
        assert_eq!(aug2["type"], "object");
    }

    #[test]
    fn sanitize_gemini_schema_strips_unsupported_keys_recursively() {
        // Strict OpenAI-style tool schemas add `additionalProperties`, which
        // Gemini's Schema subset rejects (WS 1007 "Unknown name"). The Gemini
        // sanitizer must strip it — including from nested object properties —
        // while leaving valid structure intact.
        let mut s = serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": {
                "nested": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": { "x": { "type": "string" } }
                }
            },
            "required": ["nested"]
        });
        sanitize_gemini_schema(&mut s);
        assert!(s.get("additionalProperties").is_none());
        assert!(s.get("$schema").is_none());
        assert!(
            s["properties"]["nested"]
                .get("additionalProperties")
                .is_none()
        );
        assert_eq!(
            s["properties"]["nested"]["properties"]["x"]["type"],
            "string"
        );
        assert_eq!(s["type"], "object");
    }

    #[test]
    fn augment_goal_schema_stays_provider_agnostic() {
        // augment must NOT strip provider-specific keys — that belongs to the
        // Gemini setup layer, so another provider can keep them.
        let aug = augment_goal_schema(&serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "x": { "type": "string" } }
        }));
        assert_eq!(aug["additionalProperties"], false);
    }

    #[test]
    fn augment_goal_schema_parses_stringified_schema() {
        // Some tool-call serializers deliver goal_schema as a JSON string;
        // augment must parse it, not forward it verbatim (which becomes an
        // invalid function-parameters value → Gemini WS 1007 "Invalid value at
        // …parameters").
        let s = augment_goal_schema(&serde_json::json!(
            "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"string\"}},\"required\":[\"x\"]}"
        ));
        assert_eq!(s["type"], "object");
        assert_eq!(s["properties"]["x"]["type"], "string");
        assert!(s["properties"]["amd"].is_object());
    }

    #[test]
    fn sanitize_gemini_schema_folds_type_array_to_nullable() {
        let mut s = serde_json::json!({
            "type": "object",
            "properties": { "x": { "type": ["string", "null"], "description": "d" } }
        });
        sanitize_gemini_schema(&mut s);
        assert_eq!(s["properties"]["x"]["type"], "string");
        assert_eq!(s["properties"]["x"]["nullable"], true);
    }

    #[test]
    fn assemble_includes_amd_instruction() {
        let cfg = ServerConfig::default();
        let sys = cfg.assemble_system_instruction(&scenario(serde_json::json!({"type": "object"})));
        assert!(sys.contains("amd=voicemail"), "AMD rule must be present");
        assert!(sys.contains("amd=hold"));
    }

    #[test]
    fn partial_toml_overrides_only_given_fields() {
        let cfg = Config::from_toml_str(
            r#"
            [server]
            voice = "Charon"
            language = "de-DE"

            [server.net_check]
            max_rtt_ms = 500

            [server.prompts]
            base_system_prompt = "custom persona"

            [sip]
            server = "sip.example.com:5060"
            username = "u"
            password = "p"
            "#,
        )
        .unwrap();
        // overridden
        assert_eq!(cfg.server.voice, "Charon");
        assert_eq!(cfg.server.language, "de-DE");
        assert_eq!(cfg.server.net_check.max_rtt_ms, 500);
        assert_eq!(cfg.server.prompts.base_system_prompt, "custom persona");
        assert_eq!(cfg.sip.server, "sip.example.com:5060");
        // untouched -> defaults preserved
        assert_eq!(cfg.server.net_check.samples, 10);
        assert_eq!(cfg.server.prompts.resume_cue, RESUME_CUE);
        assert_eq!(cfg.server.max_call_secs, 600);
    }

    #[test]
    fn load_missing_file_is_all_defaults() {
        let cfg = Config::load(std::path::Path::new("does-not-exist-xyz.toml")).unwrap();
        assert_eq!(cfg.server.voice, "Autonoe");
        assert!(cfg.server.api_key.is_empty());
    }

    #[test]
    fn shipped_example_toml_parses_and_is_english_only() {
        // `kutsu init` writes this file, so it must parse into Config and stay
        // English-only (clean-OSS invariant).
        let example = include_str!("../kutsu.example.toml");
        let cfg = Config::from_toml_str(example).expect("kutsu.example.toml must parse");
        assert_eq!(cfg.server.model, Model::HalfCascade);
        assert_eq!(cfg.sip.server, "sip.example.com:5060");
        assert!(
            example.is_ascii(),
            "kutsu.example.toml must be English-only"
        );
    }

    #[test]
    fn default_greet_delay_is_1000ms() {
        assert_eq!(DEFAULT_GREET_AFTER_SILENCE_MS, 1000);
    }
}
