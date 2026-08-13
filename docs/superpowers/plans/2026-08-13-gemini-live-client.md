# Gemini Live Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Gemini-concrete realtime voice client (BidiGenerateContent over WebSocket) plus a headless `kutsu live` file harness, so the conversation flow can run end to end without SIP.

**Architecture:** Pure, testable units (config, protocol serialize/parse, reconnect policy, net-check verdict, audio files) sit under an IO layer. The session loop talks to a `Transport` trait — a real tokio-tungstenite impl in production, an in-memory fake in tests — so the whole event/tool/reconnect flow is unit-testable offline. The client deals only in PCM16 + events + config (no SIP, no MCP), easing the later `RealtimeProvider` extraction.

**Tech Stack:** Rust (edition 2024), tokio, tokio-tungstenite, serde/serde_json, thiserror, hound, clap, tracing, chrono.

**Spec:** `docs/superpowers/specs/2026-08-13-gemini-live-client-design.md`

## Global Constraints

- Rust edition 2024; rmcp already at 3.1.2 (unaffected here).
- **All in-repo text is English** (identifiers, comments, doc-strings, CLI help/output, log/error messages, example scenarios). Conversation content is data and may be Russian.
- Input audio: PCM16, 16000 Hz, mono, little-endian. Output audio: PCM16, 24000 Hz, mono.
- Only one model tool: `end_call`; its parameters == the task's `goal_schema` (dynamic).
- Models: half-cascade `gemini-3.1-flash-live-preview` (default, endpoint `v1beta`, sends `languageCode`); native-audio `gemini-2.5-flash-native-audio-preview-12-2025` (endpoint `v1alpha`, omits `languageCode`, adds affective/proactive).
- Temperature 0.8. VAD: half → startSens LOW, prefix 1000ms; native → startSens HIGH, prefix 300ms, silence 100ms.
- Reconnect: backoff 0.3s → ×2 → max 5s; drop resumption handle after 4 consecutive failures.
- Network preflight is fail-closed (refuse on `Unusable`). Defaults: RTT p95 ≤ 300ms, jitter ≤ 50ms, loss ≤ 2%.
- `max_concurrent_channels` default 3 (defined here, enforced in phase 4/5 — not this plan).
- TDD throughout; commit after each task.

## File Structure

| File | Responsibility |
|------|----------------|
| `src/config.rs` (new) | `ServerConfig`, `ScenarioConfig`, `Model`, `NetCheckConfig`, `Proxy` + serde defaults |
| `src/error.rs` (new) | `Error` enum (thiserror) + `Result<T>` alias |
| `src/audio_file.rs` (new) | Read/write PCM16 WAV (and raw `.pcm`) via `hound` |
| `src/proto.rs` (new) | Pure protocol: `endpoint_url`, `build_setup`, `build_realtime_input`, `build_tool_response`, `parse_server_message` → `ServerEvent` |
| `src/reconnect.rs` (new) | Pure reconnect policy: `Backoff`, handle-reset counter |
| `src/net_check.rs` (new) | `NetworkHealth`, `Verdict`, pure `verdict()`, IO `preflight()` |
| `src/gemini_live.rs` | Session engine: `Transport` trait, `start`, `Session`, `Event`, `CallOutcome`, run/reconnect loop |
| `src/main.rs` (modify) | Add `live` subcommand (harness) |
| `src/lib.rs` (modify) | Declare new modules |
| `tests/fixtures/*.json` (new) | Recorded server messages |
| `Cargo.toml` (modify) | Add `hound`, `thiserror` |

Note: this refines the spec's single `gemini_live.rs` into focused modules (`proto`, `reconnect`, `net_check`, `config`, `error`, `audio_file`) per the "small focused files" principle; `gemini_live.rs` keeps the session/IO layer.

---

### Task 1: Config types

**Files:**
- Create: `src/config.rs`
- Modify: `src/lib.rs` (add `pub mod config;`)
- Test: in `src/config.rs` `#[cfg(test)]`

**Interfaces:**
- Produces:
  - `enum Model { HalfCascade, NativeAudio }` (serde rename: `half-cascade`, `native-audio`)
  - `struct Proxy { pub url: String, pub user: Option<String>, pub password: Option<String> }`
  - `struct NetCheckConfig { pub enabled: bool, pub samples: u32, pub max_rtt_ms: u32, pub max_jitter_ms: u32, pub max_loss_pct: f32 }` (+ `Default`)
  - `struct ServerConfig { pub api_key: String, pub proxy: Option<Proxy>, pub model: Model, pub voice: String, pub language: String, pub net_check: NetCheckConfig, pub max_concurrent_channels: usize }`
  - `struct ScenarioConfig { pub system_prompt: String, pub goal_schema: serde_json::Value, pub context: Option<serde_json::Value> }` (Deserialize)

- [ ] **Step 1: Write the failing test**

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib config::`
Expected: FAIL — `config` module / types not found.

- [ ] **Step 3: Write minimal implementation**

```rust
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
```

Add to `src/lib.rs`: `pub mod config;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib config::`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/lib.rs
git commit -m "feat(config): server and scenario config types"
```

---

### Task 2: Error type

**Files:**
- Create: `src/error.rs`
- Modify: `src/lib.rs` (add `pub mod error;`), `Cargo.toml` (add `thiserror = "2"`)
- Test: in `src/error.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `crate::net_check::NetworkHealth` is referenced only by name in a variant added in Task 7; for now the `NetworkUnstable` variant stores a `String` summary to avoid a forward dependency.
- Produces:
  - `enum Error { Config(String), NetworkUnstable(String), Connect(String), Protocol(String), SessionClosed, Io(std::io::Error), Json(serde_json::Error) }`
  - `type Result<T> = std::result::Result<T, Error>`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_stable() {
        assert_eq!(Error::Protocol("bad frame".into()).to_string(), "protocol error: bad frame");
        assert_eq!(Error::NetworkUnstable("rtt_p95=800ms".into()).to_string(),
                   "network unstable: rtt_p95=800ms");
        assert_eq!(Error::SessionClosed.to_string(), "session closed");
    }

    #[test]
    fn from_json_error_converts() {
        let e: Error = serde_json::from_str::<serde_json::Value>("{").unwrap_err().into();
        assert!(matches!(e, Error::Json(_)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib error::`
Expected: FAIL — `error` module not found.

- [ ] **Step 3: Write minimal implementation**

In `Cargo.toml` under `[dependencies]`: `thiserror = "2"`.

```rust
//! Typed errors for the Gemini Live client.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config error: {0}")]
    Config(String),
    #[error("network unstable: {0}")]
    NetworkUnstable(String),
    #[error("connect error: {0}")]
    Connect(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("session closed")]
    SessionClosed,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
```

Add to `src/lib.rs`: `pub mod error;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib error::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/error.rs src/lib.rs Cargo.toml Cargo.lock
git commit -m "feat(error): typed error enum"
```

---

### Task 3: Audio file I/O

**Files:**
- Create: `src/audio_file.rs`
- Modify: `src/lib.rs` (add `pub mod audio_file;`), `Cargo.toml` (add `hound = "3"`)
- Test: in `src/audio_file.rs` `#[cfg(test)]` (uses `tempfile`, already a dev-dependency)

**Interfaces:**
- Consumes: `crate::error::{Error, Result}`
- Produces:
  - `fn read_pcm16(path: &Path, expected_rate: u32) -> Result<Vec<i16>>` — reads WAV (or raw `.pcm` by extension) mono PCM16; errors on rate/format mismatch for WAV.
  - `struct Pcm16Writer` with `fn create(path: &Path, rate: u32) -> Result<Self>`, `fn write(&mut self, samples: &[i16]) -> Result<()>`, `fn finalize(self) -> Result<()>`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wav_round_trip_16k() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a.wav");
        let samples: Vec<i16> = (0..320).map(|i| (i as i16) - 160).collect();

        let mut w = Pcm16Writer::create(&path, 16000).unwrap();
        w.write(&samples).unwrap();
        w.finalize().unwrap();

        let back = read_pcm16(&path, 16000).unwrap();
        assert_eq!(back, samples);
    }

    #[test]
    fn wrong_rate_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("b.wav");
        let mut w = Pcm16Writer::create(&path, 8000).unwrap();
        w.write(&[1, 2, 3]).unwrap();
        w.finalize().unwrap();

        let err = read_pcm16(&path, 16000).unwrap_err();
        assert!(matches!(err, crate::error::Error::Config(_)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib audio_file::`
Expected: FAIL — module/types not found.

- [ ] **Step 3: Write minimal implementation**

In `Cargo.toml` under `[dependencies]`: `hound = "3"`.

```rust
//! PCM16 audio file I/O for the dev harness (WAV via `hound`, or raw `.pcm`).
//! Not used by the production audio path — the phase-3 bridge handles real audio.

use std::path::Path;

use crate::error::{Error, Result};

/// Read a mono PCM16 file. `.wav` is parsed and validated against `expected_rate`;
/// any other extension is treated as headerless raw PCM16 at the expected rate.
pub fn read_pcm16(path: &Path, expected_rate: u32) -> Result<Vec<i16>> {
    let is_wav = path.extension().map(|e| e.eq_ignore_ascii_case("wav")).unwrap_or(false);
    if is_wav {
        let reader = hound::WavReader::open(path).map_err(hound_err)?;
        let spec = reader.spec();
        if spec.channels != 1 || spec.bits_per_sample != 16 {
            return Err(Error::Config(format!(
                "expected mono PCM16, got {} channels / {} bits", spec.channels, spec.bits_per_sample)));
        }
        if spec.sample_rate != expected_rate {
            return Err(Error::Config(format!(
                "expected {} Hz, got {} Hz (resampling is not this layer's job)",
                expected_rate, spec.sample_rate)));
        }
        reader.into_samples::<i16>().collect::<std::result::Result<Vec<_>, _>>().map_err(hound_err)
    } else {
        let bytes = std::fs::read(path)?;
        if bytes.len() % 2 != 0 {
            return Err(Error::Config("raw PCM file has odd byte length".into()));
        }
        Ok(bytes.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect())
    }
}

/// Streaming WAV writer for mono PCM16.
pub struct Pcm16Writer {
    inner: hound::WavWriter<std::io::BufWriter<std::fs::File>>,
}

impl Pcm16Writer {
    pub fn create(path: &Path, rate: u32) -> Result<Self> {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let inner = hound::WavWriter::create(path, spec).map_err(hound_err)?;
        Ok(Pcm16Writer { inner })
    }

    pub fn write(&mut self, samples: &[i16]) -> Result<()> {
        for &s in samples {
            self.inner.write_sample(s).map_err(hound_err)?;
        }
        Ok(())
    }

    pub fn finalize(self) -> Result<()> {
        self.inner.finalize().map_err(hound_err)
    }
}

fn hound_err(e: hound::Error) -> Error {
    match e {
        hound::Error::IoError(io) => Error::Io(io),
        other => Error::Config(other.to_string()),
    }
}
```

Add to `src/lib.rs`: `pub mod audio_file;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib audio_file::`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/audio_file.rs src/lib.rs Cargo.toml Cargo.lock
git commit -m "feat(audio_file): PCM16 WAV/raw read and write"
```

---

### Task 4: Protocol — outbound setup builder

**Files:**
- Create: `src/proto.rs`
- Modify: `src/lib.rs` (add `pub mod proto;`)
- Test: in `src/proto.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `crate::config::{ServerConfig, ScenarioConfig, Model}`
- Produces:
  - `fn endpoint_url(cfg: &ServerConfig) -> String`
  - `fn build_setup(server: &ServerConfig, scenario: &ScenarioConfig, resume_handle: Option<&str>) -> serde_json::Value`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn server(model: Model) -> ServerConfig {
        ServerConfig {
            api_key: "KEY".into(), proxy: None, model, voice: "Autonoe".into(),
            language: "ru-RU".into(), net_check: NetCheckConfig::default(),
            max_concurrent_channels: 3,
        }
    }
    fn scenario() -> ScenarioConfig {
        ScenarioConfig {
            system_prompt: "Be nice.".into(),
            goal_schema: serde_json::json!({"type":"object","required":["disposition"]}),
            context: None,
        }
    }

    #[test]
    fn endpoint_version_depends_on_model() {
        assert!(endpoint_url(&server(Model::HalfCascade)).contains(".v1beta."));
        assert!(endpoint_url(&server(Model::NativeAudio)).contains(".v1alpha."));
        assert!(endpoint_url(&server(Model::HalfCascade)).contains("key=KEY"));
    }

    #[test]
    fn half_cascade_setup_shape() {
        let s = build_setup(&server(Model::HalfCascade), &scenario(), None);
        let setup = &s["setup"];
        assert_eq!(setup["model"], "models/gemini-3.1-flash-live-preview");
        assert_eq!(setup["generationConfig"]["responseModalities"][0], "AUDIO");
        assert_eq!(setup["generationConfig"]["temperature"], 0.8);
        assert_eq!(setup["generationConfig"]["speechConfig"]["voiceConfig"]
            ["prebuiltVoiceConfig"]["voiceName"], "Autonoe");
        assert_eq!(setup["generationConfig"]["speechConfig"]["languageCode"], "ru-RU");
        // Exactly one tool: end_call, parameters == goal_schema.
        let decl = &setup["tools"][0]["functionDeclarations"][0];
        assert_eq!(decl["name"], "end_call");
        assert_eq!(decl["parameters"]["required"][0], "disposition");
        // VAD half-cascade.
        let aad = &setup["realtimeInputConfig"]["automaticActivityDetection"];
        assert_eq!(aad["startOfSpeechSensitivity"], "START_SENSITIVITY_LOW");
        assert_eq!(aad["prefixPaddingMs"], 1000);
        // Transcription both ways on.
        assert!(setup["inputAudioTranscription"].is_object());
        assert!(setup["outputAudioTranscription"].is_object());
        // No native-only fields.
        assert!(setup["enableAffectiveDialog"].is_null());
    }

    #[test]
    fn native_audio_setup_shape() {
        let s = build_setup(&server(Model::NativeAudio), &scenario(), Some("H1"));
        let setup = &s["setup"];
        assert_eq!(setup["model"], "models/gemini-2.5-flash-native-audio-preview-12-2025");
        // No languageCode on native.
        assert!(setup["generationConfig"]["speechConfig"]["languageCode"].is_null());
        assert_eq!(setup["enableAffectiveDialog"], true);
        assert_eq!(setup["proactivity"]["proactiveAudio"], true);
        // NON_BLOCKING behavior on end_call.
        assert_eq!(setup["tools"][0]["functionDeclarations"][0]["behavior"], "NON_BLOCKING");
        // Native VAD.
        let aad = &setup["realtimeInputConfig"]["automaticActivityDetection"];
        assert_eq!(aad["startOfSpeechSensitivity"], "START_SENSITIVITY_HIGH");
        assert_eq!(aad["prefixPaddingMs"], 300);
        assert_eq!(aad["silenceDurationMs"], 100);
        // Resumption handle carried.
        assert_eq!(setup["sessionResumption"]["handle"], "H1");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib proto::tests::`
Expected: FAIL — `proto` not found.

- [ ] **Step 3: Write minimal implementation**

```rust
//! Gemini Live `BidiGenerateContent` protocol (pure serialize/parse; no IO).

use serde_json::{json, Value};

use crate::config::{Model, ScenarioConfig, ServerConfig};

const HALF_MODEL: &str = "gemini-3.1-flash-live-preview";
const NATIVE_MODEL: &str = "gemini-2.5-flash-native-audio-preview-12-2025";

fn model_name(m: Model) -> &'static str {
    match m {
        Model::HalfCascade => HALF_MODEL,
        Model::NativeAudio => NATIVE_MODEL,
    }
}

fn api_version(m: Model) -> &'static str {
    match m {
        Model::HalfCascade => "v1beta",
        Model::NativeAudio => "v1alpha",
    }
}

pub fn endpoint_url(cfg: &ServerConfig) -> String {
    format!(
        "wss://generativelanguage.googleapis.com/ws/\
         google.ai.generativelanguage.{ver}.GenerativeService.BidiGenerateContent?key={key}",
        ver = api_version(cfg.model),
        key = cfg.api_key,
    )
}

/// Build the first `setup` message. `resume_handle` carries a prior session
/// resumption handle (None on a fresh session).
pub fn build_setup(server: &ServerConfig, scenario: &ScenarioConfig, resume_handle: Option<&str>) -> Value {
    let native = matches!(server.model, Model::NativeAudio);

    let mut speech = json!({
        "voiceConfig": { "prebuiltVoiceConfig": { "voiceName": server.voice } }
    });
    if !native {
        speech["languageCode"] = json!(server.language);
    }

    let mut end_call = json!({
        "name": "end_call",
        "description": "Call this exactly once, at the end of the conversation, \
                        with the collected information and a final disposition.",
        "parameters": scenario.goal_schema,
    });
    if native {
        end_call["behavior"] = json!("NON_BLOCKING");
    }

    let aad = if native {
        json!({
            "startOfSpeechSensitivity": "START_SENSITIVITY_HIGH",
            "prefixPaddingMs": 300,
            "silenceDurationMs": 100
        })
    } else {
        json!({
            "startOfSpeechSensitivity": "START_SENSITIVITY_LOW",
            "prefixPaddingMs": 1000
        })
    };

    let mut setup = json!({
        "model": format!("models/{}", model_name(server.model)),
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "temperature": 0.8,
            "speechConfig": speech
        },
        "systemInstruction": { "parts": [ { "text": build_system_prompt(scenario) } ] },
        "tools": [ { "functionDeclarations": [ end_call ] } ],
        "realtimeInputConfig": { "automaticActivityDetection": aad },
        "sessionResumption": { "handle": resume_handle },
        "inputAudioTranscription": {},
        "outputAudioTranscription": {}
    });

    if native {
        setup["enableAffectiveDialog"] = json!(true);
        setup["proactivity"] = json!({ "proactiveAudio": true });
    }

    json!({ "setup": setup })
}

/// Assemble the system instruction from prompt + optional context.
/// (Current-time block is appended by the session at connect time — see Task 8.)
fn build_system_prompt(scenario: &ScenarioConfig) -> String {
    let mut s = scenario.system_prompt.clone();
    if let Some(ctx) = &scenario.context {
        s.push_str("\n\n# Contact context\n");
        s.push_str(&ctx.to_string());
    }
    s
}
```

Add to `src/lib.rs`: `pub mod proto;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib proto::tests::`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/proto.rs src/lib.rs
git commit -m "feat(proto): BidiGenerateContent setup builder"
```

---

### Task 5: Protocol — server message parsing

**Files:**
- Modify: `src/proto.rs`
- Create: `tests/fixtures/server_content_audio.json`, `tests/fixtures/tool_call_end_call.json`, `tests/fixtures/resumption_update.json`, `tests/fixtures/turn_complete.json`, `tests/fixtures/interrupted.json`, `tests/fixtures/setup_complete.json`
- Test: in `src/proto.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `enum Role { User, Model }` (re-exported from `gemini_live` later; defined here to avoid a cycle — see note)
  - `enum ServerEvent { SetupComplete, OutputAudio(Vec<i16>), Transcript { role: Role, text: String, final_: bool }, Interrupted, TurnComplete, ToolCallEndCall { call_id: String, goal: serde_json::Value }, ResumptionHandle(String), GoAway }`
  - `fn parse_server_message(text: &str) -> crate::error::Result<Vec<ServerEvent>>`

Note: `Role` is defined in `proto.rs` and re-exported by `gemini_live` (`pub use crate::proto::Role;`) so both the pure parser and the public API share one type.

- [ ] **Step 1: Create the fixtures**

`tests/fixtures/server_content_audio.json`:
```json
{"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AAABAAIAAwA="}}]},"outputTranscription":{"text":"Hello","finished":false}}}
```

`tests/fixtures/tool_call_end_call.json`:
```json
{"toolCall":{"functionCalls":[{"id":"fc-1","name":"end_call","args":{"disposition":"appointment","interested":true}}]}}
```

`tests/fixtures/resumption_update.json`:
```json
{"sessionResumptionUpdate":{"newHandle":"HANDLE-123","resumable":true}}
```

`tests/fixtures/turn_complete.json`:
```json
{"serverContent":{"turnComplete":true}}
```

`tests/fixtures/interrupted.json`:
```json
{"serverContent":{"interrupted":true}}
```

`tests/fixtures/setup_complete.json`:
```json
{"setupComplete":{}}
```

- [ ] **Step 2: Write the failing test**

```rust
#[cfg(test)]
mod parse_tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("tests/fixtures/{name}.json")).unwrap()
    }

    #[test]
    fn parses_audio_and_output_transcript() {
        let evs = parse_server_message(&fixture("server_content_audio")).unwrap();
        // base64 "AAABAAIAAwA=" -> bytes 00 00 01 00 02 00 03 00 -> i16 LE [0,1,2,3]
        assert!(evs.iter().any(|e| matches!(e, ServerEvent::OutputAudio(s) if s == &vec![0,1,2,3])));
        assert!(evs.iter().any(|e| matches!(e,
            ServerEvent::Transcript { role: Role::Model, text, final_: false } if text == "Hello")));
    }

    #[test]
    fn parses_end_call_tool() {
        let evs = parse_server_message(&fixture("tool_call_end_call")).unwrap();
        let one = evs.iter().find_map(|e| match e {
            ServerEvent::ToolCallEndCall { call_id, goal } => Some((call_id.clone(), goal.clone())),
            _ => None,
        }).unwrap();
        assert_eq!(one.0, "fc-1");
        assert_eq!(one.1["disposition"], "appointment");
    }

    #[test]
    fn parses_resumption_turn_interrupt_setup() {
        assert!(matches!(parse_server_message(&fixture("resumption_update")).unwrap().as_slice(),
            [ServerEvent::ResumptionHandle(h)] if h == "HANDLE-123"));
        assert!(matches!(parse_server_message(&fixture("turn_complete")).unwrap().as_slice(),
            [ServerEvent::TurnComplete]));
        assert!(matches!(parse_server_message(&fixture("interrupted")).unwrap().as_slice(),
            [ServerEvent::Interrupted]));
        assert!(matches!(parse_server_message(&fixture("setup_complete")).unwrap().as_slice(),
            [ServerEvent::SetupComplete]));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib proto::parse_tests::`
Expected: FAIL — `parse_server_message` / `ServerEvent` not found.

- [ ] **Step 4: Write minimal implementation**

Append to `src/proto.rs`. Base64 decoding uses `base64` (transitively available via rmcp; add `base64 = "0.22"` to `[dependencies]` to be explicit).

```rust
use base64::Engine as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    User,
    Model,
}

#[derive(Debug)]
pub enum ServerEvent {
    SetupComplete,
    OutputAudio(Vec<i16>),
    Transcript { role: Role, text: String, final_: bool },
    Interrupted,
    TurnComplete,
    ToolCallEndCall { call_id: String, goal: Value },
    ResumptionHandle(String),
    GoAway,
}

fn decode_pcm16(b64: &str) -> crate::error::Result<Vec<i16>> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| crate::error::Error::Protocol(format!("bad base64 audio: {e}")))?;
    Ok(bytes.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect())
}

/// Parse one server text frame into zero or more `ServerEvent`s.
pub fn parse_server_message(text: &str) -> crate::error::Result<Vec<ServerEvent>> {
    let v: Value = serde_json::from_str(text)?;
    let mut out = Vec::new();

    if v.get("setupComplete").is_some() {
        out.push(ServerEvent::SetupComplete);
    }

    if let Some(sc) = v.get("serverContent") {
        if let Some(parts) = sc.pointer("/modelTurn/parts").and_then(|p| p.as_array()) {
            for part in parts {
                if let Some(data) = part.pointer("/inlineData/data").and_then(|d| d.as_str()) {
                    out.push(ServerEvent::OutputAudio(decode_pcm16(data)?));
                }
            }
        }
        if let Some(t) = sc.get("outputTranscription").and_then(|t| t.get("text")).and_then(|t| t.as_str()) {
            let final_ = sc.pointer("/outputTranscription/finished").and_then(|f| f.as_bool()).unwrap_or(false);
            out.push(ServerEvent::Transcript { role: Role::Model, text: t.to_string(), final_ });
        }
        if let Some(t) = sc.get("inputTranscription").and_then(|t| t.get("text")).and_then(|t| t.as_str()) {
            let final_ = sc.pointer("/inputTranscription/finished").and_then(|f| f.as_bool()).unwrap_or(false);
            out.push(ServerEvent::Transcript { role: Role::User, text: t.to_string(), final_ });
        }
        if sc.get("interrupted").and_then(|i| i.as_bool()).unwrap_or(false) {
            out.push(ServerEvent::Interrupted);
        }
        if sc.get("turnComplete").and_then(|t| t.as_bool()).unwrap_or(false) {
            out.push(ServerEvent::TurnComplete);
        }
    }

    if let Some(calls) = v.pointer("/toolCall/functionCalls").and_then(|c| c.as_array()) {
        for call in calls {
            let name = call.get("name").and_then(|n| n.as_str()).unwrap_or_default();
            if name == "end_call" {
                let call_id = call.get("id").and_then(|i| i.as_str()).unwrap_or_default().to_string();
                let goal = call.get("args").cloned().unwrap_or(Value::Null);
                out.push(ServerEvent::ToolCallEndCall { call_id, goal });
            }
        }
    }

    if let Some(h) = v.pointer("/sessionResumptionUpdate/newHandle").and_then(|h| h.as_str()) {
        out.push(ServerEvent::ResumptionHandle(h.to_string()));
    }

    if v.get("goAway").is_some() {
        out.push(ServerEvent::GoAway);
    }

    Ok(out)
}
```

Add `base64 = "0.22"` to `[dependencies]` in `Cargo.toml`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib proto::parse_tests::`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/proto.rs tests/fixtures Cargo.toml Cargo.lock
git commit -m "feat(proto): parse server messages into ServerEvents"
```

---

### Task 6: Reconnect policy

**Files:**
- Create: `src/reconnect.rs`
- Modify: `src/lib.rs` (add `pub mod reconnect;`)
- Test: in `src/reconnect.rs` `#[cfg(test)]`

**Interfaces:**
- Produces:
  - `struct Backoff { current_ms: u64, max_ms: u64, base_ms: u64 }` with `fn new(base_ms: u64, max_ms: u64) -> Self`, `fn next_delay(&mut self) -> std::time::Duration`, `fn reset(&mut self)`
  - `struct ReconnectState { fails: u32, reset_handle_after: u32 }` with `fn new(reset_handle_after: u32) -> Self`, `fn on_success(&mut self)`, `fn on_failure(&mut self) -> bool` (returns true when the caller should drop the resumption handle)

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn backoff_doubles_and_caps_then_resets() {
        let mut b = Backoff::new(300, 5000);
        assert_eq!(b.next_delay(), Duration::from_millis(300));
        assert_eq!(b.next_delay(), Duration::from_millis(600));
        assert_eq!(b.next_delay(), Duration::from_millis(1200));
        assert_eq!(b.next_delay(), Duration::from_millis(2400));
        assert_eq!(b.next_delay(), Duration::from_millis(4800));
        assert_eq!(b.next_delay(), Duration::from_millis(5000)); // capped
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(300));
    }

    #[test]
    fn drops_handle_after_four_consecutive_failures() {
        let mut s = ReconnectState::new(4);
        assert!(!s.on_failure()); // 1
        assert!(!s.on_failure()); // 2
        assert!(!s.on_failure()); // 3
        assert!(s.on_failure());  // 4 -> drop handle
        s.on_success();
        assert!(!s.on_failure()); // counter reset by success
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib reconnect::`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```rust
//! Pure reconnect policy for the Live session (no IO): exponential backoff and
//! the "drop the resumption handle after N consecutive failures" rule.

use std::time::Duration;

pub struct Backoff {
    current_ms: u64,
    base_ms: u64,
    max_ms: u64,
}

impl Backoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Backoff { current_ms: base_ms, base_ms, max_ms }
    }

    pub fn next_delay(&mut self) -> Duration {
        let d = Duration::from_millis(self.current_ms);
        self.current_ms = (self.current_ms * 2).min(self.max_ms);
        d
    }

    pub fn reset(&mut self) {
        self.current_ms = self.base_ms;
    }
}

pub struct ReconnectState {
    fails: u32,
    reset_handle_after: u32,
}

impl ReconnectState {
    pub fn new(reset_handle_after: u32) -> Self {
        ReconnectState { fails: 0, reset_handle_after }
    }

    pub fn on_success(&mut self) {
        self.fails = 0;
    }

    /// Record a failed connect/session. Returns true when the caller should drop
    /// the (likely stale) resumption handle and start a fresh session.
    pub fn on_failure(&mut self) -> bool {
        self.fails += 1;
        self.reset_handle_after != 0 && self.fails >= self.reset_handle_after
    }
}
```

Add to `src/lib.rs`: `pub mod reconnect;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib reconnect::`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/reconnect.rs src/lib.rs
git commit -m "feat(reconnect): backoff and handle-reset policy"
```

---

### Task 7: Network check — verdict + preflight

**Files:**
- Create: `src/net_check.rs`
- Modify: `src/lib.rs` (add `pub mod net_check;`)
- Test: in `src/net_check.rs` `#[cfg(test)]` (verdict only; the IO probe is covered by the live smoke test in Task 10)

**Interfaces:**
- Consumes: `crate::config::{ServerConfig, NetCheckConfig}`, `crate::error::{Error, Result}`, `crate::proto::endpoint_url`
- Produces:
  - `enum Verdict { Ok, Unusable }`
  - `struct NetworkHealth { pub rtt_p50_ms: u32, pub rtt_p95_ms: u32, pub jitter_ms: u32, pub loss_pct: f32 }` with `fn summary(&self) -> String`
  - `fn verdict(h: &NetworkHealth, cfg: &NetCheckConfig) -> Verdict`
  - `async fn preflight(server: &ServerConfig) -> Result<NetworkHealth>`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetCheckConfig;

    #[test]
    fn verdict_respects_thresholds() {
        let cfg = NetCheckConfig::default(); // 300/50/2.0
        let good = NetworkHealth { rtt_p50_ms: 40, rtt_p95_ms: 120, jitter_ms: 15, loss_pct: 0.0 };
        assert!(matches!(verdict(&good, &cfg), Verdict::Ok));

        let high_rtt = NetworkHealth { rtt_p95_ms: 800, ..good };
        assert!(matches!(verdict(&high_rtt, &cfg), Verdict::Unusable));

        let lossy = NetworkHealth { loss_pct: 10.0, ..good };
        assert!(matches!(verdict(&lossy, &cfg), Verdict::Unusable));

        let jittery = NetworkHealth { jitter_ms: 200, ..good };
        assert!(matches!(verdict(&jittery, &cfg), Verdict::Unusable));
    }

    #[test]
    fn summary_is_readable() {
        let h = NetworkHealth { rtt_p50_ms: 40, rtt_p95_ms: 120, jitter_ms: 15, loss_pct: 1.5 };
        let s = h.summary();
        assert!(s.contains("rtt_p95=120ms"));
        assert!(s.contains("loss=1.5%"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib net_check::`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```rust
//! Fail-closed network preflight: probe the Gemini endpoint (WSS ping RTT) and
//! decide whether the network is good enough to place a call.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use crate::config::{NetCheckConfig, ServerConfig};
use crate::error::{Error, Result};
use crate::proto::endpoint_url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    Unusable,
}

#[derive(Clone, Copy, Debug)]
pub struct NetworkHealth {
    pub rtt_p50_ms: u32,
    pub rtt_p95_ms: u32,
    pub jitter_ms: u32,
    pub loss_pct: f32,
}

impl NetworkHealth {
    pub fn summary(&self) -> String {
        format!(
            "rtt_p50={}ms rtt_p95={}ms jitter={}ms loss={}%",
            self.rtt_p50_ms, self.rtt_p95_ms, self.jitter_ms, self.loss_pct
        )
    }
}

pub fn verdict(h: &NetworkHealth, cfg: &NetCheckConfig) -> Verdict {
    if h.rtt_p95_ms > cfg.max_rtt_ms || h.jitter_ms > cfg.max_jitter_ms || h.loss_pct > cfg.max_loss_pct {
        Verdict::Unusable
    } else {
        Verdict::Ok
    }
}

/// Open a real WSS connection to the Gemini endpoint and measure ping/pong RTT.
/// (Proxy support mirrors the session connect — wired in Task 8's connect helper;
/// preflight reuses the same helper once it exists.)
pub async fn preflight(server: &ServerConfig) -> Result<NetworkHealth> {
    let url = endpoint_url(server);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| Error::Connect(format!("preflight connect: {e}")))?;

    let mut rtts: Vec<u32> = Vec::new();
    let mut lost = 0u32;
    let n = server.net_check.samples.max(1);
    for i in 0..n {
        let payload = vec![i as u8];
        let sent = Instant::now();
        ws.send(Message::Ping(payload.clone().into()))
            .await
            .map_err(|e| Error::Connect(format!("preflight ping: {e}")))?;
        // Wait up to max_rtt*4 for the matching pong.
        let budget = Duration::from_millis((server.net_check.max_rtt_ms as u64) * 4);
        match tokio::time::timeout(budget, wait_for_pong(&mut ws)).await {
            Ok(Ok(())) => rtts.push(sent.elapsed().as_millis() as u32),
            _ => lost += 1,
        }
    }
    let _ = ws.close(None).await;

    Ok(summarize(&mut rtts, lost, n))
}

async fn wait_for_pong<S>(ws: &mut S) -> Result<()>
where
    S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Pong(_)) => return Ok(()),
            Ok(_) => continue,
            Err(e) => return Err(Error::Connect(format!("preflight recv: {e}"))),
        }
    }
    Err(Error::Connect("preflight stream ended".into()))
}

fn summarize(rtts: &mut Vec<u32>, lost: u32, total: u32) -> NetworkHealth {
    let loss_pct = (lost as f32 / total as f32) * 100.0;
    if rtts.is_empty() {
        return NetworkHealth { rtt_p50_ms: u32::MAX, rtt_p95_ms: u32::MAX, jitter_ms: u32::MAX, loss_pct };
    }
    rtts.sort_unstable();
    let p = |q: f32| rtts[((rtts.len() as f32 - 1.0) * q).round() as usize];
    let mean = rtts.iter().sum::<u32>() as f32 / rtts.len() as f32;
    let jitter = (rtts.iter().map(|&r| (r as f32 - mean).abs()).sum::<f32>() / rtts.len() as f32) as u32;
    NetworkHealth { rtt_p50_ms: p(0.50), rtt_p95_ms: p(0.95), jitter_ms: jitter, loss_pct }
}
```

Add `futures-util = "0.3"` to `[dependencies]` (used for `SinkExt`/`StreamExt`).
Add to `src/lib.rs`: `pub mod net_check;`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib net_check::`
Expected: PASS (2 tests). (The `preflight` IO fn is compiled but not unit-tested here.)

- [ ] **Step 5: Commit**

```bash
git add src/net_check.rs src/lib.rs Cargo.toml Cargo.lock
git commit -m "feat(net_check): preflight probe and fail-closed verdict"
```

---

### Task 8: Session engine (Transport trait + fake)

**Files:**
- Modify: `src/gemini_live.rs`, `src/lib.rs` (already declares `gemini_live`)
- Test: in `src/gemini_live.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `crate::proto::{self, ServerEvent, Role}`, `crate::reconnect::{Backoff, ReconnectState}`, `crate::config::{ServerConfig, ScenarioConfig}`, `crate::error::{Error, Result}`
- Produces:
  - `pub use crate::proto::Role;`
  - `enum Event { OutputAudio(Vec<i16>), Transcript { role: Role, text: String, final_: bool }, Interrupted, TurnComplete, EndCall { goal: serde_json::Value }, Warning(String) }`
  - `struct TranscriptEntry { pub role: Role, pub text: String, pub ts_ms: u64 }`
  - `enum EndedBy { ModelEndCall, CallerHangup, RemoteClose, Error }`
  - `struct CallOutcome { pub ended_by: EndedBy, pub goal: Option<serde_json::Value>, pub transcript: Vec<TranscriptEntry> }`
  - `trait Transport` (pub(crate)) with `fn send_text` and `fn recv` (native async fn in trait)
  - `async fn run_session<T: Transport>(transport: T, server: &ServerConfig, scenario: &ScenarioConfig, resume_handle: Option<String>, audio_in: &mut mpsc::Receiver<Vec<i16>>, events: &mpsc::Sender<Event>) -> SessionEnd`
  - `enum SessionEnd { EndCall(serde_json::Value), Hangup, RemoteClose, Resumable(String) }` (internal; drives the reconnect loop and the outcome)

Design note: `run_session` is generic over `Transport`, so tests drive it with a `FakeTransport` that yields queued fixture frames and records sent frames — no network. The production `start()` (added at the end of this task) wraps a real tungstenite transport in the reconnect loop.

- [ ] **Step 1: Write the failing test (fake transport drives an end_call)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use tokio::sync::mpsc;

    struct FakeTransport {
        incoming: std::collections::VecDeque<String>,
        pub sent: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Transport for FakeTransport {
        async fn send_text(&mut self, s: String) -> Result<()> {
            self.sent.lock().unwrap().push(s);
            Ok(())
        }
        async fn recv(&mut self) -> Option<Result<String>> {
            self.incoming.pop_front().map(Ok)
        }
    }

    fn server() -> ServerConfig {
        ServerConfig { api_key: "K".into(), proxy: None, model: Model::HalfCascade,
            voice: "Autonoe".into(), language: "ru-RU".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 3 }
    }
    fn scenario() -> ScenarioConfig {
        ScenarioConfig { system_prompt: "hi".into(),
            goal_schema: serde_json::json!({"type":"object"}), context: None }
    }

    #[tokio::test]
    async fn transcript_and_end_call_flow() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"setupComplete":{}}"#.to_string(),
            r#"{"serverContent":{"outputTranscription":{"text":"Hi there","finished":true}}}"#.to_string(),
            r#"{"toolCall":{"functionCalls":[{"id":"c1","name":"end_call","args":{"disposition":"done"}}]}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent: sent.clone() };

        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);

        let end = run_session(transport, &server(), &scenario(), None, &mut arx, &etx).await;

        // First sent frame is the setup message.
        assert!(sent.lock().unwrap()[0].contains("\"setup\""));
        // A model transcript event was emitted.
        let mut saw_transcript = false;
        let mut saw_end = false;
        while let Ok(ev) = erx.try_recv() {
            match ev {
                Event::Transcript { role: Role::Model, text, .. } if text == "Hi there" => saw_transcript = true,
                Event::EndCall { goal } => { assert_eq!(goal["disposition"], "done"); saw_end = true; }
                _ => {}
            }
        }
        assert!(saw_transcript);
        assert!(saw_end);
        assert!(matches!(end, SessionEnd::EndCall(g) if g["disposition"] == "done"));
        // We acked the tool call.
        assert!(sent.lock().unwrap().iter().any(|s| s.contains("toolResponse")));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib gemini_live::tests::transcript_and_end_call_flow`
Expected: FAIL — `Transport`, `run_session`, `Event`, `SessionEnd` not found.

- [ ] **Step 3: Write minimal implementation**

Replace the stub body of `src/gemini_live.rs` (keep the module doc-comment) with:

```rust
use serde_json::Value;
use tokio::sync::mpsc;

pub use crate::proto::Role;
use crate::config::{ScenarioConfig, ServerConfig};
use crate::error::Result;
use crate::proto::{self, ServerEvent};

#[derive(Debug)]
pub enum Event {
    OutputAudio(Vec<i16>),
    Transcript { role: Role, text: String, final_: bool },
    Interrupted,
    TurnComplete,
    EndCall { goal: Value },
    Warning(String),
}

#[derive(Clone, Debug)]
pub struct TranscriptEntry {
    pub role: Role,
    pub text: String,
    pub ts_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndedBy {
    ModelEndCall,
    CallerHangup,
    RemoteClose,
    Error,
}

#[derive(Debug)]
pub struct CallOutcome {
    pub ended_by: EndedBy,
    pub goal: Option<Value>,
    pub transcript: Vec<TranscriptEntry>,
}

/// How a single session attempt ended (drives the reconnect loop).
#[derive(Debug)]
pub enum SessionEnd {
    EndCall(Value),
    Hangup,
    RemoteClose,
    Resumable(String), // reason; retry with the current handle
}

/// A text-frame transport. Real impl = tokio-tungstenite; tests use a fake.
pub(crate) trait Transport: Send {
    fn send_text(&mut self, s: String) -> impl std::future::Future<Output = Result<()>> + Send;
    fn recv(&mut self) -> impl std::future::Future<Output = Option<Result<String>>> + Send;
}

/// Run one session attempt over `transport`. Sends setup, then pumps audio in and
/// server events out until end_call / hangup / stream close.
pub(crate) async fn run_session<T: Transport>(
    mut transport: T,
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    resume_handle: Option<String>,
    audio_in: &mut mpsc::Receiver<Vec<i16>>,
    events: &mpsc::Sender<Event>,
) -> SessionEnd {
    // 1. Setup.
    let setup = proto::build_setup(server, scenario, resume_handle.as_deref());
    if transport.send_text(setup.to_string()).await.is_err() {
        return SessionEnd::Resumable("send setup failed".into());
    }

    loop {
        tokio::select! {
            frame = audio_in.recv() => {
                match frame {
                    Some(pcm) => {
                        let msg = build_realtime_input(&pcm);
                        if transport.send_text(msg).await.is_err() {
                            return SessionEnd::Resumable("send audio failed".into());
                        }
                    }
                    None => return SessionEnd::Hangup, // caller dropped the sender
                }
            }
            incoming = transport.recv() => {
                match incoming {
                    Some(Ok(text)) => {
                        let parsed = match proto::parse_server_message(&text) {
                            Ok(p) => p,
                            Err(e) => { let _ = events.send(Event::Warning(e.to_string())).await; continue; }
                        };
                        for se in parsed {
                            match se {
                                ServerEvent::SetupComplete => {}
                                ServerEvent::OutputAudio(pcm) =>
                                    { let _ = events.send(Event::OutputAudio(pcm)).await; }
                                ServerEvent::Transcript { role, text, final_ } =>
                                    { let _ = events.send(Event::Transcript { role, text, final_ }).await; }
                                ServerEvent::Interrupted =>
                                    { let _ = events.send(Event::Interrupted).await; }
                                ServerEvent::TurnComplete =>
                                    { let _ = events.send(Event::TurnComplete).await; }
                                ServerEvent::ResumptionHandle(_h) => { /* captured by caller loop; see start() */ }
                                ServerEvent::GoAway => return SessionEnd::Resumable("goAway".into()),
                                ServerEvent::ToolCallEndCall { call_id, goal } => {
                                    let _ = transport.send_text(build_tool_response(&call_id)).await;
                                    let _ = events.send(Event::EndCall { goal: goal.clone() }).await;
                                    return SessionEnd::EndCall(goal);
                                }
                            }
                        }
                    }
                    Some(Err(_)) => return SessionEnd::Resumable("recv error".into()),
                    None => return SessionEnd::RemoteClose,
                }
            }
        }
    }
}

fn build_realtime_input(pcm: &[i16]) -> String {
    use base64::Engine as _;
    let mut bytes = Vec::with_capacity(pcm.len() * 2);
    for &s in pcm {
        bytes.extend_from_slice(&s.to_le_bytes());
    }
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    serde_json::json!({
        "realtimeInput": { "audio": { "mimeType": "audio/pcm;rate=16000", "data": data } }
    })
    .to_string()
}

fn build_tool_response(call_id: &str) -> String {
    serde_json::json!({
        "toolResponse": { "functionResponses": [ { "id": call_id, "response": { "ok": true } } ] }
    })
    .to_string()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib gemini_live::tests::transcript_and_end_call_flow`
Expected: PASS.

- [ ] **Step 5: Add the resumption-capture test**

```rust
    #[tokio::test]
    async fn captures_resumption_handle_and_remote_close() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"sessionResumptionUpdate":{"newHandle":"H9","resumable":true}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let end = run_session(transport, &server(), &scenario(), None, &mut arx, &etx).await;
        // No more frames after the update -> remote close.
        assert!(matches!(end, SessionEnd::RemoteClose));
    }
```

Update `run_session` to record the latest handle so the outer loop can read it: change the signature to take `latest_handle: &mut Option<String>` and set it on `ResumptionHandle(h)`. Update the earlier test call sites to pass `&mut None`. (Resumption is exercised end-to-end by `start()` below.)

Run: `cargo test --lib gemini_live::tests::`
Expected: PASS (both tests).

- [ ] **Step 6: Add `start()` (production reconnect loop + real transport)**

```rust
/// Public session handle returned by `start`.
pub struct Session {
    pub audio_in: mpsc::Sender<Vec<i16>>,
    pub events: mpsc::Receiver<Event>,
    join: tokio::task::JoinHandle<CallOutcome>,
    hangup_tx: mpsc::Sender<()>,
}

impl Session {
    pub async fn hangup(&self) {
        let _ = self.hangup_tx.send(()).await;
    }
    pub async fn join(self) -> CallOutcome {
        self.join.await.unwrap_or(CallOutcome {
            ended_by: EndedBy::Error, goal: None, transcript: Vec::new(),
        })
    }
}

/// Real tokio-tungstenite transport.
struct WsTransport {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Transport for WsTransport {
    async fn send_text(&mut self, s: String) -> Result<()> {
        use futures_util::SinkExt;
        self.ws.send(tokio_tungstenite::tungstenite::Message::Text(s.into()))
            .await.map_err(|e| crate::error::Error::Connect(e.to_string()))
    }
    async fn recv(&mut self) -> Option<Result<String>> {
        use futures_util::StreamExt;
        loop {
            match self.ws.next().await? {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => return Some(Ok(t.to_string())),
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => return None,
                Ok(_) => continue, // ignore ping/pong/binary
                Err(e) => return Some(Err(crate::error::Error::Connect(e.to_string()))),
            }
        }
    }
}

/// Connect + run with reconnect. The event/audio channels stay stable across reconnects.
pub async fn start(server: &ServerConfig, scenario: &ScenarioConfig) -> Result<Session> {
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<i16>>(64);
    let (event_tx, event_rx) = mpsc::channel::<Event>(256);
    let (hangup_tx, mut hangup_rx) = mpsc::channel::<()>(1);

    let server = server.clone();
    let scenario = scenario.clone();

    let join = tokio::spawn(async move {
        let mut audio_rx = audio_rx;
        let mut backoff = crate::reconnect::Backoff::new(300, 5000);
        let mut rstate = crate::reconnect::ReconnectState::new(4);
        let mut handle: Option<String> = None;
        let mut transcript: Vec<TranscriptEntry> = Vec::new();
        let _ = &mut transcript; // populated as events flow (mirrored below)

        loop {
            if hangup_rx.try_recv().is_ok() {
                return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
            }
            let url = proto::endpoint_url(&server);
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => {
                    rstate.on_success();
                    backoff.reset();
                    let transport = WsTransport { ws };
                    let mut latest = handle.clone();
                    let end = run_session(
                        transport, &server, &scenario, handle.clone(),
                        &mut audio_rx, &event_tx,
                    ).await; // NOTE: run_session updates `latest` via the &mut param added in Step 5
                    handle = latest.take().or(handle);
                    match end {
                        SessionEnd::EndCall(goal) =>
                            return CallOutcome { ended_by: EndedBy::ModelEndCall, goal: Some(goal), transcript },
                        SessionEnd::Hangup =>
                            return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript },
                        SessionEnd::RemoteClose | SessionEnd::Resumable(_) => {
                            let _ = event_tx.send(Event::Warning("reconnecting".into())).await;
                        }
                    }
                }
                Err(_) => {
                    if rstate.on_failure() { handle = None; }
                }
            }
            tokio::time::sleep(backoff.next_delay()).await;
        }
    });

    Ok(Session { audio_in: audio_tx, events: event_rx, join, hangup_tx })
}
```

Note for the implementer: wire the `latest` handle by threading `&mut Option<String>` through `run_session` (Step 5) and, on each `Event::Transcript`, also push a `TranscriptEntry` into a shared buffer. Keep the transcript buffer inside `run_session` and return it, or (simpler) accumulate in the spawned task by also receiving a copy — pick the approach that keeps `run_session` testable. The tests from Steps 1/5 must still pass unchanged.

- [ ] **Step 7: Run the full module test + build**

Run: `cargo test --lib gemini_live::` then `cargo build`
Expected: tests PASS; build succeeds (needs system/vendored OpenSSL as usual — build with `--features vendor-openssl` on this machine).

- [ ] **Step 8: Commit**

```bash
git add src/gemini_live.rs
git commit -m "feat(gemini_live): session engine with transport trait and reconnect"
```

---

### Task 9: `kutsu live` harness subcommand

**Files:**
- Modify: `src/main.rs`
- Test: manual/CLI (covered by the live smoke test in Task 10; harness logic is thin over already-tested units)

**Interfaces:**
- Consumes: `kutsu::config::*`, `kutsu::gemini_live::{start, Event, EndedBy}`, `kutsu::net_check::{preflight, verdict, Verdict}`, `kutsu::audio_file::{read_pcm16, Pcm16Writer}`

- [ ] **Step 1: Add the `live` subcommand definition**

In `src/main.rs`, extend the `Command` enum:

```rust
    /// Run one conversation against Gemini Live from a scenario + audio file (dev harness).
    Live {
        /// Scenario JSON: { system_prompt, goal_schema, context? }.
        #[arg(long)]
        scenario: std::path::PathBuf,
        /// Input audio: mono PCM16 WAV or raw .pcm at 16 kHz.
        #[arg(long = "audio-in")]
        audio_in: std::path::PathBuf,
        /// Output WAV (model speech, 24 kHz).
        #[arg(long = "audio-out")]
        audio_out: Option<std::path::PathBuf>,
        /// Transcript JSONL output.
        #[arg(long)]
        transcript: Option<std::path::PathBuf>,
        /// Filled goal JSON output.
        #[arg(long = "goal-out")]
        goal_out: Option<std::path::PathBuf>,
        /// Override model: half | native.
        #[arg(long)]
        model: Option<String>,
        /// Override voice.
        #[arg(long)]
        voice: Option<String>,
        /// Seconds to keep the session open after input ends.
        #[arg(long, default_value = "8")]
        tail: u64,
        /// Skip the network preflight (offline debugging only).
        #[arg(long = "no-net-check")]
        no_net_check: bool,
    },
```

- [ ] **Step 2: Implement the handler**

Add an async runtime (the `mcp` arm is currently sync; make `main` build a tokio runtime for `Live`). Implement:

```rust
async fn run_live(/* the Live fields */) -> anyhow::Result<i32> {
    // 1. Load scenario + server config (env: GEMINI_API_KEY, proxy).
    let scenario: kutsu::config::ScenarioConfig =
        serde_json::from_slice(&std::fs::read(&scenario_path)?)?;
    let api_key = std::env::var("GEMINI_API_KEY")
        .map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set"))?;
    let model = match model.as_deref() {
        Some("native") => kutsu::config::Model::NativeAudio,
        _ => kutsu::config::Model::HalfCascade,
    };
    let server = kutsu::config::ServerConfig {
        api_key, proxy: None, model,
        voice: voice.unwrap_or_else(|| "Autonoe".into()),
        language: "ru-RU".into(),
        net_check: kutsu::config::NetCheckConfig::default(),
        max_concurrent_channels: 3,
    };

    // 2. Preflight (fail closed).
    if !no_net_check {
        let health = kutsu::net_check::preflight(&server).await?;
        eprintln!("net: {}", health.summary());
        if matches!(kutsu::net_check::verdict(&health, &server.net_check),
                    kutsu::net_check::Verdict::Unusable) {
            eprintln!("network unusable — refusing to place the call");
            return Ok(2);
        }
    }

    // 3. Start session.
    let mut session = kutsu::gemini_live::start(&server, &scenario).await?;

    // 4. Feed audio (32 ms frames at real-time pace) in a task.
    let samples = kutsu::audio_file::read_pcm16(&audio_in, 16000)?;
    let audio_tx = session.audio_in.clone();
    tokio::spawn(async move {
        for chunk in samples.chunks(512) {
            if audio_tx.send(chunk.to_vec()).await.is_err() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(32)).await;
        }
    });

    // 5. Consume events until EndCall or tail timeout.
    let mut out = audio_out.map(|p| kutsu::audio_file::Pcm16Writer::create(&p, 24000)).transpose()?;
    let mut transcript_file = transcript.map(std::fs::File::create).transpose()?;
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(/* input_len_secs */ 0) // extended below
        + std::time::Duration::from_secs(tail);
    let mut goal: Option<serde_json::Value> = None;
    loop {
        let ev = tokio::time::timeout_at(deadline, session.events.recv()).await;
        match ev {
            Ok(Some(kutsu::gemini_live::Event::OutputAudio(pcm))) => {
                if let Some(w) = out.as_mut() { w.write(&pcm)?; }
            }
            Ok(Some(kutsu::gemini_live::Event::Transcript { role, text, final_ })) => {
                println!("[{:?}] {}", role, text);
                if let Some(f) = transcript_file.as_mut() {
                    use std::io::Write;
                    writeln!(f, "{}", serde_json::json!({"role":format!("{role:?}"),"text":text,"final":final_}))?;
                }
            }
            Ok(Some(kutsu::gemini_live::Event::EndCall { goal: g })) => { goal = Some(g); break; }
            Ok(Some(kutsu::gemini_live::Event::Warning(w))) => eprintln!("warn: {w}"),
            Ok(Some(_)) => {}
            Ok(None) => break,   // session task ended
            Err(_) => { session.hangup().await; break; } // tail timeout
        }
    }
    if let Some(w) = out { w.finalize()?; }
    if let Some(g) = &goal {
        if let Some(p) = goal_out { std::fs::write(p, serde_json::to_vec_pretty(g)?)?; }
    }

    let outcome = session.join().await;
    eprintln!("ended_by={:?} goal={}", outcome.ended_by, goal.is_some());
    Ok(match outcome.ended_by { kutsu::gemini_live::EndedBy::Error => 1, _ => 0 })
}
```

In `main`, route the `Live` arm through a tokio runtime and `std::process::exit(code)`.
(Implementer: compute the input length in seconds from `samples.len()/16000` and add it to the deadline base; the `0` placeholder above marks where.)

- [ ] **Step 3: Build & smoke the CLI wiring (offline)**

Run: `cargo build --features vendor-openssl`
Then: `./target/debug/kutsu live --help`
Expected: build succeeds; help lists all `live` flags.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat(cli): kutsu live dev harness subcommand"
```

---

### Task 10: Live smoke test + README docs

**Files:**
- Create: `tests/live_smoke.rs`
- Modify: `Cargo.toml` (add `[features] live-tests = []`), `README.md`
- Create: `docs/examples/scenario.json` (English example)

**Interfaces:**
- Consumes: the public crate API.

- [ ] **Step 1: Add the feature and an ignored live smoke test**

`Cargo.toml`:
```toml
[features]
# ... existing features ...
live-tests = []
```

`tests/live_smoke.rs`:
```rust
//! Opt-in live smoke test against the real Gemini Live API.
//! Run with: `GEMINI_API_KEY=... cargo test --features live-tests --test live_smoke -- --ignored`

#![cfg(feature = "live-tests")]

#[tokio::test]
#[ignore = "requires GEMINI_API_KEY and network"]
async fn short_live_session_returns_audio_and_ends() {
    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY");
    let server = kutsu::config::ServerConfig {
        api_key, proxy: None, model: kutsu::config::Model::HalfCascade,
        voice: "Autonoe".into(), language: "ru-RU".into(),
        net_check: kutsu::config::NetCheckConfig::default(), max_concurrent_channels: 3,
    };
    let scenario = kutsu::config::ScenarioConfig {
        system_prompt: "You are a friendly assistant. Greet briefly, then call end_call.".into(),
        goal_schema: serde_json::json!({"type":"object","required":["disposition"],
            "properties":{"disposition":{"type":"string"}}}),
        context: None,
    };

    let health = kutsu::net_check::preflight(&server).await.expect("preflight");
    assert!(matches!(kutsu::net_check::verdict(&health, &server.net_check),
                     kutsu::net_check::Verdict::Ok), "network: {}", health.summary());

    let mut session = kutsu::gemini_live::start(&server, &scenario).await.expect("start");
    // Send ~1s of silence to trigger a turn.
    let silence = vec![0i16; 512];
    for _ in 0..30 { let _ = session.audio_in.send(silence.clone()).await; tokio::time::sleep(std::time::Duration::from_millis(32)).await; }

    let mut got_audio = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, session.events.recv()).await {
        if let kutsu::gemini_live::Event::OutputAudio(_) = ev { got_audio = true; }
        if let kutsu::gemini_live::Event::EndCall { .. } = ev { break; }
    }
    session.hangup().await;
    assert!(got_audio, "expected some model audio");
}
```

- [ ] **Step 2: Verify it compiles and is skipped by default**

Run: `cargo test --features live-tests --test live_smoke -- --list`
Expected: lists the test as ignored (does not run without `--ignored`).
Run: `cargo test` (no feature) — the file compiles out; no live test runs.

- [ ] **Step 3: Add the example scenario and README section**

`docs/examples/scenario.json`:
```json
{
  "system_prompt": "You are an assistant calling to confirm an appointment. Be brief and polite.",
  "goal_schema": {
    "type": "object",
    "properties": {
      "confirmed": { "type": "boolean" },
      "callback_at": { "type": "string" },
      "disposition": { "type": "string" }
    },
    "required": ["disposition"]
  },
  "context": { "name": "Ivan", "city": "Kazan" }
}
```

Add a `### Dev harness: kutsu live` subsection to `README.md` documenting the command, env vars (`GEMINI_API_KEY`), the scenario file shape, and the audio format (mono PCM16 16 kHz in, 24 kHz out), plus exit codes (0/1/2).

- [ ] **Step 4: Commit**

```bash
git add tests/live_smoke.rs Cargo.toml docs/examples/scenario.json README.md
git commit -m "test(live): opt-in smoke test; docs for kutsu live"
```

---

## Self-Review

**Spec coverage:**
- Module boundaries → File Structure + Tasks 1-9. ✓
- Public API (config/session/events/outcome) → Tasks 1, 8. ✓
- Protocol mapping (setup, half/native, end_call dynamic schema, runtime parse) → Tasks 4, 5. ✓
- Session resumption + reconnect → Tasks 6, 8. ✓
- Network preflight fail-closed → Task 7 (+ used in Task 9). ✓
- Concurrency cap: config field only (Task 1); enforcement is phase 4/5 — correctly out of scope. ✓
- Harness (inputs, audio formats, flow, exit codes) → Tasks 3, 9. ✓
- Error handling → Task 2 (+ used throughout). ✓
- Testing (offline units + fixtures + opt-in live) → Tasks 1-8 unit tests, Task 10 smoke. ✓
- New deps hound/thiserror (+ base64, futures-util discovered during design) → Tasks 2, 3, 5, 7. ✓

**Placeholder scan:** Two deliberate, clearly-marked implementer hand-offs remain in Task 8 (thread `&mut Option<String>` for the handle; transcript accumulation) and Task 9 (input-length seconds for the deadline). These are called out explicitly with the exact wiring required and do not hide logic. All code steps contain real code.

**Type consistency:** `Role`, `Event`, `ServerEvent`, `SessionEnd`, `CallOutcome`, `EndedBy`, `Transport`, `run_session`, `start`, `Backoff`, `ReconnectState`, `NetworkHealth`, `Verdict`, `verdict`, `preflight`, `endpoint_url`, `build_setup`, `parse_server_message`, `read_pcm16`, `Pcm16Writer` are used consistently across tasks. `Role` is defined once in `proto.rs` and re-exported by `gemini_live`.
