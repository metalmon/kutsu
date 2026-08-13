//! Gemini Live `BidiGenerateContent` protocol (pure serialize/parse; no IO).

use base64::Engine as _;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn server(model: Model) -> ServerConfig {
        ServerConfig {
            api_key: "KEY".into(), proxy: None, model, voice: "Autonoe".into(),
            language: "ru-RU".into(), net_check: NetCheckConfig::default(),
            max_concurrent_channels: 3, greet_after_silence_ms: 4000,
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
