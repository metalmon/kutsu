//! Call-content prompt assembly for a Gemini Live session.
//!
//! The Gemini Live *wire* protocol (setup serialization, server-message parsing,
//! endpoint URL, model ids) now lives in the `gemini-live` crate. What stays
//! here is call *content*: assembling the system instruction from the scenario
//! plus the standard call-protocol layer (voice-gender + closing + language
//! directives). kutsu hands the finished text to the crate via
//! `SetupConfig::system_instruction`.

use crate::config::{Gender, ScenarioConfig, ServerConfig};

/// Standard call-closing directive appended to every system prompt so the model
/// closes deterministically via the tool. Without it the model tends to speak a
/// goodbye WITHOUT calling `end_call`; the session stays open and it emits a
/// second closing turn (the double-goodbye). Ported from the proven voice-cloud
/// client's END_INSTRUCTION_TOOL. English by repo convention — the goodbye
/// itself is spoken in the scenario's language.
const CLOSING_INSTRUCTION: &str = "\n\n# Ending the call\n\
    When the conversation is finished — you have said your goodbyes, agreed on a \
    next step, or the other party firmly refuses or is rude — say ONE short, \
    natural goodbye and, in the SAME turn, call the `end_call` tool. Say the \
    goodbye only once: the line is played in full and then the call is hung up, \
    so never add a second goodbye and do not keep talking after it.";

/// Grammatical-gender directive matched to the configured voice, so the agent
/// doesn't (e.g.) say a masculine «я понял» in a female voice. English by repo
/// convention; the Russian examples anchor the gendered forms that matter.
fn gender_instruction(gender: Gender) -> &'static str {
    match gender {
        Gender::Female =>
            "\n\n# Your voice\nYou speak with a FEMALE voice. Always refer to \
             yourself using feminine grammatical forms — verbs, adjectives, \
             participles (in Russian: «я поняла», «была рада», «сама», «готова»). \
             Never use masculine self-reference.",
        Gender::Male =>
            "\n\n# Your voice\nYou speak with a MALE voice. Always refer to \
             yourself using masculine grammatical forms — verbs, adjectives, \
             participles (in Russian: «я понял», «был рад», «сам», «готов»). \
             Never use feminine self-reference.",
        Gender::Neutral => "",
    }
}

/// Pin the spoken language via the system prompt. Essential on native-audio,
/// which ignores the structured `languageCode` and otherwise picks a language
/// on its own (it drifts to English from an English scaffold). Harmless on
/// half-cascade, where it just reinforces the `languageCode`. Keeps
/// `KUTSU_LANGUAGE` authoritative on every model. `language` is a BCP-47 tag
/// (e.g. `ru-RU`), which the model understands directly.
fn language_instruction(language: &str) -> String {
    format!(
        "\n\n# Language\nSpeak ONLY in the language with BCP-47 code `{language}`, always — \
         from the very first word and in every single reply. You may understand the other party \
         when they use another language, but you MUST always answer in `{language}`, pronouncing \
         it cleanly and naturally like a native speaker, with no foreign accent."
    )
}

/// Assemble the per-call scenario layer: prompt + optional contact context,
/// followed by the standard call-protocol layer (voice-gender + closing
/// directives). The language directive is appended separately by
/// [`assemble_system_instruction`].
fn build_system_prompt(scenario: &ScenarioConfig, gender: Gender) -> String {
    let mut s = scenario.system_prompt.clone();
    if let Some(ctx) = &scenario.context {
        s.push_str("\n\n# Contact context\n");
        s.push_str(&ctx.to_string());
    }
    s.push_str(gender_instruction(gender));
    s.push_str(CLOSING_INSTRUCTION);
    s
}

/// Build the full system instruction for a session: the scenario + call-protocol
/// layer, plus the language directive pinned from `server.language`. This is the
/// text handed to the crate as `SetupConfig::system_instruction`.
pub fn assemble_system_instruction(server: &ServerConfig, scenario: &ScenarioConfig) -> String {
    build_system_prompt(scenario, server.voice_gender) + &language_instruction(&server.language)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn server() -> ServerConfig {
        ServerConfig {
            api_key: "KEY".into(), proxy: None, model: Model::NativeAudio, voice: "Autonoe".into(),
            voice_gender: Gender::Female,
            language: "en-US".into(), net_check: NetCheckConfig::default(),
            max_concurrent_channels: 3, greet_after_silence_ms: 4000,
            transcript_dir: None, dump_uplink_dir: None, dump_downlink_dir: None, max_call_secs: 600,
            quality: QualityConfig::default(), retry: RetryConfig::default(),
            vad: VadConfig::default(), resume_cue: RESUME_CUE.into(),
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
    fn system_prompt_appends_closing_instruction() {
        let p = build_system_prompt(&scenario(), Gender::Female);
        assert!(p.starts_with("Be nice."));
        assert!(p.contains("end_call"), "closing must reference the tool");
        assert!(p.contains("SAME turn"), "closing must couple the goodbye to the tool");
        assert!(p.to_lowercase().contains("goodbye"));
    }

    #[test]
    fn system_prompt_injects_voice_gender() {
        let f = build_system_prompt(&scenario(), Gender::Female);
        assert!(f.contains("FEMALE voice"));
        assert!(f.contains("feminine"));
        let m = build_system_prompt(&scenario(), Gender::Male);
        assert!(m.contains("MALE voice"));
        assert!(m.contains("masculine"));
        // Neutral adds no voice-gender block.
        let n = build_system_prompt(&scenario(), Gender::Neutral);
        assert!(!n.contains("# Your voice"));
    }

    #[test]
    fn assemble_system_instruction_pins_language() {
        // The language directive (pinned from server.language) is what keeps a
        // native-audio model — which ignores the structured languageCode — from
        // drifting. It must be present and carry the configured tag.
        let sys = assemble_system_instruction(&server(), &scenario());
        assert!(sys.contains("# Language"), "system instruction must carry a language section");
        assert!(sys.contains("en-US"), "language directive must pin server.language (en-US here)");
        // The scenario + closing layer is still present ahead of it.
        assert!(sys.starts_with("Be nice."));
        assert!(sys.contains("end_call"));
    }
}
