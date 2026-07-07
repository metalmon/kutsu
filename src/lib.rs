//! Kutsu — outbound SIP calling MCP server.
//!
//! Places real phone calls over a generic SIP trunk and bridges the call
//! audio to Gemini Live (`BidiGenerateContent`) for a realtime conversational
//! agent, exposed as async MCP tools (`place_call` / `get_call_status` /
//! `get_call_transcript` / `end_call` / `list_calls`) so any MCP client can
//! drive cold calls without blocking on call duration.
//!
//! This is an early scaffold — most modules are stubs. See the project plan
//! for the intended build phases (SIP spike, Gemini Live client, audio
//! bridge, call engine, MCP layer).

pub mod bridge;
pub mod engine;
pub mod gemini_live;
pub mod mcp;
pub mod sip;
pub mod state;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_non_empty() {
        assert!(!version().is_empty());
    }
}
