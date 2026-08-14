//! MCP server layer (`rmcp` 3.1) — thin wrapper over the call engine.
//!
//! Four tools, each a thin wrapper over [`crate::engine::Engine`] /
//! [`crate::state`]:
//! - `place_call(to_number, system_prompt, goal_schema, context)` — spawns
//!   the call in the background and returns a `call_id` immediately (async
//!   execution model, chosen to avoid MCP client tool-call timeouts on calls
//!   that run minutes).
//! - `get_call_status(call_id)`
//! - `get_call_transcript(call_id)`
//! - `end_call(call_id)`
//!
//! Enumerating all calls is deliberately *not* an MCP tool: the client that
//! placed a call already holds its `call_id` and polls that one call. A global
//! listing is a server-side operational concern (admin/HTTP), not a capability
//! the model needs to drive a call — mirroring the MCP Tasks extension, which
//! dropped `tasks/list` for the same reason.

use std::sync::Arc;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::config::ScenarioConfig;
use crate::engine::Engine;

/// MCP handler wrapping the call [`Engine`]. Cheap to clone (both fields are
/// `Arc`-backed).
#[derive(Clone)]
pub struct KutsuServer {
    engine: Arc<Engine>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PlaceCallArgs {
    /// Callee number in E.164, e.g. "+79991234567".
    to_number: String,
    /// System prompt / persona that drives the agent for this call.
    system_prompt: String,
    /// JSON Schema the agent fills and submits via end_call (the call goal).
    goal_schema: serde_json::Value,
    /// Optional lead/context object merged into the prompt.
    #[serde(default)]
    context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CallIdArgs {
    /// The call_id returned by place_call.
    call_id: String,
}

fn unknown_call(id: &str) -> ErrorData {
    ErrorData::invalid_params(format!("unknown call_id: {id}"), None)
}

#[tool_router]
impl KutsuServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine, tool_router: Self::tool_router() }
    }

    #[tool(description = "Place an outbound phone call and bridge it to the AI agent. \
        Returns a call_id immediately; the call runs in the background. Poll \
        get_call_status until the state is terminal, then read get_call_transcript.")]
    async fn place_call(
        &self,
        Parameters(a): Parameters<PlaceCallArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let scenario = ScenarioConfig {
            system_prompt: a.system_prompt,
            goal_schema: a.goal_schema,
            context: a.context,
        };
        let call_id = self.engine.place_call(a.to_number, scenario).await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "call_id": call_id }).to_string(),
        )]))
    }

    #[tool(description = "Get the current state of a call by call_id (lightweight; no transcript).")]
    async fn get_call_status(
        &self,
        Parameters(a): Parameters<CallIdArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rec = self.engine.store().get(&a.call_id).ok_or_else(|| unknown_call(&a.call_id))?;
        let pos = self.engine.store().queued_position(&a.call_id);
        let body = serde_json::json!({
            "call_id": rec.call_id, "state": rec.state, "number": rec.number,
            "started_ms": rec.started_ms, "ended_ms": rec.ended_ms,
            "error": rec.error, "queued_position": pos,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(body.to_string())]))
    }

    #[tool(description = "Get the full transcript and filled goal of a call by call_id.")]
    async fn get_call_transcript(
        &self,
        Parameters(a): Parameters<CallIdArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rec = self.engine.store().get(&a.call_id).ok_or_else(|| unknown_call(&a.call_id))?;
        let body = serde_json::json!({
            "call_id": rec.call_id, "state": rec.state,
            "transcript": rec.transcript, "goal": rec.goal,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(body.to_string())]))
    }

    #[tool(description = "End (hang up / cancel) a running or queued call by call_id.")]
    async fn end_call(
        &self,
        Parameters(a): Parameters<CallIdArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        if self.engine.store().get(&a.call_id).is_none() {
            return Err(unknown_call(&a.call_id));
        }
        let signalled = self.engine.end_call(&a.call_id);
        let state = self.engine.store().get(&a.call_id).map(|r| r.state);
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "call_id": a.call_id, "signalled": signalled, "state": state }).to_string(),
        )]))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for KutsuServer {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` (= `InitializeResult`) is `#[non_exhaustive]` in rmcp
        // 3.1.2, so struct-literal construction (even with
        // `..Default::default()`) is rejected outside the defining crate;
        // use its builder methods instead.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Outbound calling: place_call → poll get_call_status → \
             get_call_transcript; end_call to hang up.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Model, NetCheckConfig, ServerConfig, SipConfig};

    #[tokio::test]
    async fn lists_exactly_four_tools() {
        let server = ServerConfig {
            api_key: "k".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: 1,
            greet_after_silence_ms: 4000,
            transcript_dir: None,
            max_call_secs: 600,
        };
        let sip = SipConfig {
            server: "127.0.0.1:5060".into(),
            username: "t".into(),
            password: "t".into(),
            from_user: None,
            local_ip: Some("127.0.0.1".parse().unwrap()),
            register: false,
            transport: Default::default(),
        };
        let engine = Engine::new(Arc::new(server), &sip).await.unwrap();
        let srv = KutsuServer::new(Arc::new(engine));

        let names: Vec<_> = srv.tool_router.list_all().into_iter().map(|t| t.name.to_string()).collect();
        assert_eq!(names.len(), 4);
        for n in ["place_call", "get_call_status", "get_call_transcript", "end_call"] {
            assert!(names.contains(&n.to_string()), "missing {n}");
        }
    }
}
