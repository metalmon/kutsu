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
use std::time::Duration;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResponse, CallToolResult, CancelTaskParams, ContentBlock, CreateTaskResult,
    GetTaskParams, GetTaskResult, ServerCapabilities, ServerInfo, UpdateTaskParams,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::task_manager::{TaskExit, TaskManager, TaskOptions};
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
    /// SEP-2663 Tasks runtime, backing the `place_call` task branch and the
    /// `tasks/get|update|cancel` handler methods. Cheap to clone (Arc-backed).
    tasks: Arc<TaskManager>,
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

/// Map a [`CallState`] to its corresponding MCP [`TaskStatus`].
///
/// Used by the `place_call` task branch.
fn task_status_for(state: crate::state::CallState) -> rmcp::model::TaskStatus {
    use crate::state::CallState;
    use rmcp::model::TaskStatus;
    match state {
        CallState::Queued | CallState::Ringing | CallState::InProgress => TaskStatus::Working,
        CallState::Completed | CallState::HungUp => TaskStatus::Completed,
        CallState::Cancelled => TaskStatus::Cancelled,
        CallState::Failed => TaskStatus::Failed,
    }
}

/// Check if a [`CallState`] is terminal (not active).
///
/// Used by the `place_call` task branch.
fn is_terminal_state(state: crate::state::CallState) -> bool {
    use crate::state::CallState;
    !matches!(state, CallState::Queued | CallState::Ringing | CallState::InProgress)
}

/// Poll the call store until `call_id` reaches a terminal [`CallState`],
/// returning that state.
///
/// Pure watcher, factored out of the `place_call` task future so it can be
/// unit-tested directly. It contains no MCP-task glue — the framework
/// [`TaskContext`] plumbing (status messages, cooperative cancellation) wraps
/// this in `place_call_inner`.
async fn watch_call_to_terminal(engine: &Engine, call_id: &str) -> crate::state::CallState {
    loop {
        if let Some(rec) = engine.store().get(call_id) {
            if is_terminal_state(rec.state) {
                return rec.state;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tool_router]
impl KutsuServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine, tasks: Arc::new(TaskManager::new()), tool_router: Self::tool_router() }
    }

    #[tool(description = "Place an outbound phone call and bridge it to the AI agent. \
        For task-capable clients this runs as an MCP task (poll tasks/get for the \
        transcript+goal on completion). For plain clients it returns a call_id \
        immediately; poll get_call_status until terminal, then get_call_transcript.")]
    async fn place_call(
        &self,
        Parameters(a): Parameters<PlaceCallArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        // Branch on the *current client's* declared tasks capability — the same
        // signal the framework enforces at the call_tool boundary
        // (`handler/server.rs`). A direct method call (e.g. Task 6/7 tests) has
        // no RequestContext client capabilities and takes the immediate branch.
        let client_supports_tasks =
            context.client_capabilities().is_some_and(|c| c.supports_tasks());
        self.place_call_inner(a, client_supports_tasks).await
    }

    /// Core of `place_call`, factored out so the capability decision is an
    /// explicit `bool` and both branches are directly unit-testable without a
    /// live [`RequestContext`].
    async fn place_call_inner(
        &self,
        a: PlaceCallArgs,
        client_supports_tasks: bool,
    ) -> Result<CallToolResponse, ErrorData> {
        let scenario = ScenarioConfig {
            system_prompt: a.system_prompt,
            goal_schema: a.goal_schema,
            context: a.context,
        };
        let call_id = self.engine.place_call(a.to_number, scenario).await;

        // Plain clients: unchanged immediate `{call_id}` result (Task 6/7).
        if !client_supports_tasks {
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::json!({ "call_id": call_id }).to_string(),
            )])
            .into());
        }

        // Task-capable clients: spawn an MCP task that drives the call to a
        // terminal state, surfacing progress via status messages and honoring
        // cooperative cancellation (which hangs up the underlying call).
        // Size the task TTL from the engine's hard call deadline (+30 s for
        // teardown and a post-call observation window) so a long-but-valid call
        // is never swept to `failed`/aborted mid-flight by the SDK's TTL sweeper
        // — the default 5-min TTL is shorter than `max_call_secs` (default 600).
        let ttl_ms = self.engine.max_call_secs() * 1000 + 30_000;
        let options = TaskOptions::default().with_ttl_ms(Some(ttl_ms));
        let engine = self.engine.clone();
        let cid = call_id.clone();
        // If this session's `TaskManager` is dropped (e.g. an http session ends)
        // before the task finishes, the spawned future detaches rather than
        // aborts — but it's bounded, not a leak: it exits as soon as the call
        // reaches a terminal state, which is guaranteed within `max_call_secs`.
        let task = self.tasks.spawn(options, move |ctx| {
            Box::pin(async move {
                let terminal = loop {
                    // Publish the current human-readable status before waiting.
                    if let Some(rec) = engine.store().get(&cid) {
                        let status = task_status_for(rec.state);
                        let msg = match engine.store().queued_position(&cid) {
                            Some(pos) => format!("call {cid}: {status:?} (queue position {pos})"),
                            None => format!("call {cid}: {status:?}"),
                        };
                        ctx.set_status_message(msg);
                    }
                    tokio::select! {
                        // Reuse the pure watcher for terminal detection.
                        state = watch_call_to_terminal(&engine, &cid) => break state,
                        // Cooperative cancellation: hang up, settle as cancelled.
                        _ = ctx.cancelled() => {
                            engine.end_call(&cid);
                            return Err(TaskExit::Cancelled);
                        }
                        // Otherwise wake periodically to refresh the status.
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                };

                use crate::state::CallState;
                match terminal {
                    CallState::Completed | CallState::HungUp => {
                        let body = engine
                            .store()
                            .get(&cid)
                            .map(|r| serde_json::json!({ "transcript": r.transcript, "goal": r.goal }))
                            .unwrap_or_else(|| serde_json::json!({}));
                        Ok(CallToolResult::success(vec![ContentBlock::text(body.to_string())]))
                    }
                    CallState::Failed => {
                        let msg = engine
                            .store()
                            .get(&cid)
                            .and_then(|r| r.error)
                            .unwrap_or_else(|| "call failed".to_string());
                        Err(TaskExit::Error(ErrorData::internal_error(msg, None)))
                    }
                    CallState::Cancelled => Err(TaskExit::Cancelled),
                    // watch_call_to_terminal only returns terminal states.
                    other => Err(TaskExit::Error(ErrorData::internal_error(
                        format!("unexpected non-terminal state: {other:?}"),
                        None,
                    ))),
                }
            })
        });
        Ok(CallToolResponse::Task(CreateTaskResult::new(task)))
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
        ServerInfo::new(ServerCapabilities::builder().enable_tools().enable_tasks().build())
            .with_instructions(
                "Outbound calling: place_call → poll get_call_status → \
                 get_call_transcript; end_call to hang up. Task-capable clients \
                 may instead auto-poll place_call as an MCP task.",
            )
    }

    // SEP-2663 task methods. The framework has no `task_manager()` hook in
    // rmcp 3.1.2 — the default `get_task`/`update_task`/`cancel_task` return
    // `method_not_found`, so we override all three to delegate to our
    // [`TaskManager`]. The `tasks/*` capability gate in `handler/server.rs`
    // guards these before dispatch.
    async fn get_task(
        &self,
        request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, ErrorData> {
        self.tasks.get_task(&request.task_id).map(GetTaskResult::new)
    }

    async fn update_task(
        &self,
        request: UpdateTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.tasks.update_task(&request.task_id, request.input_responses)
    }

    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.tasks.cancel_task(&request.task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Model, NetCheckConfig, ServerConfig, SipConfig};

    /// Build a valid engine for tests, bound to loopback so `SipTransport::new`
    /// binds offline (no real trunk) — mirrors `engine::tests::test_configs`.
    async fn test_engine(cap: usize) -> Engine {
        let server = ServerConfig {
            api_key: "k".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: cap,
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
        Engine::new(Arc::new(server), &sip).await.unwrap()
    }

    /// Destructure the first content block's text, as the MCP tools always
    /// return a single `ContentBlock::Text` JSON body.
    fn first_text(r: &CallToolResult) -> String {
        match r.content.first().unwrap() {
            ContentBlock::Text(t) => t.text.clone(),
            _ => panic!("expected text content"),
        }
    }

    /// Unwrap the immediate (non-task) branch of a `place_call` response.
    fn complete(r: CallToolResponse) -> CallToolResult {
        match r {
            CallToolResponse::Complete(x) => x,
            _ => panic!("expected an immediate Complete response"),
        }
    }

    fn args(to: &str) -> PlaceCallArgs {
        PlaceCallArgs {
            to_number: to.into(),
            system_prompt: "hi".into(),
            goal_schema: serde_json::json!({ "type": "object" }),
            context: None,
        }
    }

    #[tokio::test]
    async fn lists_exactly_four_tools() {
        let engine = test_engine(1).await;
        let srv = KutsuServer::new(Arc::new(engine));

        let names: Vec<_> = srv.tool_router.list_all().into_iter().map(|t| t.name.to_string()).collect();
        assert_eq!(names.len(), 4);
        for n in ["place_call", "get_call_status", "get_call_transcript", "end_call"] {
            assert!(names.contains(&n.to_string()), "missing {n}");
        }
    }

    #[tokio::test]
    async fn place_then_status_and_transcript_roundtrip() {
        let engine = Arc::new(test_engine(1).await);
        let srv = KutsuServer::new(engine.clone());

        // Direct call: no client capability → immediate `{call_id}` branch.
        let out = complete(srv.place_call_inner(args("600"), false).await.unwrap());
        let text = first_text(&out);
        let call_id =
            serde_json::from_str::<serde_json::Value>(&text).unwrap()["call_id"].as_str().unwrap().to_string();
        assert!(!call_id.is_empty());

        let status =
            srv.get_call_status(Parameters(CallIdArgs { call_id: call_id.clone() })).await.unwrap();
        let status_json = serde_json::from_str::<serde_json::Value>(&first_text(&status)).unwrap();
        assert_eq!(status_json["call_id"], call_id);
        assert!(status_json.get("state").is_some());

        let tr = srv.get_call_transcript(Parameters(CallIdArgs { call_id: call_id.clone() })).await.unwrap();
        let tr_json = serde_json::from_str::<serde_json::Value>(&first_text(&tr)).unwrap();
        assert_eq!(tr_json["call_id"], call_id);
        assert!(tr_json.get("transcript").is_some());

        drop(srv);
        Arc::into_inner(engine).expect("only strong ref left").shutdown().await;
    }

    #[tokio::test]
    async fn unknown_call_id_is_invalid_params() {
        let engine = Arc::new(test_engine(1).await);
        let srv = KutsuServer::new(engine.clone());

        for r in [
            srv.get_call_status(Parameters(CallIdArgs { call_id: "x".into() })).await,
            srv.get_call_transcript(Parameters(CallIdArgs { call_id: "x".into() })).await,
            srv.end_call(Parameters(CallIdArgs { call_id: "x".into() })).await,
        ] {
            let err = r.unwrap_err();
            assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        }

        drop(srv);
        Arc::into_inner(engine).expect("only strong ref left").shutdown().await;
    }

    #[test]
    fn task_status_mapping() {
        use rmcp::model::TaskStatus::*;
        use crate::state::CallState as S;
        assert!(matches!(task_status_for(S::Queued), Working));
        assert!(matches!(task_status_for(S::Ringing), Working));
        assert!(matches!(task_status_for(S::InProgress), Working));
        assert!(matches!(task_status_for(S::Completed), Completed));
        assert!(matches!(task_status_for(S::HungUp), Completed));
        assert!(matches!(task_status_for(S::Cancelled), Cancelled));
        assert!(matches!(task_status_for(S::Failed), Failed));
        assert!(!is_terminal_state(S::Queued));
        assert!(is_terminal_state(S::Completed));
        assert!(is_terminal_state(S::Cancelled));
    }

    /// The pure watcher must return the terminal state a call settles into.
    /// cap-0 parks the call in `Queued`; a delayed `end_call` flips it to
    /// `Cancelled`, and the watcher must resolve with `Cancelled`.
    #[tokio::test]
    async fn watch_call_to_terminal_returns_cancelled() {
        use crate::state::CallState;
        let engine = Arc::new(test_engine(0).await);
        let call_id = engine
            .place_call("600".into(), ScenarioConfig {
                system_prompt: "hi".into(),
                goal_schema: serde_json::json!({ "type": "object" }),
                context: None,
            })
            .await;
        assert_eq!(engine.store().get(&call_id).unwrap().state, CallState::Queued);

        // Cancel shortly after the watcher begins polling.
        let e2 = engine.clone();
        let id2 = call_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            e2.end_call(&id2);
        });

        let terminal = watch_call_to_terminal(&engine, &call_id).await;
        assert!(matches!(terminal, CallState::Cancelled), "got {terminal:?}");
    }

    /// A task-capable branch returns a `CallToolResponse::Task` handle rather
    /// than the immediate `{call_id}` result.
    #[tokio::test]
    async fn place_call_task_branch_returns_task_handle() {
        // cap-0 parks the call in Queued so no real INVITE is attempted.
        let engine = Arc::new(test_engine(0).await);
        let srv = KutsuServer::new(engine);

        let resp = srv.place_call_inner(args("600"), true).await.unwrap();
        let task = match resp {
            CallToolResponse::Task(create) => create.task,
            other => panic!("expected a Task response, got {other:?}"),
        };
        // TTL is derived from the engine's hard call deadline (600 s here) plus
        // a 30 s teardown/observation margin, not the SDK's 5-min default — so a
        // long-but-valid call is never TTL-swept mid-flight.
        assert_eq!(task.ttl_ms, Some(600 * 1000 + 30_000));

        // Abort the background watcher task so it stops touching the engine.
        srv.tasks.shutdown();
    }
}
