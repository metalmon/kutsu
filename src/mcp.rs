//! MCP server layer (`rmcp` 3.1) — thin wrapper over the call engine.
//!
//! Four tools, each a thin wrapper over [`crate::engine::Engine`] /
//! [`crate::state`]:
//! - `place_call(to_number, goal_schema, context?, prompt_override?)` — spawns
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
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
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
    /// JSON Schema the agent fills and submits via end_call. Carries the call's
    /// objective (via its field descriptions) as well as the output shape; it is
    /// also injected into the prompt so the model knows what to gather.
    goal_schema: serde_json::Value,
    /// Optional lead/context object merged into the prompt.
    #[serde(default)]
    context: Option<serde_json::Value>,
    /// Optional per-call persona override. When absent, the deployment's base
    /// system prompt (`[server.prompts]` / `KUTSU_SYSTEM_PROMPT`) is used.
    #[serde(default)]
    prompt_override: Option<String>,
    /// Optional UTC epoch ms to place the call at. Past/absent = immediate.
    #[serde(default)]
    schedule_at: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CallIdArgs {
    /// The call_id returned by place_call.
    call_id: String,
}

fn unknown_call(id: &str) -> ErrorData {
    ErrorData::invalid_params(format!("unknown call_id: {id}"), None)
}

/// Human-readable label for a call record, used in a task's progress
/// message. (The task's actual MCP `TaskStatus` is derived by the framework
/// from the task future's terminal `Ok`/`Err`, not from this label.) While
/// live this is the lifecycle state; once `Ended` it's the resolved
/// disposition (e.g. "voicemail", "busy"), which is far more informative
/// than a bare "ended".
fn call_status_label(rec: &crate::state::CallRecord) -> String {
    use crate::state::CallState;
    match rec.state {
        CallState::Queued => "queued".into(),
        CallState::Ringing => "ringing".into(),
        CallState::InProgress => "in progress".into(),
        CallState::Ended => rec
            .disposition
            .and_then(|d| serde_json::to_value(d).ok())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "ended".into()),
    }
}

/// Check if a [`CallState`] is terminal (not active).
///
/// Used by the `place_call` task branch.
fn is_terminal_state(state: crate::state::CallState) -> bool {
    matches!(state, crate::state::CallState::Ended)
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
        if let Some(rec) = engine.store().get(call_id)
            && is_terminal_state(rec.state)
        {
            return rec.state;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Map a finished call's record to the MCP task result: `Ok` with the collected
/// data for answered calls (`Completed`, or an answering-machine disposition
/// which still carries a transcript/goal — the "always return a result"
/// thesis), `Err` for cancellation and technical/dial failures. Pure, so the
/// disposition → task-result mapping is unit-testable without the task plumbing.
fn task_result_for(rec: Option<crate::state::CallRecord>) -> Result<CallToolResult, TaskExit> {
    use crate::state::Disposition;
    match rec.as_ref().and_then(|r| r.disposition) {
        Some(Disposition::Completed) => {
            let server_time_unix = crate::engine::now_ms();
            let body = rec
                .map(|r| {
                    serde_json::json!({
                        "transcript": r.transcript, "goal": r.goal,
                        "server_time_unix": server_time_unix,
                    })
                })
                .unwrap_or_else(|| serde_json::json!({ "server_time_unix": server_time_unix }));
            Ok(CallToolResult::success(vec![ContentBlock::text(
                body.to_string(),
            )]))
        }
        d @ Some(
            Disposition::Voicemail
            | Disposition::Announcement
            | Disposition::Ivr
            | Disposition::Hold,
        ) => {
            let server_time_unix = crate::engine::now_ms();
            let body = rec
                .map(|r| {
                    serde_json::json!({
                        "disposition": d, "transcript": r.transcript,
                        "goal": r.goal, "server_time_unix": server_time_unix,
                    })
                })
                .unwrap_or_else(
                    || serde_json::json!({ "disposition": d, "server_time_unix": server_time_unix }),
                );
            Ok(CallToolResult::success(vec![ContentBlock::text(
                body.to_string(),
            )]))
        }
        Some(Disposition::Cancelled) => Err(TaskExit::Cancelled),
        // Everything else is a technical/dial failure (Failed, Busy, NoAnswer,
        // Rejected, NotFound, Unavailable).
        Some(d) => {
            let msg = rec
                .as_ref()
                .and_then(|r| r.error.clone())
                .unwrap_or_else(|| {
                    let name = serde_json::to_value(d)
                        .ok()
                        .and_then(|v| v.as_str().map(String::from))
                        .unwrap_or_else(|| "failed".to_string());
                    format!("call ended: {name}")
                });
            Err(TaskExit::Error(ErrorData::internal_error(msg, None)))
        }
        // watch_call_to_terminal only returns once Ended, which always carries a
        // disposition.
        None => Err(TaskExit::Error(ErrorData::internal_error(
            "call ended with no disposition".to_string(),
            None,
        ))),
    }
}

#[tool_router]
impl KutsuServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            tasks: Arc::new(TaskManager::new()),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Place an outbound phone call and bridge it to the AI agent. \
        For task-capable clients this runs as an MCP task (poll tasks/get for the \
        transcript+goal on completion). For plain clients it returns a call_id \
        immediately; poll get_call_status until terminal, then get_call_transcript."
    )]
    async fn place_call(
        &self,
        Parameters(a): Parameters<PlaceCallArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        // Branch on the *current client's* declared tasks capability — the same
        // signal the framework enforces at the call_tool boundary
        // (`handler/server.rs`). A direct method call (e.g. Task 6/7 tests) has
        // no RequestContext client capabilities and takes the immediate branch.
        let client_supports_tasks = context
            .client_capabilities()
            .is_some_and(|c| c.supports_tasks());
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
        if a.to_number.trim().is_empty() {
            return Err(ErrorData::invalid_params(
                "to_number must not be empty".to_string(),
                None,
            ));
        }
        let scenario = ScenarioConfig {
            goal_schema: a.goal_schema,
            context: a.context,
            prompt_override: a.prompt_override,
        };
        // A `schedule_at` in the past (or absent) means "now" — never an
        // error, just immediate eligibility.
        let now = crate::engine::now_ms();
        let eligible = a.schedule_at.filter(|t| *t > now).unwrap_or(now);
        let scheduled = eligible > now;
        let call_id = self
            .engine
            .place_call_at(a.to_number, scenario, eligible, 1)
            .await;

        // Plain clients, and any scheduled call (even for task-capable
        // clients): immediate `{call_id, server_time_unix}` result. A
        // scheduled call must never enter the task/poll flavor below — that
        // task would sit idle (no dispatch, no status changes) until
        // `eligible_at_ms`, which defeats the point of an MCP task.
        if scheduled || !client_supports_tasks {
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::json!({ "call_id": call_id, "server_time_unix": now }).to_string(),
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
        // Suggest a poll cadence adapted to queue depth at creation (rmcp fixes
        // pollIntervalMs at spawn; no runtime setter). Queued-behind-busy → longer.
        let poll_ms = self.engine.suggested_poll_interval_ms(&call_id);
        let options = TaskOptions::default()
            .with_ttl_ms(Some(ttl_ms))
            .with_poll_interval_ms(poll_ms);
        let engine = self.engine.clone();
        let cid = call_id.clone();
        // If this session's `TaskManager` is dropped (e.g. an http session ends)
        // before the task finishes, the spawned future detaches rather than
        // aborts — but it's bounded, not a leak: it exits as soon as the call
        // reaches a terminal state, which is guaranteed within `max_call_secs`.
        let task = self.tasks.spawn(options, move |ctx| {
            Box::pin(async move {
                loop {
                    // Publish the current human-readable status before waiting.
                    if let Some(rec) = engine.store().get(&cid) {
                        let label = call_status_label(&rec);
                        let msg = match engine.queued_position(&cid) {
                            Some(pos) => format!("call {cid}: {label} (queue position {pos})"),
                            None => format!("call {cid}: {label}"),
                        };
                        ctx.set_status_message(msg);
                    }
                    tokio::select! {
                        // Reuse the pure watcher for terminal detection. The
                        // only terminal CallState is Ended, so its resolved
                        // Disposition (not the lifecycle state) drives the
                        // task's Ok/Err outcome below.
                        _ = watch_call_to_terminal(&engine, &cid) => break,
                        // Cooperative cancellation: hang up, settle as cancelled.
                        _ = ctx.cancelled() => {
                            engine.end_call(&cid);
                            return Err(TaskExit::Cancelled);
                        }
                        // Otherwise wake periodically to refresh the status.
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                }

                task_result_for(engine.store().get(&cid))
            })
        });
        Ok(CallToolResponse::Task(CreateTaskResult::new(task)))
    }

    #[tool(description = "Get a call's current state, disposition, and filled goal by call_id.")]
    async fn get_call_status(
        &self,
        Parameters(a): Parameters<CallIdArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rec = self
            .engine
            .store()
            .get(&a.call_id)
            .ok_or_else(|| unknown_call(&a.call_id))?;
        let pos = self.engine.queued_position(&a.call_id);
        let body = serde_json::json!({
            "call_id": rec.call_id, "state": rec.state, "number": rec.number,
            "started_ms": rec.started_ms, "ended_ms": rec.ended_ms,
            "error": rec.error, "queued_position": pos, "quality": rec.quality,
            "disposition": rec.disposition,
            "goal": rec.goal,
            "attempt": rec.attempt,
            "server_time_unix": crate::engine::now_ms(),
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            body.to_string(),
        )]))
    }

    #[tool(description = "Get the full transcript and filled goal of a call by call_id.")]
    async fn get_call_transcript(
        &self,
        Parameters(a): Parameters<CallIdArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rec = self
            .engine
            .store()
            .get(&a.call_id)
            .ok_or_else(|| unknown_call(&a.call_id))?;
        let body = serde_json::json!({
            "call_id": rec.call_id, "state": rec.state, "disposition": rec.disposition,
            "transcript": rec.transcript, "goal": rec.goal, "quality": rec.quality,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            body.to_string(),
        )]))
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
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "call_id": a.call_id, "signalled": signalled }).to_string(),
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
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tasks()
                .build(),
        )
        .with_instructions(
            "Outbound calling to an AI agent. Tools: place_call (start a call), \
             get_call_status (state, disposition, filled goal), get_call_transcript \
             (full transcript), end_call (hang up). Task-capable clients may run \
             place_call as an MCP task.",
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
        self.tasks
            .get_task(&request.task_id)
            .map(GetTaskResult::new)
    }

    async fn update_task(
        &self,
        request: UpdateTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.tasks
            .update_task(&request.task_id, request.input_responses)
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
    use crate::config::{Model, NetCheckConfig, ServerConfig, SipConfig, VadConfig};

    /// Build a valid engine for tests, bound to loopback so `SipTransport::new`
    /// binds offline (no real trunk) — mirrors `engine::tests::test_configs`.
    async fn test_engine(cap: usize) -> Engine {
        let server = ServerConfig {
            api_key: "k".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            voice_gender: crate::config::Gender::Female,
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: cap,
            greet_after_silence_ms: 4000,
            transcript_dir: None,
            dump_uplink_dir: None,
            dump_downlink_dir: None,
            max_call_secs: 600,
            mcp_poll_interval_ms: 5000,
            mcp_poll_interval_max_ms: 30000,
            quality: crate::config::QualityConfig::default(),
            retry: crate::config::RetryConfig::default(),
            vad: VadConfig::default(),
            agc: crate::config::AgcConfig::default(),
            prompts: crate::config::PromptsConfig::default(),
        };
        let sip = SipConfig {
            server: "127.0.0.1:5060".into(),
            username: "t".into(),
            password: "t".into(),
            from_user: None,
            local_ip: Some("127.0.0.1".parse().unwrap()),
            local_port: None,
            sip_domain: None,
            register: false,
            register_expiry_secs: None,
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
            goal_schema: serde_json::json!({ "type": "object" }),
            context: None,
            prompt_override: None,
            schedule_at: None,
        }
    }

    #[tokio::test]
    async fn lists_exactly_four_tools() {
        let engine = test_engine(1).await;
        let srv = KutsuServer::new(Arc::new(engine));

        let names: Vec<_> = srv
            .tool_router
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();
        assert_eq!(names.len(), 4);
        for n in [
            "place_call",
            "get_call_status",
            "get_call_transcript",
            "end_call",
        ] {
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
        let call_id = serde_json::from_str::<serde_json::Value>(&text).unwrap()["call_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!call_id.is_empty());

        let status = srv
            .get_call_status(Parameters(CallIdArgs {
                call_id: call_id.clone(),
            }))
            .await
            .unwrap();
        let status_json = serde_json::from_str::<serde_json::Value>(&first_text(&status)).unwrap();
        assert_eq!(status_json["call_id"], call_id);
        assert!(status_json.get("state").is_some());
        assert!(status_json.get("goal").is_some());
        assert_eq!(status_json["attempt"], 1);

        let tr = srv
            .get_call_transcript(Parameters(CallIdArgs {
                call_id: call_id.clone(),
            }))
            .await
            .unwrap();
        let tr_json = serde_json::from_str::<serde_json::Value>(&first_text(&tr)).unwrap();
        assert_eq!(tr_json["call_id"], call_id);
        assert!(tr_json.get("transcript").is_some());
        assert!(tr_json.get("disposition").is_some());

        drop(srv);
        Arc::into_inner(engine)
            .expect("only strong ref left")
            .shutdown()
            .await;
    }

    #[tokio::test]
    async fn place_call_empty_number_is_invalid_params() {
        let engine = Arc::new(test_engine(1).await);
        let srv = KutsuServer::new(engine.clone());
        let err = srv.place_call_inner(args("   "), false).await.unwrap_err();
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        drop(srv);
        Arc::into_inner(engine)
            .expect("only strong ref left")
            .shutdown()
            .await;
    }

    #[tokio::test]
    async fn unknown_call_id_is_invalid_params() {
        let engine = Arc::new(test_engine(1).await);
        let srv = KutsuServer::new(engine.clone());

        for r in [
            srv.get_call_status(Parameters(CallIdArgs {
                call_id: "x".into(),
            }))
            .await,
            srv.get_call_transcript(Parameters(CallIdArgs {
                call_id: "x".into(),
            }))
            .await,
            srv.end_call(Parameters(CallIdArgs {
                call_id: "x".into(),
            }))
            .await,
        ] {
            let err = r.unwrap_err();
            assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        }

        drop(srv);
        Arc::into_inner(engine)
            .expect("only strong ref left")
            .shutdown()
            .await;
    }

    #[test]
    fn call_status_label_and_terminal() {
        use crate::state::{CallState as S, Disposition};

        fn rec(state: S, disposition: Option<Disposition>) -> crate::state::CallRecord {
            crate::state::CallRecord {
                call_id: "c1".into(),
                number: "600".into(),
                state,
                transcript: vec![],
                affect: vec![],
                goal: None,
                error: None,
                started_ms: 0,
                ended_ms: None,
                quality: Default::default(),
                disposition,
                attempt: 1,
            }
        }

        assert_eq!(call_status_label(&rec(S::Queued, None)), "queued");
        assert_eq!(call_status_label(&rec(S::Ringing, None)), "ringing");
        assert_eq!(call_status_label(&rec(S::InProgress, None)), "in progress");
        assert_eq!(
            call_status_label(&rec(S::Ended, Some(Disposition::Completed))),
            "completed"
        );
        assert_eq!(
            call_status_label(&rec(S::Ended, Some(Disposition::Cancelled))),
            "cancelled"
        );
        // Ended with no disposition (shouldn't happen via finalize, but the
        // label must degrade gracefully rather than panic).
        assert_eq!(call_status_label(&rec(S::Ended, None)), "ended");

        assert!(!is_terminal_state(S::Queued));
        assert!(!is_terminal_state(S::Ringing));
        assert!(!is_terminal_state(S::InProgress));
        assert!(is_terminal_state(S::Ended));
    }

    #[test]
    fn task_result_for_maps_each_disposition() {
        use crate::state::{CallRecord, CallState, Disposition};

        fn rec(disposition: Option<Disposition>, error: Option<String>) -> CallRecord {
            CallRecord {
                call_id: "c1".into(),
                number: "600".into(),
                state: CallState::Ended,
                transcript: vec![],
                affect: vec![],
                goal: Some(serde_json::json!({ "x": 1 })),
                error,
                started_ms: 0,
                ended_ms: Some(1),
                quality: Default::default(),
                disposition,
                attempt: 1,
            }
        }

        // Completed → Ok carrying transcript + goal, no disposition field.
        let t =
            first_text(&task_result_for(Some(rec(Some(Disposition::Completed), None))).unwrap());
        assert!(t.contains("\"goal\"") && t.contains("\"transcript\""));
        assert!(!t.contains("\"disposition\""));

        // Answering-machine outcomes → Ok, carrying the disposition + goal.
        for (d, name) in [
            (Disposition::Voicemail, "voicemail"),
            (Disposition::Announcement, "announcement"),
            (Disposition::Ivr, "ivr"),
            (Disposition::Hold, "hold"),
        ] {
            let t = first_text(&task_result_for(Some(rec(Some(d), None))).unwrap());
            assert!(
                t.contains(&format!("\"disposition\":\"{name}\"")),
                "got: {t}"
            );
            assert!(t.contains("\"goal\""));
        }

        // Cancelled → Err(Cancelled).
        assert!(matches!(
            task_result_for(Some(rec(Some(Disposition::Cancelled), None))),
            Err(TaskExit::Cancelled)
        ));

        // Dial/technical failures → Err(Error); the disposition names the reason
        // when no explicit error string is present.
        for (d, name) in [
            (Disposition::Busy, "busy"),
            (Disposition::NoAnswer, "no_answer"),
            (Disposition::NotFound, "not_found"),
            (Disposition::Unavailable, "unavailable"),
            (Disposition::Rejected, "rejected"),
            (Disposition::Failed, "failed"),
        ] {
            match task_result_for(Some(rec(Some(d), None))) {
                Err(TaskExit::Error(e)) => {
                    assert!(
                        e.message.contains(name),
                        "disposition {name}: {}",
                        e.message
                    )
                }
                other => panic!("expected Err(Error) for {name}, got {other:?}"),
            }
        }

        // An explicit error string is preferred over the disposition name.
        match task_result_for(Some(rec(Some(Disposition::Failed), Some("boom".into())))) {
            Err(TaskExit::Error(e)) => assert!(e.message.contains("boom")),
            other => panic!("expected Err(Error), got {other:?}"),
        }

        // No disposition (defensive) → Err(Error).
        match task_result_for(None) {
            Err(TaskExit::Error(e)) => assert!(e.message.contains("no disposition")),
            other => panic!("expected Err(Error), got {other:?}"),
        }
    }

    /// The pure watcher must return the terminal state a call settles into
    /// (always `Ended`, the only terminal `CallState`); the resolved
    /// `Disposition` on the record is what actually distinguishes cancelled
    /// from completed/failed. cap-0 parks the call in `Queued`; a delayed
    /// `end_call` flips it to `Ended` with `Disposition::Cancelled`.
    #[tokio::test]
    async fn watch_call_to_terminal_returns_cancelled() {
        use crate::state::{CallState, Disposition};
        let engine = Arc::new(test_engine(0).await);
        let call_id = engine
            .place_call(
                "600".into(),
                ScenarioConfig {
                    goal_schema: serde_json::json!({ "type": "object" }),
                    context: None,
                    prompt_override: None,
                },
            )
            .await;
        assert_eq!(
            engine.store().get(&call_id).unwrap().state,
            CallState::Queued
        );

        // Cancel shortly after the watcher begins polling.
        let e2 = engine.clone();
        let id2 = call_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            e2.end_call(&id2);
        });

        let terminal = watch_call_to_terminal(&engine, &call_id).await;
        assert_eq!(terminal, CallState::Ended);
        assert_eq!(
            engine.store().get(&call_id).unwrap().disposition,
            Some(Disposition::Cancelled)
        );
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

    /// A scheduled call (`schedule_at` in the future) must take the immediate
    /// `{call_id, server_time_unix}` branch even for a task-capable client —
    /// scheduled calls never enter the task/poll flavor, because the task
    /// would sit idle (no dispatch) until `eligible_at_ms`, defeating the
    /// point of the immediate-return + client-side polling model.
    #[tokio::test]
    async fn scheduled_call_takes_immediate_branch() {
        // cap-0 parks the call in Queued so no real INVITE is attempted.
        let engine = Arc::new(test_engine(0).await);
        let srv = KutsuServer::new(engine);

        let mut a = args("600");
        a.schedule_at = Some(crate::engine::now_ms() + 60_000);
        let resp = srv.place_call_inner(a, true).await.unwrap();
        let out = complete(resp);
        let text = first_text(&out);
        let json = serde_json::from_str::<serde_json::Value>(&text).unwrap();
        assert!(
            json.get("call_id")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
        );
        assert!(
            json.get("server_time_unix")
                .and_then(|v| v.as_u64())
                .is_some()
        );

        srv.tasks.shutdown();
    }
}
