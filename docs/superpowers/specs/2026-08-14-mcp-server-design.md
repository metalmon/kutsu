# MCP server layer (phase 5) — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-14
**Scope:** `src/mcp.rs` (new), plus foundation changes in `src/engine.rs` / `src/state.rs`, and the `mcp` CLI bootstrap in `src/main.rs`.

## Goal

Expose the call engine over MCP so an LLM client can place and observe outbound
phone calls. Four tools, thin wrappers over `crate::engine` / `crate::state`:
`place_call`, `get_call_status`, `get_call_transcript`, `end_call`. Both stdio
and streamable-http transports.

Enumerating all calls is deliberately **not** a tool: the client that placed a
call already holds its `call_id`. Global listing is an operational concern
(mirrors ext-tasks dropping `tasks/list`).

## Decisions (this session)

- **MCP model = hybrid: plain tools + ext-tasks on top.** The four tools are
  plain `#[tool]` functions (universal, work with any client). `place_call`
  additionally declares `TaskSupport::Optional`, so a task-aware client (e.g.
  the Claude harness) runs it as an MCP task and **polls `tasks/get`
  automatically** — the model never spends tool-calls hand-polling
  `get_call_status`. A non-task client gets the immediate `{call_id}` result.
  Rationale for keeping ext-tasks (vs pure plain tools): harness auto-polling.
- **Cap behavior = queue via `Semaphore`**, not reject. `place_call` always
  accepts and returns a `call_id` immediately; the worker waits for a permit;
  while waiting the record is `Queued`.
- **CLI = minimal.** Keep `mcp` (serve), self-contained `call` / `live`. No
  operator client subcommands.
- **No tool-gating profiles.** Always publish all four tools — the surface is
  already minimal.
- **rmcp 3.1** (our `Cargo.toml`), edition 2024, `schemars 1.0`, `axum 0.8`.
  Same stack as the sibling project `e:/glossa` **except** glossa is on rmcp
  1.8 — its patterns port, but exact API paths (`disable_route`,
  `#[tool_handler(router=…)]`, transport modules, `task_manager`) must be
  verified against rmcp 3.1 sources during implementation.

## Reused patterns from `e:/glossa` (proven, same stack)

- `#[derive(Clone)]` handler struct holding `Arc<…>` shared state; the router is
  built by the macro (`Self::tool_router()`), stored on an instance field, and
  the handler binds the **instance** router via `#[tool_handler(router =
  self.tool_router)]` (binding the default router silently drops any route
  mutation — documented glossa gotcha).
- Args wrapped `Parameters(a): Parameters<T>` where `T:
  Deserialize + JsonSchema` with per-field `#[schemars(description = …)]`.
- One error helper `fn internal(e) -> McpError { McpError::internal_error(…) }`,
  applied via `.map_err(internal)`. Success = `CallToolResult::success(vec![
  Content::text(json)])`.
- Transports: stdio via `serve` over the io transport; streamable-http via
  `StreamableHttpService` + `LocalSessionManager`, mounted with axum
  `nest_service("/mcp", svc)` and a session factory `move || Ok(server.clone())`.
- Logs to **stderr only** (stdout is the JSON-RPC channel on stdio).
- Graceful shutdown via `tokio_util::sync::CancellationToken` +
  `axum::serve(…).with_graceful_shutdown(…)`, Ctrl-C / SIGTERM.
- `get_info` advertises `ServerCapabilities::builder().enable_tools()` and rich
  `instructions`.

## Component 1 — engine / state foundation

These three items were flagged by the phase-4 engine as phase-5 scope (real
gaps, not tails). All changes are confined to `engine.rs` and `state.rs`; no
change to `sip` / `bridge` / `gemini_live`.

### 1a. Queue via `Semaphore` (replaces atomic CAS reject)

- `Engine` field: `permits: Arc<tokio::sync::Semaphore>` sized to
  `server.max_concurrent_channels`.
- `place_call` no longer cap-checks. It: inserts `CallRecord { state: Queued,
  … }`, creates the cancel channel (1b), spawns `run_call`, and returns the
  `call_id` immediately. `EngineError::CapReached` is **removed** (no code path
  can reach the cap now); the existing cap-reject unit test is replaced by the
  queue test in Component 4.
- Inside `run_call`, **before INVITE**, acquire the permit:
  `let permit = permits.acquire_owned().await` (raced against cancel — 1b).
  While the future is pending, the record stays `Queued`.
- The owned permit is held for the whole call and dropped in teardown — it
  replaces `SlotGuard` (the panic-safety property is preserved: an owned
  `SemaphorePermit` releases on drop, including on unwind).
- `queued_position` (best-effort): computed on read in `get_call_status` as the
  count of `Queued` records with a smaller sequence number. Not stored.

### 1b. `end_call` by id (cancel map)

- `Engine` field: `cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>`.
- `place_call` creates `oneshot::channel()`, inserts the sender under `call_id`,
  moves the receiver into `run_call`.
- `run_call` selects on `cancel_rx` in **both** phases:
  1. while awaiting the permit — `select! { p = acquire_owned() => …, _ =
     &mut cancel_rx => { finalize Cancelled; cleanup; return } }`;
  2. in the orchestration loop — a new `select!` arm `_ = &mut cancel_rx =>
     break CallState::Cancelled`.
- On any exit, `run_call` removes its own entry from `cancels` (no leak).
- `Engine::end_call(&self, call_id: &str)` — public: remove the sender from the
  map and `send(())`. Idempotent: unknown/already-ended id is a no-op that the
  MCP layer maps to `invalid_params` only when the call record is absent
  entirely (an already-terminal call returns its terminal status, not an error).
- No new crate — `tokio::sync::oneshot`.

### 1c. New `CallState` variants

- Add `Queued` (before `Ringing`) and `Cancelled`. `#[serde(rename_all =
  "snake_case")]` already applied.
- Lifecycle: `Queued → Ringing → InProgress → { Completed | Failed | HungUp |
  Cancelled }`.
- Teardown reconcile (unchanged behavior) still applies: a model-initiated end
  (`EndedBy::ModelEndCall`) resolves to `Completed` even if its `EndCall` event
  lost the select race.

## Component 2 — `src/mcp.rs`

### Handler

```rust
#[derive(Clone)]
pub struct KutsuServer {
    engine: Arc<Engine>,
    tool_router: ToolRouter<Self>,
}
```

`#[tool_router]` on the impl; `KutsuServer::new(engine)` calls
`Self::tool_router()` and stores it. `#[tool_handler(router = self.tool_router)]`
on the `ServerHandler` impl. `get_info` → capabilities with tools enabled +
`instructions` describing the outbound-call workflow (place → poll → read
transcript / end).

### Tools

Each takes `Parameters<T>` and returns `Result<CallToolResult, McpError>`.

- **`place_call`** — `TaskSupport::Optional`.
  Args `PlaceCallArgs { to_number: String, system_prompt: String, goal_schema:
  serde_json::Value, context: Option<serde_json::Value> }`.
  Builds `ScenarioConfig`, calls `engine.place_call(to_number, scenario)`,
  returns `{ "call_id": … }`. As a task, the task id doubles as the `call_id`;
  status is driven from `CallState` (see mapping).
- **`get_call_status`** — Args `CallIdArgs { call_id }`.
  `engine.store().get(&call_id)` → `{ call_id, state, number, started_ms,
  ended_ms?, error?, queued_position? }`. Missing → `invalid_params`.
- **`get_call_transcript`** — Args `CallIdArgs`.
  → `{ call_id, state, transcript: [ {role, text, ts_ms} ], goal? }`.
  Missing → `invalid_params`.
- **`end_call`** — Args `CallIdArgs`.
  `engine.end_call(&call_id)`; returns `{ call_id, state }` (current state after
  signalling). Missing record → `invalid_params`.

### ext-tasks mapping

`place_call`-as-task status is derived from `CallState`:

| CallState               | TaskStatus  | message / result                       |
|-------------------------|-------------|----------------------------------------|
| Queued                  | working     | "queued (position N)"                  |
| Ringing                 | working     | "ringing"                              |
| InProgress              | working     | "in progress"                          |
| Completed               | completed   | result: `{ transcript, goal }`         |
| HungUp                  | completed   | result: `{ transcript, goal }`         |
| Cancelled               | cancelled   | —                                      |
| Failed                  | failed      | error text                             |

- `tasks/get` mirrors `get_call_status` (harness auto-polls this).
- `tasks/cancel` → `engine.end_call`.
- The task's terminal result carries the final transcript + goal so a task-aware
  client gets the outcome without a separate `get_call_transcript` call.

**Implementation risk / spike:** the exact rmcp 3.1 `task_manager` wiring (how a
tool registers a task, updates its status from an external state source, and
supplies the terminal result) must be read from the rmcp 3.1 sources before
coding this. If the 3.1 task API proves unworkable within phase-5 scope, the
fallback is to ship the four plain tools only and document the task layer as a
follow-up — a decision to be made and recorded during implementation, not
deferred silently.

### Errors

`fn internal(e) -> McpError` helper (`internal_error`). Unknown `call_id` →
`McpError::invalid_params`. No panics across the tool boundary.

## Component 3 — transports, bootstrap, CLI

- **stdio:** serve `KutsuServer` over the stdio io-transport; logs to stderr.
- **streamable-http:** `StreamableHttpService` + `LocalSessionManager`, mounted
  at `/mcp` via axum `nest_service`, plus a `/health` route. Session factory
  clones the handler over the shared `Arc<Engine>`. Default bind
  `127.0.0.1:8090`.
- **http auth (optional, YAGNI):** `--auth-token` / env `KUTSU_MCP_TOKEN`. If
  set, an axum middleware requires `Authorization: Bearer <token>` (else 401).
  If unset, no auth (loopback default).
- **Graceful shutdown:** `CancellationToken` on Ctrl-C / SIGTERM. On shutdown,
  `end_call` every active call first (SIP shutdown is abrupt/best-effort BYE —
  the engine must hang up calls before `sip.shutdown()`), then
  `engine.shutdown()`.

### Security hardening (IB baseline)

Distilled from an internal information-security baseline; only the low-cost
items that touch the phase-5 surface — persisted transcripts are PII (phone
number, full conversation, goal), so these are real gaps in what we build now,
not future scope.

- **Default bind `127.0.0.1`** (already the default) — the baseline names a
  loopback default as the correct network posture; exposure is opt-in via
  `--bind`.
- **Transcript file permissions.** The `transcript_dir` JSON written by
  `run_call` (`std::fs::write`) holds PII; write it with owner-only permissions
  (unix `0o600` via `OpenOptions`; on Windows, rely on the directory ACL and
  document that the operator must place `transcript_dir` outside world-readable
  paths). Applies to the `engine.rs` persist step, not `mcp.rs`.
- **Secret / PII logging discipline.** Never log `api_key`, `--auth-token` /
  `KUTSU_MCP_TOKEN`, the SIP password, or transcript text at `info`. Tracing
  stays at `info` by default (debug/trace off); the auth middleware must not log
  the presented token. A `#[derive(Debug)]` on any struct holding a secret must
  redact it (manual `Debug` or `secrecy`-style wrapper) — audited during
  implementation.
- **Bearer token on the http transport** — see `--auth-token` above; the
  baseline frames network integrations as needing a separate limited-access
  credential, which the token provides for the non-loopback case.

### Future security scope (named, not phase-5)

- Structured **security-event audit log** (categories auth / rights / accounts /
  admin; fields type/time/source/result/subject; export to SIEM via OTLP or
  webhook) — an observability-phase concern.
- **OIDC / IdP integration + RBAC / per-caller authorization** — a whole future
  subsystem; the dropped observer-profile / "service principal with a narrow
  profile" idea belongs here.
- **Idle-session timeout** — distinct from the existing `max_call_secs`
  call-duration cap.
- **Cloud egress in an on-prem contour** — kutsu reaches Gemini via proxy; in a
  strict isolated-network deployment this is an operator/deployment flag, not
  code.
- **CLI:** the existing `Mcp { transport, bind }` branch (currently a stub) is
  implemented: assemble `ServerConfig` + `SipConfig` from env/config, build
  `Engine::new`, start the chosen transport, await shutdown. Add `--auth-token`.
  `call` / `live` unchanged.

## Component 4 — testing

**Engine (unit, offline — loopback bind, calls fail at INVITE but the
store/queue/cancel paths exercise fully):**

- Queue: with `max_concurrent_channels = 1`, a second `place_call` stays
  `Queued`; releasing the first permit lets it proceed; `queued_position` is
  correct.
- `end_call` cancels both a `Queued` call (before INVITE) and an active call →
  `Cancelled`; the `cancels` map entry is removed (no leak).
- State transitions and the `ModelEndCall`-vs-race reconcile remain green.

**MCP layer (unit):**

- `list_tools` returns exactly the four tools; assert names and schemas.
- `place_call` returns a valid `call_id`; stringified-number arg coercion works
  (as glossa tests assert).
- Round-trip over a real `Arc<Engine>` on loopback: `place_call` →
  `get_call_status` → `get_call_transcript` (call fails at INVITE, but the
  response shapes and the store path are verified).
- Unknown `call_id` → `invalid_params` for all three id-taking tools.
- `CallState → TaskStatus` is a pure function with a table-driven test.
- `task_manager` tests are written **after** the rmcp 3.1 task API is pinned
  during implementation (avoid testing an invented signature).

**Live (`#[ignore]`, requires the WSL Asterisk stend):** start an in-process
stdio server, call `place_call`, await a `completed` task, assert the transcript.
Kept `#[ignore]` like the other live tests.

## Migration / touch list

- `src/state.rs`: add `CallState::Queued`, `CallState::Cancelled`.
- `src/engine.rs`: `permits` + `cancels` fields; rewrite the cap path to a
  semaphore acquire inside `run_call`; add cancel selects in both phases; remove
  `SlotGuard` (superseded by the owned permit); add `Engine::end_call`; remove
  `EngineError::CapReached` and its cap-reject test; write the transcript JSON
  with owner-only permissions (Security hardening).
- Logging: audit `Debug`/`tracing` sites for secret/PII leakage (Security
  hardening) across `mcp.rs`, `main.rs`, `engine.rs`.
- `src/mcp.rs`: implement the handler, four tools, task mapping, errors, `get_info`.
- `src/main.rs`: implement the `Mcp` branch; add `--auth-token`.
- `Cargo.toml`: confirm `tokio-util` (CancellationToken) availability; add if not
  already transitive.

## Out of scope (named as future scope, not tails)

- In-call "model needs an answer" → `input_required` + `tasks/update`: belongs
  with external tools / tool webhooks (phase 7).
- Recording (phase 6), external tools + webhooks (phase 7).
- Tool-gating profiles (decided not needed).
- Operator CLI client subcommands (decided minimal CLI).
