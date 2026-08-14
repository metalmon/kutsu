# MCP server (phase 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the call engine over MCP (stdio + streamable-http) with four tools — `place_call`, `get_call_status`, `get_call_transcript`, `end_call` — plus queue-based concurrency, cancel-by-id, ops endpoints, and IB-baseline hardening.

**Architecture:** Thin `src/mcp.rs` handler wraps a shared `Arc<Engine>`; the engine gains a `Semaphore` queue, a cancel-map for `end_call`, and a metrics snapshot. `place_call` is a plain tool that returns a `call_id` immediately (spawn + poll), additionally declaring MCP task support so a task-aware client auto-polls; the other three are plain tools.

**Tech Stack:** Rust (edition 2024), `rmcp 3.1` (features `server`, `transport-io`, `transport-streamable-http-server`, `macros`), `schemars 1.0`, `axum 0.8`, `tokio`, `serde_json`, `thiserror`.

**Spec:** `docs/superpowers/specs/2026-08-14-mcp-server-design.md`

## Global Constraints

- rmcp version pinned at `3.1` (`Cargo.toml`); edition `2024`.
- No heavy new dependency for metrics — `/metrics` is hand-rolled Prometheus text.
- `tokio::sync::oneshot` for cancel (no new crate); `tokio::sync::Semaphore` for the queue.
- Logs go to **stderr only** (stdout is the JSON-RPC channel on stdio).
- Never log secrets/PII: `api_key`, `KUTSU_MCP_TOKEN`/`--auth-token`, the SIP password, or transcript text.
- Persisted transcript JSON is PII — written owner-only (`0o600` on unix).
- Build/test on Windows uses `--features vendor-openssl` (SRTP/OpenSSL); the vendored-OpenSSL toolchain is already installed on this machine.
- TDD: every task is failing-test → run(fail) → minimal impl → run(pass) → commit.

## File structure

- `src/state.rs` (modify) — add `CallState::{Queued, Cancelled}`; `CallStore` gains `queued_position` and count helpers.
- `src/engine.rs` (modify) — `Semaphore` queue; `cancels` map + `Engine::end_call`; cumulative counters + `Engine::metrics_snapshot`; owner-only transcript write; remove `SlotGuard`/`EngineError::CapReached`.
- `src/mcp.rs` (rewrite from stub) — `KutsuServer` handler, four tools, arg/result types, `CallState → TaskStatus` mapping, errors, `get_info`.
- `src/main.rs` (modify) — implement the `Mcp` branch: build engine, start the chosen transport, mount ops routes, `--auth-token`, graceful shutdown.
- `src/mcp_http.rs` (create) — axum wiring for streamable-http: `/mcp` service, `/health`, `/ready`, `/metrics`, optional bearer middleware. (Split from `main.rs` to keep it focused.)

---

## Component 1 — engine / state foundation

### Task 1: `CallState` gains `Queued` and `Cancelled`

**Files:**
- Modify: `src/state.rs` (the `CallState` enum, ~line 15)
- Test: `src/state.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `CallState::Queued`, `CallState::Cancelled` (serde snake_case → `"queued"`, `"cancelled"`).

- [ ] **Step 1: Write the failing test**

Add to `src/state.rs` tests:

```rust
#[test]
fn queued_and_cancelled_serialize_snake_case() {
    assert_eq!(serde_json::to_string(&CallState::Queued).unwrap(), "\"queued\"");
    assert_eq!(serde_json::to_string(&CallState::Cancelled).unwrap(), "\"cancelled\"");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu state::tests::queued_and_cancelled_serialize_snake_case`
Expected: FAIL — `no variant named Queued` / `Cancelled`.

- [ ] **Step 3: Add the variants**

In `src/state.rs`, extend the enum (keep `#[serde(rename_all = "snake_case")]`):

```rust
pub enum CallState {
    Queued,
    Ringing,
    InProgress,
    Completed,
    Failed,
    HungUp,
    Cancelled,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features vendor-openssl -p kutsu state::tests::queued_and_cancelled_serialize_snake_case`
Expected: PASS. Also run `cargo build --features vendor-openssl` to surface any non-exhaustive `match` on `CallState` and fix arms as compiler directs (there should be none outside tests yet).

- [ ] **Step 5: Commit**

```bash
git add src/state.rs
git commit -m "feat(state): add CallState::Queued and CallState::Cancelled"
```

### Task 2: `CallStore` position + count helpers

**Files:**
- Modify: `src/state.rs` (`impl CallStore`)
- Test: `src/state.rs` tests

**Interfaces:**
- Consumes: `CallState::Queued` (Task 1).
- Produces:
  - `CallStore::queued_position(&self, call_id: &str) -> Option<usize>` — 1-based position among `Queued` records ordered by `started_ms` then `call_id`; `None` if the record is absent or not `Queued`.
  - `CallStore::counts(&self) -> StateCounts` where `pub struct StateCounts { pub active: usize, pub queued: usize }` (`active` = records in `Ringing | InProgress`).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn queued_position_ranks_by_started_then_id() {
    let store = CallStore::new();
    let mut a = rec("call-0"); a.state = CallState::Queued; a.started_ms = 100;
    let mut b = rec("call-1"); b.state = CallState::Queued; b.started_ms = 200;
    let mut c = rec("call-2"); c.state = CallState::InProgress; c.started_ms = 50;
    store.insert(a); store.insert(b); store.insert(c);
    assert_eq!(store.queued_position("call-0"), Some(1));
    assert_eq!(store.queued_position("call-1"), Some(2));
    assert_eq!(store.queued_position("call-2"), None); // not queued
    assert_eq!(store.queued_position("missing"), None);
    let counts = store.counts();
    assert_eq!(counts.queued, 2);
    assert_eq!(counts.active, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu state::tests::queued_position_ranks_by_started_then_id`
Expected: FAIL — no method `queued_position` / `counts`.

- [ ] **Step 3: Implement the helpers**

Add to `impl CallStore` in `src/state.rs`:

```rust
/// 1-based position of a `Queued` call among all queued calls, ordered by
/// (started_ms, call_id). `None` if absent or not queued.
pub fn queued_position(&self, call_id: &str) -> Option<usize> {
    let g = self.inner.lock().unwrap();
    let target = g.get(call_id)?;
    if target.state != CallState::Queued {
        return None;
    }
    let key = (target.started_ms, target.call_id.as_str());
    let ahead = g
        .values()
        .filter(|r| r.state == CallState::Queued)
        .filter(|r| (r.started_ms, r.call_id.as_str()) < key)
        .count();
    Some(ahead + 1)
}

/// Snapshot of active (Ringing|InProgress) and Queued counts.
pub fn counts(&self) -> StateCounts {
    let g = self.inner.lock().unwrap();
    let mut c = StateCounts { active: 0, queued: 0 };
    for r in g.values() {
        match r.state {
            CallState::Ringing | CallState::InProgress => c.active += 1,
            CallState::Queued => c.queued += 1,
            _ => {}
        }
    }
    c
}
```

And add the struct near the top of `src/state.rs`:

```rust
/// Live counts of in-flight calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateCounts {
    pub active: usize,
    pub queued: usize,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features vendor-openssl -p kutsu state::tests::queued_position_ranks_by_started_then_id`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/state.rs
git commit -m "feat(state): queued_position + counts helpers on CallStore"
```

### Task 3: Engine queue via `Semaphore` (replace CAS reject)

**Files:**
- Modify: `src/engine.rs` — `Engine` struct/fields, `Engine::new`, `place_call`, `run_call` signature + permit acquire; remove `SlotGuard` + `EngineError::CapReached` + their tests.
- Test: `src/engine.rs` tests.

**Interfaces:**
- Consumes: `CallState::Queued` (Task 1).
- Produces:
  - `Engine.permits: Arc<tokio::sync::Semaphore>`.
  - `place_call` now returns `Ok(call_id)` unconditionally (signature `async fn place_call(&self, number: String, scenario: ScenarioConfig) -> String` — no `Result`, since the only prior error, `CapReached`, is gone; SIP/other failures surface via `CallState::Failed` in the record).
  - `run_call` acquires a permit before INVITE; the record is `Queued` until then.

- [ ] **Step 1: Write the failing test**

Replace `place_call_rejects_when_at_cap` with a queue test:

```rust
#[tokio::test]
async fn place_call_queues_when_at_cap() {
    // cap = 0: every call must sit in Queued forever (no permit available).
    let (server, sip_cfg) = test_configs(0);
    let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
    let id = engine.place_call("600".into(), test_scenario()).await;
    // No permit → the spawned run_call is parked before INVITE; state stays Queued.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let rec = engine.store().get(&id).unwrap();
    assert_eq!(rec.state, CallState::Queued);
    assert_eq!(engine.store().queued_position(&id), Some(1));
    engine.shutdown().await;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu engine::tests::place_call_queues_when_at_cap`
Expected: FAIL to compile (`place_call` still returns `Result`; `CapReached` path).

- [ ] **Step 3: Implement the semaphore queue**

In `src/engine.rs`:

1. Remove the `SlotGuard` struct + its two tests (`slot_guard_releases_on_drop`, `slot_guard_releases_on_panic`), the `AtomicUsize` `active`/`seq` cap logic, and `EngineError::CapReached`. Keep `seq` as an `AtomicUsize` for id generation.
2. Fields:

```rust
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    permits: Arc<tokio::sync::Semaphore>,
    seq: AtomicUsize,
}
```

3. `Engine::new` — build permits from the cap:

```rust
let permits = Arc::new(tokio::sync::Semaphore::new(server.max_concurrent_channels));
Ok(Self { sip, store: CallStore::new(), server, permits, seq: AtomicUsize::new(0) })
```

4. `place_call` — no cap check, no `Result`:

```rust
pub async fn place_call(&self, number: String, scenario: ScenarioConfig) -> String {
    let n = self.seq.fetch_add(1, Ordering::Relaxed);
    let call_id = format!("call-{n}");
    self.store.insert(CallRecord {
        call_id: call_id.clone(),
        number: number.clone(),
        state: CallState::Queued,
        transcript: vec![],
        goal: None,
        error: None,
        started_ms: now_ms(),
        ended_ms: None,
    });
    let sip = self.sip.clone();
    let store = self.store.clone();
    let server = self.server.clone();
    let permits = self.permits.clone();
    let id = call_id.clone();
    tokio::spawn(async move {
        run_call(sip, store, server, permits, scenario, number, id).await;
    });
    call_id
}
```

5. `run_call` — new `permits` param; acquire before INVITE:

```rust
async fn run_call(
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    permits: Arc<tokio::sync::Semaphore>,
    scenario: ScenarioConfig,
    number: String,
    call_id: String,
) {
    // Wait for a concurrency slot. Held (as an owned permit) for the whole
    // call; released on drop, including on panic unwind — replaces SlotGuard.
    let _permit = permits.acquire_owned().await.expect("semaphore not closed");
    store.set_state(&call_id, CallState::Ringing);
    // ... existing body from step "1. INVITE" onward, unchanged ...
}
```

(Move the record to `Ringing` right after acquiring; the existing body already sets `InProgress` after answer.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features vendor-openssl -p kutsu engine::`
Expected: PASS (`place_call_queues_when_at_cap` + the untouched engine tests). Fix any callers of `place_call` that expected a `Result` (the CLI `Call` branch in `main.rs` — adjust to the new `String` return; a call that fails now shows up as a `Failed` record, so the CLI reads the store instead of a `Result`).

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/main.rs
git commit -m "feat(engine): queue via Semaphore, drop CapReached/SlotGuard"
```

### Task 4: `end_call` by id (cancel-map) + `Cancelled`

**Files:**
- Modify: `src/engine.rs` — `cancels` field, `place_call` wiring, `run_call` selects, `Engine::end_call`, teardown cleanup.
- Test: `src/engine.rs` tests.

**Interfaces:**
- Consumes: `CallState::Cancelled` (Task 1); the queue machinery (Task 3).
- Produces: `Engine::end_call(&self, call_id: &str) -> bool` (true if a live cancel signal was delivered; false if unknown/already-ended).

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn end_call_cancels_a_queued_call() {
    let (server, sip_cfg) = test_configs(0); // cap 0 → parked in Queued
    let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
    let id = engine.place_call("600".into(), test_scenario()).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(engine.store().get(&id).unwrap().state, CallState::Queued);
    assert!(engine.end_call(&id));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(engine.store().get(&id).unwrap().state, CallState::Cancelled);
    // Cancel map entry cleaned up: a second end_call finds nothing live.
    assert!(!engine.end_call(&id));
    engine.shutdown().await;
}

#[tokio::test]
async fn end_call_unknown_id_is_false() {
    let (server, sip_cfg) = test_configs(1);
    let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
    assert!(!engine.end_call("nope"));
    engine.shutdown().await;
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --features vendor-openssl -p kutsu engine::tests::end_call_`
Expected: FAIL — no method `end_call`.

- [ ] **Step 3: Implement the cancel-map**

In `src/engine.rs`:

1. Field + import:

```rust
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;
// ...
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    permits: Arc<tokio::sync::Semaphore>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    seq: AtomicUsize,
}
```

Initialize `cancels: Arc::new(Mutex::new(HashMap::new()))` in `new`.

2. `place_call` — create the channel, register the sender, pass the receiver:

```rust
let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
self.cancels.lock().unwrap().insert(call_id.clone(), cancel_tx);
let cancels = self.cancels.clone();
// ...spawn:
run_call(sip, store, server, permits, cancels, cancel_rx, scenario, number, id).await;
```

3. `run_call` — new params `cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>` and `mut cancel_rx: oneshot::Receiver<()>`. Race the cancel against the permit:

```rust
let _permit = tokio::select! {
    p = permits.acquire_owned() => p.expect("semaphore not closed"),
    _ = &mut cancel_rx => {
        store.finalize(&call_id, CallState::Cancelled, None, None, now_ms());
        cancels.lock().unwrap().remove(&call_id);
        return;
    }
};
store.set_state(&call_id, CallState::Ringing);
```

Add a cancel arm to the orchestration `select!`:

```rust
_ = &mut cancel_rx => break CallState::Cancelled,
```

At the very end of `run_call` (after finalize/persist), drop the map entry:

```rust
cancels.lock().unwrap().remove(&call_id);
```

Reconcile: keep the existing `EndedBy::ModelEndCall → Completed` rule; a `Cancelled` end_state stays `Cancelled` unless the model already ended (then `Completed`).

4. `Engine::end_call`:

```rust
/// Signal a running/queued call to end. Returns true if a live signal was sent.
pub fn end_call(&self, call_id: &str) -> bool {
    if let Some(tx) = self.cancels.lock().unwrap().remove(call_id) {
        tx.send(()).is_ok()
    } else {
        false
    }
}
```

Note: `end_call` removes the sender, so `run_call`'s own final `remove` is a harmless no-op in the cancel path.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features vendor-openssl -p kutsu engine::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): end_call cancel-map + Cancelled state"
```

### Task 5: Metrics snapshot + cumulative counters + owner-only transcript

**Files:**
- Modify: `src/engine.rs` — cumulative counters, `Engine::metrics_snapshot`, `MetricsSnapshot`, owner-only transcript write.
- Test: `src/engine.rs` tests.

**Interfaces:**
- Consumes: `CallStore::counts` (Task 2).
- Produces:
  - `pub struct MetricsSnapshot { pub active: usize, pub queued: usize, pub placed_total: u64, pub completed_total: u64, pub failed_total: u64, pub cancelled_total: u64, pub channels_cap: usize }`.
  - `Engine::metrics_snapshot(&self) -> MetricsSnapshot`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn metrics_snapshot_counts_placed_and_queued() {
    let (server, sip_cfg) = test_configs(0); // cap 0 → calls park in Queued
    let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
    let _a = engine.place_call("600".into(), test_scenario()).await;
    let _b = engine.place_call("601".into(), test_scenario()).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let m = engine.metrics_snapshot();
    assert_eq!(m.placed_total, 2);
    assert_eq!(m.queued, 2);
    assert_eq!(m.active, 0);
    assert_eq!(m.channels_cap, 0);
    engine.shutdown().await;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu engine::tests::metrics_snapshot_counts_placed_and_queued`
Expected: FAIL — no `metrics_snapshot`.

- [ ] **Step 3: Implement counters + snapshot + secure write**

1. Add cumulative atomics to `Engine` (all `Arc<AtomicU64>`): `placed`, `completed`, `failed`, `cancelled`. Initialize to 0 in `new`. Clone the four into the spawned task so `run_call` can bump them.
2. `place_call`: `self.placed.fetch_add(1, Ordering::Relaxed);` right after inserting the record.
3. `run_call` finalize points: after the terminal `store.finalize(...)`, bump the matching counter based on `final_state` (`Completed | HungUp → completed`, `Failed → failed`, `Cancelled → cancelled`). Pass an `Arc<Counters>` bundle or the four clones; a small `struct Counters { placed, completed, failed, cancelled }` wrapped in `Arc` keeps the `run_call` signature tidy — define it and thread one `Arc<Counters>` instead of four params.
4. `metrics_snapshot`:

```rust
pub fn metrics_snapshot(&self) -> MetricsSnapshot {
    let counts = self.store.counts();
    MetricsSnapshot {
        active: counts.active,
        queued: counts.queued,
        placed_total: self.counters.placed.load(Ordering::Relaxed),
        completed_total: self.counters.completed.load(Ordering::Relaxed),
        failed_total: self.counters.failed.load(Ordering::Relaxed),
        cancelled_total: self.counters.cancelled.load(Ordering::Relaxed),
        channels_cap: self.server.max_concurrent_channels,
    }
}
```

5. Owner-only transcript write — replace the persist `std::fs::write` block:

```rust
#[cfg(unix)]
fn write_owner_only(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true).create(true).truncate(true).mode(0o600).open(path)?;
    std::io::Write::write_all(&mut f, data)
}
#[cfg(not(unix))]
fn write_owner_only(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, data) // Windows: rely on directory ACL (documented)
}
```

and call `write_owner_only(&path, json.as_bytes())` in the persist step.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features vendor-openssl -p kutsu engine::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): metrics_snapshot + counters + owner-only transcript write"
```

---

## Component 2 — `src/mcp.rs` (MCP handler + tools)

rmcp 3.1.2 facts (confirmed from `~/.cargo/registry/.../rmcp-3.1.2/src`):
imports `rmcp::{tool, tool_router, tool_handler, ServerHandler, ErrorData}`,
`rmcp::handler::server::tool::ToolRouter`,
`rmcp::handler::server::wrapper::Parameters`,
`rmcp::model::{CallToolResult, ContentBlock, ServerInfo, ServerCapabilities}`.
Errors are `rmcp::ErrorData` (no `McpError` export); content is `ContentBlock`
(not `Content`); results via `CallToolResult::success(vec![...])` /
`::structured(Value)`.

### Task 6: Handler skeleton, arg types, `get_info`, tool list

**Files:**
- Rewrite: `src/mcp.rs` (currently a doc-only stub)
- Test: `src/mcp.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::engine::Engine` (`store()`, `place_call`, `end_call`,
  `metrics_snapshot`).
- Produces:
  - `pub struct KutsuServer { engine: Arc<Engine>, tool_router: ToolRouter<Self> }` (`#[derive(Clone)]`).
  - `KutsuServer::new(engine: Arc<Engine>) -> Self`.
  - Arg types `PlaceCallArgs`, `CallIdArgs`.

- [ ] **Step 1: Write the failing test**

Add a local test config helper (loopback, offline) + a list_tools assertion:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Model, NetCheckConfig, ServerConfig, SipConfig};

    fn test_engine() -> Arc<Engine> {
        let server = ServerConfig {
            api_key: "k".into(), proxy: None, model: Model::HalfCascade,
            voice: "Autonoe".into(), language: "en-US".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 1,
            greet_after_silence_ms: 4000, transcript_dir: None, max_call_secs: 600,
        };
        let sip = SipConfig {
            server: "127.0.0.1:5060".into(), username: "t".into(), password: "t".into(),
            from_user: None, local_ip: Some("127.0.0.1".parse().unwrap()),
            register: false, transport: Default::default(),
        };
        let engine = tokio::runtime::Handle::current()
            .block_on(async { Engine::new(std::sync::Arc::new(server), &sip).await.unwrap() });
        Arc::new(engine)
    }

    #[tokio::test]
    async fn lists_exactly_four_tools() {
        let srv = KutsuServer::new(
            std::sync::Arc::new(Engine::new(/* build inline like test_engine */ todo_configs()).await.unwrap())
        );
        let names: Vec<_> = srv.tool_router.list_all().into_iter().map(|t| t.name.to_string()).collect();
        assert_eq!(names.len(), 4);
        for n in ["place_call", "get_call_status", "get_call_transcript", "end_call"] {
            assert!(names.contains(&n.to_string()), "missing {n}");
        }
    }
}
```

(Build the engine directly in the async test — inline the `ServerConfig`/
`SipConfig` from the helper above; drop the `block_on` variant. Keep one
inline builder to avoid the `block_on`-inside-async pitfall.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu mcp::tests::lists_exactly_four_tools`
Expected: FAIL to compile — `KutsuServer` undefined.

- [ ] **Step 3: Implement the skeleton + four tool stubs**

```rust
//! MCP server layer (rmcp 3.1) — thin wrapper over the call engine.
use std::sync::Arc;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::config::ScenarioConfig;
use crate::engine::Engine;

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

fn internal(e: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(e.to_string(), None)
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
    async fn place_call(&self, Parameters(a): Parameters<PlaceCallArgs>)
        -> Result<CallToolResult, ErrorData> {
        let scenario = ScenarioConfig {
            system_prompt: a.system_prompt, goal_schema: a.goal_schema, context: a.context,
        };
        let call_id = self.engine.place_call(a.to_number, scenario).await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "call_id": call_id }).to_string(),
        )]))
    }

    #[tool(description = "Get the current state of a call by call_id (lightweight; no transcript).")]
    async fn get_call_status(&self, Parameters(a): Parameters<CallIdArgs>)
        -> Result<CallToolResult, ErrorData> {
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
    async fn get_call_transcript(&self, Parameters(a): Parameters<CallIdArgs>)
        -> Result<CallToolResult, ErrorData> {
        let rec = self.engine.store().get(&a.call_id).ok_or_else(|| unknown_call(&a.call_id))?;
        let body = serde_json::json!({
            "call_id": rec.call_id, "state": rec.state,
            "transcript": rec.transcript, "goal": rec.goal,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(body.to_string())]))
    }

    #[tool(description = "End (hang up / cancel) a running or queued call by call_id.")]
    async fn end_call(&self, Parameters(a): Parameters<CallIdArgs>)
        -> Result<CallToolResult, ErrorData> {
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
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some(
                "Outbound calling: place_call → poll get_call_status → \
                 get_call_transcript; end_call to hang up.".into()),
            ..Default::default()
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features vendor-openssl -p kutsu mcp::tests::lists_exactly_four_tools`
Expected: PASS. Also `cargo build --features vendor-openssl` clean.

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs src/lib.rs
git commit -m "feat(mcp): KutsuServer handler + four tools + get_info"
```

### Task 7: Tool behavior round-trip + error tests

**Files:**
- Test: `src/mcp.rs` tests (behavioral coverage for the tools from Task 6)

**Interfaces:**
- Consumes: `KutsuServer` tools (Task 6). Tools are private methods; test them by
  calling them directly on a `KutsuServer` instance (same crate).

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn place_then_status_and_transcript_roundtrip() {
    let engine = /* inline test engine, cap 1 */;
    let srv = KutsuServer::new(engine.clone());
    let out = srv.place_call(Parameters(PlaceCallArgs {
        to_number: "600".into(), system_prompt: "hi".into(),
        goal_schema: serde_json::json!({"type":"object"}), context: None,
    })).await.unwrap();
    let text = first_text(&out);
    let call_id = serde_json::from_str::<serde_json::Value>(&text).unwrap()["call_id"]
        .as_str().unwrap().to_string();

    let status = srv.get_call_status(Parameters(CallIdArgs { call_id: call_id.clone() })).await.unwrap();
    assert!(first_text(&status).contains("\"state\""));

    let tr = srv.get_call_transcript(Parameters(CallIdArgs { call_id })).await.unwrap();
    assert!(first_text(&tr).contains("\"transcript\""));
    engine.shutdown().await;
}

#[tokio::test]
async fn unknown_call_id_is_invalid_params() {
    let engine = /* inline test engine */;
    let srv = KutsuServer::new(engine.clone());
    for r in [
        srv.get_call_status(Parameters(CallIdArgs { call_id: "x".into() })).await,
        srv.get_call_transcript(Parameters(CallIdArgs { call_id: "x".into() })).await,
        srv.end_call(Parameters(CallIdArgs { call_id: "x".into() })).await,
    ] {
        let err = r.unwrap_err();
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }
    engine.shutdown().await;
}

// helper
fn first_text(r: &CallToolResult) -> String {
    match r.content.first().unwrap() {
        ContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text content"),
    }
}
```

(Confirm the `ContentBlock::Text` variant shape and `ErrorData.code` /
`ErrorCode::INVALID_PARAMS` path from `rmcp::model` when writing — adjust the
destructuring to the actual variant if it differs.)

- [ ] **Step 2: Run tests to verify they fail, then pass**

Run: `cargo test --features vendor-openssl -p kutsu mcp::tests::`
Expected: initially FAIL if helpers/paths are off; once the destructuring matches
rmcp's actual `ContentBlock`/`ErrorData`, PASS (the Task-6 impl already satisfies
the behavior — these tests lock it in).

- [ ] **Step 3: Commit**

```bash
git add src/mcp.rs
git commit -m "test(mcp): tool round-trip + invalid_params coverage"
```

### Task 8: `CallState → TaskStatus` pure mapping

**Files:**
- Modify: `src/mcp.rs` — add `task_status_for`.
- Test: `src/mcp.rs` tests.

**Interfaces:**
- Produces: `fn task_status_for(state: CallState) -> rmcp::model::TaskStatus`
  and `fn is_terminal_state(state: CallState) -> bool`.

- [ ] **Step 1: Write the failing test**

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu mcp::tests::task_status_mapping`
Expected: FAIL — `task_status_for` undefined.

- [ ] **Step 3: Implement the mapping**

```rust
use rmcp::model::TaskStatus;
use crate::state::CallState;

fn task_status_for(state: CallState) -> TaskStatus {
    match state {
        CallState::Queued | CallState::Ringing | CallState::InProgress => TaskStatus::Working,
        CallState::Completed | CallState::HungUp => TaskStatus::Completed,
        CallState::Cancelled => TaskStatus::Cancelled,
        CallState::Failed => TaskStatus::Failed,
    }
}
fn is_terminal_state(state: CallState) -> bool {
    !matches!(state, CallState::Queued | CallState::Ringing | CallState::InProgress)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features vendor-openssl -p kutsu mcp::tests::task_status_mapping`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs
git commit -m "feat(mcp): CallState -> TaskStatus mapping"
```

### Task 9: `place_call` task branch (harness auto-poll)

**Isolated by design:** the plain-tool `place_call` (Task 6) already works for
every client. This task adds the ext-tasks path so a task-capable client (the
harness) auto-polls. It is reviewable on its own; if the framework wiring below
proves to need a different accessor, only this task changes.

**Files:**
- Modify: `src/mcp.rs` — `KutsuServer` gains an `Arc<TaskManager>`; `place_call`
  branches to a task when the client declared the tasks capability; `get_info`
  gains `.enable_tasks()`.
- Test: `src/mcp.rs` tests (task future watcher on a `Cancelled` path).

**Interfaces:**
- Consumes: `task_status_for` / `is_terminal_state` (Task 8); `Engine::{place_call, end_call, store}`.
- Produces: task-returning `place_call` for task-capable clients.

- [ ] **Step 1: Reconnaissance (read, then code — not a placeholder)**

Read these rmcp 3.1.2 sources to pin two accessors:
1. `task_manager.rs` — `TaskManager::new(...)`, `spawn(TaskOptions, |ctx: TaskContext| TaskFuture) -> Task`, and how `ServerHandler`'s default `get_task`/`update_task`/`cancel_task` locate the manager (look for a `fn task_manager(&self)` hook on `ServerHandler` in `handler/server.rs`). Whichever hook exists, override it to return `self.tasks`.
2. `handler/server.rs:~200-215` (the task-capability gate) and `service` peer info — how a tool fn reads whether the **current client** declared the tasks capability. If a `RequestContext<RoleServer>` param exposes `peer.peer_info()`/negotiated capabilities, use it; that is the branch condition.

Write down the two exact signatures before Step 3.

- [ ] **Step 2: Write the failing test (cancellation via the task future)**

```rust
#[tokio::test]
async fn task_future_cancels_via_engine() {
    // A pure exercise of the watcher loop: drive a queued call to Cancelled and
    // assert the future resolves to a Cancelled exit. Uses a cap-0 engine so the
    // call parks in Queued, then end_call flips it to Cancelled.
    let engine = /* inline cap-0 test engine */;
    let call_id = engine.place_call("600".into(), test_scenario()).await;
    // watcher: loop set_status_message until terminal (simulated inline here)
    let exit = super::watch_call_to_terminal(&engine, &call_id, /* cancel probe */).await;
    // after end_call the state is Cancelled -> exit maps to Cancelled
    engine.end_call(&call_id);
    assert!(matches!(exit_after_cancel(&engine, &call_id), CallState::Cancelled));
}
```

Factor the watcher into a testable async fn
`async fn watch_call_to_terminal(engine: &Engine, call_id: &str) -> CallState`
that polls `store().get(call_id).state` on a short interval until
`is_terminal_state`, returning the terminal `CallState`. Test THAT directly
(spawn end_call after a delay, assert it returns `Cancelled`). Keep the
framework `TaskContext` glue thin around this pure watcher.

- [ ] **Step 3: Implement**

1. Add `tasks: Arc<rmcp::task_manager::TaskManager>` to `KutsuServer`; build it in
   `new` (`TaskManager::new(Default::default())` or the confirmed constructor).
2. Override the `ServerHandler` task-manager hook (from Step 1) to return `&self.tasks`.
3. `get_info`: `ServerCapabilities::builder().enable_tools().enable_tasks().build()`.
4. Extract the pure watcher:

```rust
async fn watch_call_to_terminal(engine: &Engine, call_id: &str) -> crate::state::CallState {
    loop {
        if let Some(rec) = engine.store().get(call_id) {
            if is_terminal_state(rec.state) { return rec.state; }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

5. In `place_call`, after `let call_id = engine.place_call(...)`, branch:
   - **Client supports tasks** → `self.tasks.spawn(TaskOptions::default(), move |ctx| {
     Box::pin(async move { /* loop: update ctx.set_status_message from state +
     queued_position; select on ctx.cancelled() -> engine.end_call(&call_id) ->
     Err(TaskExit::Cancelled); on terminal -> build CallToolResult::structured
     from the record -> Ok, or Err(TaskExit::Error) if Failed */ }) })` then return
     `CallToolResponse::Task(CreateTaskResult::new(task)).into()` /
     the `IntoCallToolResult` path confirmed in Step 1.
   - **Otherwise** → the existing immediate `{call_id}` result.

   Use the pure `watch_call_to_terminal` inside the future, interleaved with
   `set_status_message` and `ctx.cancelled()` via `tokio::select!`.

- [ ] **Step 4: Run tests**

Run: `cargo test --features vendor-openssl -p kutsu mcp::`
Expected: PASS (watcher test + earlier mcp tests). If the client-capability
accessor from Step 1 turns out unavailable, STOP and report — do not silently
ship place_call always-immediate; the design intends the task branch. (This is
the spec's named decision point, now with the reconnaissance in hand.)

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs
git commit -m "feat(mcp): place_call task branch for task-capable clients"
```

## Component 3 — transports, ops endpoints, CLI

### Task 10: `src/mcp_http.rs` — ops endpoints + Prometheus + bearer

**Files:**
- Create: `src/mcp_http.rs`
- Modify: `src/lib.rs` (add `pub mod mcp_http;`)
- Test: `src/mcp_http.rs` tests (pure Prometheus formatter)

**Interfaces:**
- Consumes: `Engine::metrics_snapshot() -> MetricsSnapshot` (Task 5); `KutsuServer` (Task 6).
- Produces:
  - `pub fn render_prometheus(m: &MetricsSnapshot) -> String`.
  - `pub async fn serve(engine: Arc<Engine>, bind: &str, auth_token: Option<String>) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MetricsSnapshot;
    #[test]
    fn prometheus_has_all_series() {
        let m = MetricsSnapshot { active: 1, queued: 2, placed_total: 3,
            completed_total: 4, failed_total: 5, cancelled_total: 6, channels_cap: 3 };
        let s = render_prometheus(&m);
        for line in [
            "kutsu_calls_active 1", "kutsu_calls_queued 2", "kutsu_calls_placed_total 3",
            "kutsu_calls_completed_total 4", "kutsu_calls_failed_total 5",
            "kutsu_calls_cancelled_total 6", "kutsu_channels_cap 3",
        ] { assert!(s.contains(line), "missing: {line}"); }
        assert!(s.contains("# TYPE kutsu_calls_placed_total counter"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features vendor-openssl -p kutsu mcp_http::tests::prometheus_has_all_series`
Expected: FAIL — module/fn undefined.

- [ ] **Step 3: Implement the formatter + axum server**

```rust
//! streamable-http transport wiring + ops endpoints (/health, /ready, /metrics).
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};

use crate::engine::{Engine, MetricsSnapshot};
use crate::mcp::KutsuServer;

pub fn render_prometheus(m: &MetricsSnapshot) -> String {
    let mut s = String::new();
    let g = |s: &mut String, name: &str, help: &str, kind: &str, v: String| {
        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}\n"));
    };
    g(&mut s, "kutsu_calls_active", "Calls ringing or in progress.", "gauge", m.active.to_string());
    g(&mut s, "kutsu_calls_queued", "Calls waiting for a channel.", "gauge", m.queued.to_string());
    g(&mut s, "kutsu_calls_placed_total", "Calls placed since start.", "counter", m.placed_total.to_string());
    g(&mut s, "kutsu_calls_completed_total", "Calls completed.", "counter", m.completed_total.to_string());
    g(&mut s, "kutsu_calls_failed_total", "Calls failed.", "counter", m.failed_total.to_string());
    g(&mut s, "kutsu_calls_cancelled_total", "Calls cancelled.", "counter", m.cancelled_total.to_string());
    g(&mut s, "kutsu_channels_cap", "Max concurrent channels.", "gauge", m.channels_cap.to_string());
    s
}

async fn health() -> impl IntoResponse { (StatusCode::OK, "ok") }
async fn ready() -> impl IntoResponse { (StatusCode::OK, "ready") }
async fn metrics(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    (StatusCode::OK, [("content-type", "text/plain; version=0.0.4")],
     render_prometheus(&engine.metrics_snapshot()))
}

pub async fn serve(engine: Arc<Engine>, bind: &str, auth_token: Option<String>) -> anyhow::Result<()> {
    let mcp_engine = engine.clone();
    let svc = StreamableHttpService::new(
        move || Ok(KutsuServer::new(mcp_engine.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig { ..Default::default() },
    );
    let mut mcp_router = Router::new().route_service("/mcp", svc);
    if let Some(token) = auth_token {
        mcp_router = mcp_router.layer(axum::middleware::from_fn(move |req, next| {
            let token = token.clone();
            async move { bearer_guard(token, req, next).await }
        }));
    }
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(engine.clone())
        .merge(mcp_router);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "kutsu MCP streamable-http listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(crate::mcp_http::shutdown_signal())
        .await?;
    Ok(())
}
```

Add `bearer_guard` (compare `Authorization: Bearer <token>`; 401 on mismatch —
never log the presented token) and `shutdown_signal` (Ctrl-C / SIGTERM via
`tokio::signal`). Confirm the exact `route_service` / middleware signatures
against axum 0.8 when compiling; adjust the `from_fn` closure arity if needed.

- [ ] **Step 4: Run test + build**

Run: `cargo test --features vendor-openssl -p kutsu mcp_http::tests::prometheus_has_all_series`
then `cargo build --features vendor-openssl`.
Expected: PASS + clean build.

- [ ] **Step 5: Commit**

```bash
git add src/mcp_http.rs src/lib.rs
git commit -m "feat(mcp): streamable-http serve + /health /ready /metrics + bearer"
```

### Task 11: `main.rs` — implement the `Mcp` branch

**Files:**
- Modify: `src/main.rs` (the `Command::Mcp` arm, currently a stub) + add `--auth-token` arg.
- Read for reuse: `src/main_support.rs` (existing `ServerConfig`/`SipConfig` assembly used by `call`/`live`).

**Interfaces:**
- Consumes: `KutsuServer` (Task 6), `mcp_http::serve` (Task 10), the config
  loader in `main_support.rs`, `Engine::new`.

- [ ] **Step 1: Read the existing loader**

Read `src/main_support.rs` and the `Call`/`Live` arms in `main.rs` to find the
exact function(s) that build `ServerConfig` + `SipConfig` from env/args. Reuse
them — do NOT invent new env var names.

- [ ] **Step 2: Add `--auth-token` to the `Mcp` subcommand**

```rust
/// Bearer token required on the streamable-http transport (env KUTSU_MCP_TOKEN).
#[arg(long = "auth-token", env = "KUTSU_MCP_TOKEN")]
auth_token: Option<String>,
```

- [ ] **Step 3: Implement the branch**

```rust
Command::Mcp { transport, bind, auth_token } => {
    let (server_cfg, sip_cfg) = /* reuse main_support loader */;
    let engine = std::sync::Arc::new(
        kutsu::engine::Engine::new(std::sync::Arc::new(server_cfg), &sip_cfg).await?);
    match transport.as_str() {
        "stdio" => {
            use rmcp::{transport::io::stdio, ServiceExt};
            let handler = kutsu::mcp::KutsuServer::new(engine.clone());
            let service = handler.serve(stdio()).await?;
            service.waiting().await?;
        }
        "streamable-http" => {
            kutsu::mcp_http::serve(engine.clone(), &bind, auth_token).await?;
        }
        other => anyhow::bail!("unknown transport: {other}"),
    }
    // Graceful teardown: hang up any live calls, then shut the SIP transport.
    for rec in engine.store().list() {
        engine.end_call(&rec.call_id);
    }
    // engine is Arc; unwrap-or-drop. If Arc::try_unwrap succeeds, call shutdown().
    if let Ok(e) = std::sync::Arc::try_unwrap(engine) { e.shutdown().await; }
}
```

(For stdio, shutdown runs after `service.waiting()` returns. Confirm
`ServiceExt`/`serve`/`waiting` names against rmcp 3.1 `service.rs` while
compiling.)

- [ ] **Step 4: Build + manual smoke**

Run: `cargo build --features vendor-openssl`
Manual: `cargo run --features vendor-openssl -- mcp --transport streamable-http --bind 127.0.0.1:8090`
then `curl -s localhost:8090/health` → `ok`, `curl -s localhost:8090/metrics` → the series.
Expected: clean build; endpoints respond.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat(cli): implement kutsu mcp serve (stdio + streamable-http)"
```

### Task 12: Selectable JSON log format

**Files:**
- Modify: `Cargo.toml` (add `json` to `tracing-subscriber` features).
- Modify: `src/main.rs` (logger init ~line 74; add `--log-format` global arg).

**Interfaces:**
- Consumes: nothing. Cross-cutting — affects all subcommands' logging.
- Produces: `--log-format text|json` (env `KUTSU_LOG_FORMAT`, default `text`).

- [ ] **Step 1: Enable the feature**

In `Cargo.toml`:

```toml
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "json"] }
```

- [ ] **Step 2: Add the global arg**

On the top-level `Cli` struct in `src/main.rs`:

```rust
/// Log output format: text (dev, default) or json (machine-parseable / SIEM).
#[arg(long = "log-format", env = "KUTSU_LOG_FORMAT", default_value = "text", global = true)]
log_format: String,
```

- [ ] **Step 3: Branch the subscriber init**

Replace the `tracing_subscriber::fmt()...try_init()` block (~line 74) with a
branch on `cli.log_format`, keeping the existing `EnvFilter` + `stderr` writer:

```rust
let filter = tracing_subscriber::EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
match cli.log_format.as_str() {
    "json" => { let _ = builder.json().try_init(); }
    _ => { let _ = builder.try_init(); }
}
```

- [ ] **Step 4: Build + manual verify**

Run: `cargo build --features vendor-openssl`
Manual: `cargo run --features vendor-openssl -- --log-format json mcp --transport streamable-http`
Expected: startup log line is a single-line JSON object (`{"timestamp":...,"level":"INFO",...}`) on stderr; default (no flag) stays human-readable text.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/main.rs
git commit -m "feat(cli): selectable text/json log format (KUTSU_LOG_FORMAT)"
```

## Component 4 — live end-to-end (optional, gated)

### Task 13: `#[ignore]` in-process stdio round-trip

**Files:**
- Create: `tests/mcp_stdio.rs` (integration test, `#[ignore]`)

**Interfaces:**
- Consumes: the built binary / `KutsuServer` via an in-process rmcp client.

- [ ] **Step 1: Write the ignored test**

```rust
//! Live MCP round-trip against the WSL Asterisk stend. Run with:
//!   cargo test --features vendor-openssl --test mcp_stdio -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires the WSL Asterisk stend + a reachable trunk"]
async fn place_call_via_mcp_completes() {
    // Build KutsuServer over a real Engine (stend SIP config), call place_call
    // through an in-process rmcp client transport, poll get_call_status until a
    // terminal state, then assert get_call_transcript is non-empty.
    // Fill the client-transport setup from rmcp 3.1 `service` docs at impl time.
}
```

- [ ] **Step 2: Verify it is collected but skipped**

Run: `cargo test --features vendor-openssl --test mcp_stdio`
Expected: `0 passed; ... 1 ignored`.

- [ ] **Step 3: Commit**

```bash
git add tests/mcp_stdio.rs
git commit -m "test(mcp): ignored live stdio round-trip harness"
```

---

## Self-review

**Spec coverage:** engine queue (Task 3), end_call cancel-map (Task 4), Queued/
Cancelled states (Task 1), queued_position (Task 2), metrics_snapshot + owner-
only transcript (Task 5), four tools (Task 6–7), task branch + status mapping
(Task 8–9), stdio + streamable-http + /health//ready//metrics + bearer (Task
10–11), CLI `mcp` branch + graceful shutdown (Task 11), selectable JSON log
format (Task 12), live test (Task 13). All spec sections map to a task.

**Known confirm-at-impl points (reconnaissance steps, not placeholders):**
Task 7 `ContentBlock`/`ErrorData` destructuring; Task 9 the client-tasks-
capability accessor + `ServerHandler` task-manager hook; Task 10 axum 0.8
`route_service`/middleware arity; Task 11 rmcp `ServiceExt` names + the
`main_support` loader. Each names the exact file to read.

**Out of scope (spec):** input_required/webhooks (phase 7), recording (phase 6),
OIDC/RBAC, idle timeout, OTLP push, tool-gating profiles, operator CLI client.
