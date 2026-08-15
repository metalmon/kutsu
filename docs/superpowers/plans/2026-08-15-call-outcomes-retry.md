# Call Outcomes + Retry Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the collapsed `Failed` dial result into a structured `CallOutcome`, and replace the implicit semaphore queue with an engine scheduler that retries busy calls internally and lets an external agent schedule no-answer callbacks.

**Architecture:** A pure `CallOutcome` is parsed from the SIP status code and carried on `CallRecord`. An engine `Scheduler` (behind a `QueueStore` trait, in-memory for now) owns an ordered pending queue with per-entry `eligible_at`, replacing spawn-and-park-on-semaphore. Busy retries requeue internally; no-answer is terminal and re-submitted externally via `place_call(schedule_at)`.

**Tech Stack:** Rust 2024, tokio, ezk-sip-ua/ezk-sip-types (`StatusCode::into_u16`), rmcp 3.1.2 (MCP + Tasks extension).

**Spec:** `docs/superpowers/specs/2026-08-15-call-outcomes-retry-design.md`

## Global Constraints

- Build/test ONLY with `cargo test --lib --features vendor-openssl`. Plain `cargo test` fails to link OpenSSL on this Windows host. Verify the live-tests build with `cargo test --no-run --features "vendor-openssl live-tests" --test live_smoke`.
- All in-repo text (code, comments, logs) is English.
- Times are UTC epoch **milliseconds** (`now_ms()` already exists in `engine.rs`). `schedule_at` is absolute; a past `schedule_at` means immediate (`eligible_at = now`), never an error.
- `max_concurrent_channels` (from `ServerConfig`) is the running-call cap; the scheduler enforces it (the old `tokio::Semaphore` is removed).
- New cumulative counters bump once at finalize.
- Retry defaults: `busy_max_attempts = 3`, `busy_retry_interval_ms = 300_000` (5 min).
- `CallState` stays coarse (a non-connected dial finalizes `Failed`); `CallOutcome` is the fine-grained discriminator. Do NOT add new `CallState` variants.

---

### Task 1: `CallOutcome` enum + `outcome_from_status` mapping

**Files:**
- Create: `src/sip/outcome.rs`
- Modify: `src/sip/mod.rs` (add `mod outcome; pub use outcome::CallOutcome;`)

**Interfaces:**
- Consumes: nothing.
- Produces: `enum CallOutcome { Completed, Busy, NoAnswer, Rejected, NotFound, Unavailable, Failed }` (derives `Clone, Copy, Debug, PartialEq, Eq, serde::Serialize`), `fn outcome_from_status(code: u16) -> CallOutcome`, `fn CallOutcome::retryable(&self) -> bool`.

- [ ] **Step 1: Write the failing tests**

Create `src/sip/outcome.rs`:

```rust
//! Structured dial result, parsed from the SIP response status code. This is
//! the fine-grained outcome of an attempt; `CallState` stays coarse.

/// Terminal outcome of one dial attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallOutcome {
    /// A connected call ended normally (model or caller hung up).
    Completed,
    /// 486 Busy Here.
    Busy,
    /// 408 request timeout, 480 temporarily unavailable, or ring timeout.
    NoAnswer,
    /// 603 decline, 403 forbidden, other 6xx.
    Rejected,
    /// 404 not found, 484 address incomplete.
    NotFound,
    /// 503 service unavailable, other 5xx.
    Unavailable,
    /// Technical failure (media, bridge, gemini) or an unmapped code.
    Failed,
}

impl CallOutcome {
    /// Transient outcomes worth an automatic or scheduled retry.
    pub fn retryable(&self) -> bool {
        matches!(self, CallOutcome::Busy | CallOutcome::NoAnswer)
    }
}

/// Map a SIP response status code to a `CallOutcome`.
pub fn outcome_from_status(code: u16) -> CallOutcome {
    match code {
        200..=299 => CallOutcome::Completed,
        404 | 484 => CallOutcome::NotFound,
        486 | 600 => CallOutcome::Busy,      // 486 Busy Here, 600 Busy Everywhere
        408 | 480 => CallOutcome::NoAnswer,
        403 | 603 => CallOutcome::Rejected,
        500..=599 => CallOutcome::Unavailable,
        _ => CallOutcome::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_codes() {
        assert_eq!(outcome_from_status(486), CallOutcome::Busy);
        assert_eq!(outcome_from_status(600), CallOutcome::Busy);
        assert_eq!(outcome_from_status(404), CallOutcome::NotFound);
        assert_eq!(outcome_from_status(484), CallOutcome::NotFound);
        assert_eq!(outcome_from_status(408), CallOutcome::NoAnswer);
        assert_eq!(outcome_from_status(480), CallOutcome::NoAnswer);
        assert_eq!(outcome_from_status(603), CallOutcome::Rejected);
        assert_eq!(outcome_from_status(403), CallOutcome::Rejected);
        assert_eq!(outcome_from_status(503), CallOutcome::Unavailable);
        assert_eq!(outcome_from_status(200), CallOutcome::Completed);
    }

    #[test]
    fn unmapped_code_is_failed() {
        assert_eq!(outcome_from_status(481), CallOutcome::Failed);
        assert_eq!(outcome_from_status(100), CallOutcome::Failed);
    }

    #[test]
    fn retryable_only_busy_and_no_answer() {
        assert!(CallOutcome::Busy.retryable());
        assert!(CallOutcome::NoAnswer.retryable());
        for o in [CallOutcome::Completed, CallOutcome::Rejected, CallOutcome::NotFound, CallOutcome::Unavailable, CallOutcome::Failed] {
            assert!(!o.retryable());
        }
    }
}
```

Add to `src/sip/mod.rs` near the other `mod`/`pub use` lines:
```rust
mod outcome;
pub use outcome::{outcome_from_status, CallOutcome};
```

- [ ] **Step 2: Run tests to verify they pass** (new pure code — no separate RED needed beyond confirming compile+pass)

Run: `cargo test --lib --features vendor-openssl sip::outcome`
Expected: PASS (3 tests).

- [ ] **Step 3: Commit**

```bash
git add src/sip/outcome.rs src/sip/mod.rs
git commit -m "feat(sip): CallOutcome + outcome_from_status SIP code mapping"
```

---

### Task 2: Classify the SIP termination into a `CallOutcome`

**Files:**
- Modify: `src/sip/mod.rs` (`TermReason::Failed` carries a `CallOutcome` + detail)
- Modify: `src/sip/call.rs` (`map_make_err` and the two `wait_for_completion`/`finish` error arms classify `StatusLine.code`)

**Interfaces:**
- Consumes: `outcome_from_status`, `CallOutcome` (Task 1).
- Produces: `TermReason::Failed { outcome: CallOutcome, detail: String }` (engine reads `outcome` in Task 3).

- [ ] **Step 1: Change `TermReason`**

In `src/sip/mod.rs` (`TermReason`, ~line 65):
```rust
#[derive(Debug, Clone)]
pub enum TermReason {
    /// The remote party (or trunk) ended the call.
    RemoteHangup,
    /// We ended the call via `SipCall::hangup()`.
    LocalHangup,
    /// The call failed before or during setup/teardown. `outcome` is the
    /// classified SIP result; `detail` is the human-readable status line.
    Failed { outcome: CallOutcome, detail: String },
}
```
(Add `use outcome::CallOutcome;` visibility as needed — it is already re-exported from this module.)

- [ ] **Step 2: Classify in `call.rs`**

`map_make_err` (`src/sip/call.rs:260`) currently returns a `SipError`. Add a classifier and use it in the two `Terminated(TermReason::Failed(...))` arms (`call.rs:143-145`, `151-153`). Both arms take a `MakeCallError`-typed `e`. Replace the `TermReason::Failed(e.to_string())` construction with:

```rust
let (outcome, detail) = classify_make_err(&e);
let _ = ev_tx.send(SipEvent::Terminated(TermReason::Failed { outcome, detail })).await;
```

Add the classifier (near `map_make_err`), using the real `MakeCallError` variants (`Failed(StatusLine)`, `Core(ezk_sip_core::Error::RequestTimedOut)`):
```rust
fn classify_make_err<M: std::fmt::Debug, A: std::fmt::Debug>(e: &ezk_sip_ua::MakeCallError<M, A>) -> (crate::sip::CallOutcome, String) {
    use crate::sip::{outcome_from_status, CallOutcome};
    match e {
        ezk_sip_ua::MakeCallError::Failed(line) => {
            let code = line.code.into_u16();
            (outcome_from_status(code), format!("{line:?}"))
        }
        ezk_sip_ua::MakeCallError::Core(ezk_sip_core::Error::RequestTimedOut) => {
            (CallOutcome::NoAnswer, "request timed out".into())
        }
        other => (CallOutcome::Failed, format!("{other:?}")),
    }
}
```
(Confirm the exact `MakeCallError` generic parameters against the existing `map_make_err` signature at `call.rs:260`; reuse the same `<A, M>` bounds it already declares. `StatusLine.code.into_u16()` is the numeric code — `ezk-sip-types` `StatusCode::into_u16`.)

Keep `map_make_err` for the immediate `reply.send(Err(...))` path (it stays a `SipError`); no outcome needed there since that path already routes to a technical `Failed` in the engine.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib --features vendor-openssl sip::`
Expected: PASS (existing sip tests still green; the `sipcall_split`/answered tests do not construct `TermReason::Failed`, but if any test builds it, update it to the struct form).

- [ ] **Step 4: Commit**

```bash
git add src/sip/mod.rs src/sip/call.rs
git commit -m "feat(sip): classify termination into CallOutcome from the status code"
```

---

### Task 3: `CallRecord` outcome/attempt/retry_of + engine finalize

**Files:**
- Modify: `src/state.rs` (`CallRecord` fields; `rec()` test helper; `set_quality`/insert literals)
- Modify: `src/engine.rs` (the answer-wait `Terminated` arm and finalize set `outcome`; drop the `"no answer:"` mislabel)

**Interfaces:**
- Consumes: `CallOutcome` (Task 1), `TermReason::Failed { outcome, .. }` (Task 2).
- Produces: `CallRecord { .., outcome: Option<CallOutcome>, attempt: u32, retry_of: Option<String> }`; `CallStore::finalize` gains an `outcome` parameter (see below).

- [ ] **Step 1: Add fields + thread `outcome` through `finalize`**

In `src/state.rs`, `CallRecord`:
```rust
    /// Structured dial result; None while in-flight.
    pub outcome: Option<crate::sip::CallOutcome>,
    /// 1 for the first dial; incremented per internal busy-retry.
    pub attempt: u32,
    /// The prior call_id this attempt continues (busy-retry or external retry_of).
    pub retry_of: Option<String>,
```
Update `CallStore::finalize` (`state.rs:88`) to take `outcome: Option<CallOutcome>` and store it (`r.outcome = outcome;`). Update every `finalize(...)` call site in `engine.rs` to pass an outcome (see Step 2). Update the `rec()`/insert test helpers and `set_state`/`insert` literals in `state.rs` and `engine.rs` to include `outcome: None, attempt: 1, retry_of: None` — build-driven.

- [ ] **Step 2: Engine sets the outcome**

In `src/engine.rs`:
- The place_call literal (`engine.rs:171`): add `outcome: None, attempt: 1, retry_of: None` (attempt is overwritten by the scheduler in Task 7; default 1 here).
- The dial-error path (`engine.rs:277`): `store.finalize(&call_id, CallState::Failed, None, Some(e.to_string()), Some(CallOutcome::Failed), now_ms());`
- The answer-wait `Terminated(reason)` arm (`engine.rs:289-290`): extract the outcome and drop the mislabel:
```rust
                Some(SipEvent::Terminated(reason)) => {
                    let (outcome, detail) = match reason {
                        crate::sip::TermReason::Failed { outcome, detail } => (outcome, detail),
                        // Remote/local hangup before answer is effectively no-answer.
                        _ => (CallOutcome::NoAnswer, format!("{reason:?}")),
                    };
                    store.finalize(&call_id, CallState::Failed, None, Some(detail), Some(outcome), now_ms());
                    bump_counter_outcome(&counters, outcome); // Task 4 helper; for now bump_counter(Failed)
                    return;
                }
```
- The `None` arm (`engine.rs:294`) and gemini-connect (`309`), bridge/finalize (`425`): pass `Some(CallOutcome::Failed)` (technical). The in-call terminal finalize (`~425`, `final_state`) passes `Some(CallOutcome::Completed)` when `final_state == Completed`, else `Some(CallOutcome::Failed)`.
- Note: Task 4 introduces `bump_counter_outcome`; until then use the existing `bump_counter` with the CallState. To keep this task self-contained, in Step 2 keep calling `bump_counter(&counters, CallState::Failed)` and let Task 4 replace those with outcome-aware bumps. Do NOT block Task 3 on Task 4.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib --features vendor-openssl state:: engine::`
Expected: PASS (fix any literal the compiler flags).

- [ ] **Step 4: Commit**

```bash
git add src/state.rs src/engine.rs
git commit -m "feat(state): CallRecord outcome/attempt/retry_of + finalize sets outcome"
```

---

### Task 4: Per-outcome counters + Prometheus

**Files:**
- Modify: `src/engine.rs` (`Counters`, `MetricsSnapshot`, `metrics_snapshot`, `bump_counter_outcome`)
- Modify: `src/mcp_http.rs` (`render_prometheus` series + test literals)

**Interfaces:**
- Consumes: `CallOutcome`.
- Produces: `MetricsSnapshot { .., busy_total, no_answer_total, rejected_total, not_found_total, unavailable_total: u64 }`.

- [ ] **Step 1: Write the failing test**

In `src/mcp_http.rs`, add `outcome_series_present` mirroring `prometheus_has_quality_series`: build a `MetricsSnapshot` with `busy_total: 5, no_answer_total: 4, ...` and assert `render_prometheus` contains `kutsu_calls_busy_total 5`, `kutsu_calls_no_answer_total 4`, etc.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl mcp_http::tests::outcome_series_present`
Expected: FAIL to compile (no field `busy_total`).

- [ ] **Step 3: Implement**

`engine.rs`: add `busy`, `no_answer`, `rejected`, `not_found`, `unavailable: AtomicU64` to `Counters`; the five `*_total: u64` to `MetricsSnapshot`; populate them in `metrics_snapshot()`. Add:
```rust
fn bump_counter_outcome(counters: &Counters, outcome: CallOutcome) {
    match outcome {
        CallOutcome::Completed => counters.completed.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Busy => counters.busy.fetch_add(1, Ordering::Relaxed),
        CallOutcome::NoAnswer => counters.no_answer.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Rejected => counters.rejected.fetch_add(1, Ordering::Relaxed),
        CallOutcome::NotFound => counters.not_found.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Unavailable => counters.unavailable.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Failed => counters.failed.fetch_add(1, Ordering::Relaxed),
    };
}
```
Replace the outcome-bearing `bump_counter(&counters, CallState::Failed)` sites from Task 3 with `bump_counter_outcome(&counters, outcome)`. Keep `bump_counter` for the Cancelled/technical paths that have no outcome, or pass `CallOutcome::Failed`.

`mcp_http.rs` `render_prometheus`: after `kutsu_calls_failed_total`, emit `kutsu_calls_busy_total`, `kutsu_calls_no_answer_total`, `kutsu_calls_rejected_total`, `kutsu_calls_not_found_total`, `kutsu_calls_unavailable_total` via the `g(...)` helper. Fix every `MetricsSnapshot` literal in mcp_http tests (build-driven).

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl engine:: mcp_http::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/mcp_http.rs
git commit -m "feat(engine): per-outcome cumulative counters + Prometheus series"
```

---

### Task 5: `RetryConfig` (config + env)

**Files:**
- Modify: `src/config.rs` (`RetryConfig` + `ServerConfig.retry`)
- Modify: `src/main_support.rs` (env)
- Modify: every `ServerConfig` literal (build-driven, ~9 sites — same set Task 5 of the uplink feature touched)

**Interfaces:**
- Produces: `ServerConfig.retry: RetryConfig { busy_max_attempts: u32, busy_retry_interval_ms: u64 }`.

- [ ] **Step 1: Add the config type + field**

In `src/config.rs`, mirroring `QualityConfig`:
```rust
/// Retry policy for transient dial outcomes.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Max dial attempts for a busy number (incl. the first). Default 3.
    pub busy_max_attempts: u32,
    /// Delay before a busy retry, ms. Default 300_000 (5 min).
    pub busy_retry_interval_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self { busy_max_attempts: 3, busy_retry_interval_ms: 300_000 }
    }
}
```
Add `pub retry: RetryConfig,` to `ServerConfig`.

- [ ] **Step 2: Env read**

In `src/main_support.rs`, next to the quality env reads, build a `RetryConfig` from `KUTSU_BUSY_MAX_ATTEMPTS` / `KUTSU_BUSY_RETRY_INTERVAL_MS` (parse with `.and_then(|s| s.parse().ok()).unwrap_or(default)`; follow the exact pattern the `QualityConfig` env read uses). Set `retry: <built>` on the `ServerConfig`.

- [ ] **Step 3: Fix literals**

Add `retry: RetryConfig::default(),` (or the built one in main_support) to every `ServerConfig { .. }` the compiler flags. Run `cargo build --lib --features vendor-openssl` and iterate.

- [ ] **Step 4: Verify**

Run: `cargo test --lib --features vendor-openssl config::` and `cargo test --no-run --features "vendor-openssl live-tests" --test live_smoke`.
Expected: PASS / compiles.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/main_support.rs src/main.rs src/engine.rs tests/live_smoke.rs
git commit -m "feat(config): RetryConfig (busy attempts/interval) + env"
```

---

### Task 6: `QueueStore` trait + in-memory impl

**Files:**
- Create: `src/engine/queue.rs` (or `src/queue.rs` if `engine` is a single file — put it beside the engine; if `engine.rs` is a flat file, create `src/queue.rs` and `mod queue;` in `main.rs`/`lib.rs`)
- Modify: the crate root to register the module.

**Interfaces:**
- Produces: `struct PendingEntry { call_id, number, scenario, eligible_at_ms, attempt, retry_of }`; `trait QueueStore { push, pop_eligible, peek_next_eligible_at, remove }`; `struct MemQueue` implementing it.

- [ ] **Step 1: Write the failing tests + types**

Create the module:
```rust
//! Pending-call queue for the engine scheduler. In-memory now; the trait is
//! the seam for a persistent (SQLite) backend later.
use crate::config::ScenarioConfig;

#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub call_id: String,
    pub number: String,
    pub scenario: ScenarioConfig,
    pub eligible_at_ms: u64,
    pub attempt: u32,
    pub retry_of: Option<String>,
}

pub trait QueueStore: Send {
    /// Enqueue an entry.
    fn push(&mut self, entry: PendingEntry);
    /// Remove and return the earliest entry whose `eligible_at_ms <= now_ms`,
    /// tie-broken by (eligible_at_ms, call_id). None if nothing is eligible.
    fn pop_eligible(&mut self, now_ms: u64) -> Option<PendingEntry>;
    /// The soonest `eligible_at_ms` of any pending entry (eligible or not).
    fn peek_next_eligible_at(&self) -> Option<u64>;
    /// Remove a pending entry by call_id (cancel before dispatch). Returns it if present.
    fn remove(&mut self, call_id: &str) -> Option<PendingEntry>;
    /// Count of pending entries.
    fn len(&self) -> usize;
    /// 1-based position of `call_id` in dispatch order among pending entries.
    fn position(&self, call_id: &str) -> Option<usize>;
}

/// In-memory `QueueStore`: a `BTreeMap` keyed by (eligible_at_ms, call_id).
#[derive(Default)]
pub struct MemQueue {
    // BTreeMap gives ordered iteration for pop/peek/position.
    entries: std::collections::BTreeMap<(u64, String), PendingEntry>,
}

impl MemQueue {
    pub fn new() -> Self { Self::default() }
}

impl QueueStore for MemQueue {
    fn push(&mut self, entry: PendingEntry) {
        self.entries.insert((entry.eligible_at_ms, entry.call_id.clone()), entry);
    }
    fn pop_eligible(&mut self, now_ms: u64) -> Option<PendingEntry> {
        let key = self.entries.range(..=(now_ms, String::from("\u{10FFFF}")))
            .next().map(|(k, _)| k.clone())?;
        self.entries.remove(&key)
    }
    fn peek_next_eligible_at(&self) -> Option<u64> {
        self.entries.keys().next().map(|(t, _)| *t)
    }
    fn remove(&mut self, call_id: &str) -> Option<PendingEntry> {
        let key = self.entries.iter().find(|(_, v)| v.call_id == call_id).map(|(k, _)| k.clone())?;
        self.entries.remove(&key)
    }
    fn len(&self) -> usize { self.entries.len() }
    fn position(&self, call_id: &str) -> Option<usize> {
        self.entries.values().position(|v| v.call_id == call_id).map(|i| i + 1)
    }
}
```

Tests (same file):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    fn entry(id: &str, at: u64) -> PendingEntry {
        PendingEntry { call_id: id.into(), number: "1".into(),
            scenario: ScenarioConfig { system_prompt: String::new(), goal_schema: serde_json::json!({}), context: None },
            eligible_at_ms: at, attempt: 1, retry_of: None }
    }
    #[test]
    fn pops_earliest_eligible_only() {
        let mut q = MemQueue::new();
        q.push(entry("b", 200));
        q.push(entry("a", 100));
        assert!(q.pop_eligible(50).is_none());          // nothing eligible yet
        assert_eq!(q.pop_eligible(150).unwrap().call_id, "a"); // only a is eligible
        assert!(q.pop_eligible(150).is_none());          // b not yet
        assert_eq!(q.pop_eligible(250).unwrap().call_id, "b");
    }
    #[test]
    fn peek_and_position_and_remove() {
        let mut q = MemQueue::new();
        q.push(entry("a", 100));
        q.push(entry("b", 200));
        assert_eq!(q.peek_next_eligible_at(), Some(100));
        assert_eq!(q.position("b"), Some(2));
        assert_eq!(q.remove("a").unwrap().call_id, "a");
        assert_eq!(q.position("b"), Some(1));
        assert_eq!(q.len(), 1);
    }
}
```
(Confirm `ScenarioConfig`'s real field names against `src/config.rs`; adjust the test constructor to match.)

- [ ] **Step 2: Run tests**

Run: `cargo test --lib --features vendor-openssl queue::`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/queue.rs src/main.rs   # + lib module registration
git commit -m "feat(engine): QueueStore trait + in-memory MemQueue"
```

---

### Task 7: Scheduler dispatcher replaces the semaphore

**Files:**
- Modify: `src/engine.rs` (remove the `Semaphore`; add the scheduler + dispatcher; `place_call` enqueues; `end_call` can remove a pending entry)

**Interfaces:**
- Consumes: `QueueStore`/`MemQueue`/`PendingEntry` (Task 6), `RetryConfig` (Task 5).
- Produces: `Engine::place_call(number, scenario) -> String` (unchanged signature; now enqueues), `Engine::place_call_at(number, scenario, eligible_at_ms, retry_of) -> String` (new, for scheduled + retry), a running dispatcher.

**Design (integration — implement against the real engine code):**
- Replace `permits: Arc<tokio::sync::Semaphore>` with `queue: Arc<Mutex<Box<dyn QueueStore>>>` (holding a `MemQueue`) and a `running: Arc<AtomicUsize>` plus a `tokio::sync::Notify` (`wake`) the dispatcher waits on.
- `place_call_at` builds a `PendingEntry` (with `eligible_at_ms`), inserts the `CallRecord` as `Queued` (with `attempt`, `retry_of`), pushes to the queue, and `wake.notify_one()`. `place_call` = `place_call_at(number, scenario, now_ms(), None)`.
- A dispatcher task (spawned in `Engine::new`, holds `engine` handles via `Arc`) loops:
  1. Compute `sleep_until` = if `running < cap` then `queue.peek_next_eligible_at()` else `None`.
  2. `tokio::select!` on `wake.notified()` OR (if `sleep_until` is `Some(t)`) a timer to `t` (use `tokio::time::sleep(Duration::from_millis(t.saturating_sub(now_ms())))`), OR a `slot_freed` notify.
  3. On wake: while `running < cap` and `queue.pop_eligible(now_ms())` yields an entry: set the record `Ringing`, `running += 1`, `tokio::spawn(run_call(entry, ...))`; the spawned task decrements `running` and `wake.notify_one()` on completion.
- `run_call` loses the `permits.acquire_owned()` block (`engine.rs:264-271`) and the cancel-before-permit race; cancellation of a *pending* call is now handled by `end_call` removing it from the queue. `run_call` takes the `PendingEntry`'s fields (number/scenario/attempt/retry_of) instead of the old params.
- `end_call(call_id)`: first try `queue.remove(call_id)` — if present, finalize `Cancelled` immediately (never dispatched); else fall through to the existing running-call cancel signal.
- `queued_position` (`state.rs`) delegates to `queue.position(call_id)` (real order) — thread the queue handle or move the position query to the engine. Simplest: add `Engine::queued_position(call_id)` that reads the queue; keep the store method for display of non-scheduler callers or drop it. Pick one and note it.
- `metrics_snapshot` `queued` count comes from `queue.len()`; `active` from `running`.

- [ ] **Step 1: Write the failing test**

Add an engine test `scheduler_respects_cap_and_eligibility` (use `tokio::time::pause()`): with `max_concurrent_channels = 1`, enqueue two immediate calls (to a number that will not connect in the test harness — reuse the existing engine test scaffolding that asserts `Queued`/state without a live SIP stand) and assert only one leaves `Queued` at a time; enqueue one with a future `eligible_at` and assert it stays `Queued` until time advances past it. Mirror the construction of the existing `place_call_queues_when_at_cap` test (`engine.rs:512`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features vendor-openssl engine::tests::scheduler_respects_cap_and_eligibility`
Expected: FAIL (dispatcher/`place_call_at` not present).

- [ ] **Step 3: Implement the scheduler** per the design above.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl engine::`
Expected: PASS, including the existing `place_call_queues_when_at_cap` and cancel tests (adapt them if they assumed the semaphore).

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): scheduler dispatcher (eligible_at queue) replaces the semaphore"
```

---

### Task 8: Busy internal retry

**Files:**
- Modify: `src/engine.rs` (`run_call` requeues on Busy; `should_retry_busy` pure helper)

**Interfaces:**
- Consumes: scheduler `place_call_at`/requeue (Task 7), `RetryConfig` (Task 5), `CallOutcome` (Task 1).
- Produces: `fn should_retry_busy(attempt: u32, cfg: &RetryConfig) -> bool`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn should_retry_busy_stops_at_max() {
    let cfg = crate::config::RetryConfig { busy_max_attempts: 3, busy_retry_interval_ms: 1000 };
    assert!(should_retry_busy(1, &cfg));
    assert!(should_retry_busy(2, &cfg));
    assert!(!should_retry_busy(3, &cfg)); // 3rd attempt is the last; no 4th
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib --features vendor-openssl engine::tests::should_retry_busy_stops_at_max`
Expected: FAIL (no `should_retry_busy`).

- [ ] **Step 3: Implement**

```rust
fn should_retry_busy(attempt: u32, cfg: &RetryConfig) -> bool {
    attempt < cfg.busy_max_attempts
}
```
In `run_call`, when the classified outcome is `CallOutcome::Busy`: finalize THIS record as `Failed`/`Busy` (with `outcome = Busy`), and if `should_retry_busy(attempt, &server.retry)`, enqueue a follow-up via `place_call_at(number.clone(), scenario.clone(), now_ms() + server.retry.busy_retry_interval_ms, Some(call_id.clone()))` with `attempt + 1` (extend `place_call_at` to accept `attempt`, or add `requeue_busy` that sets it). The follow-up gets a fresh `call_id`; the finalized record is linked forward by the follow-up's `retry_of`. Do NOT bump `busy_total` for the intermediate attempts differently — each finalized attempt bumps `busy_total` once (a 3-attempt busy call bumps busy_total 3×, which is correct: three busy responses occurred). Note this in the commit.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl engine::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): internal busy retry (requeue with interval, capped, retry_of chain)"
```

---

### Task 9: MCP `schedule_at`/`retry_of` + bypass task branch + `server_time_unix`

**Files:**
- Modify: `src/mcp.rs` (`PlaceCallArgs`, `place_call_inner`, responses)

**Interfaces:**
- Consumes: `Engine::place_call_at` (Task 7).

- [ ] **Step 1: Write the failing test**

Add an mcp test `scheduled_call_takes_immediate_branch`: mirror the existing capability-branch test; call `place_call_inner` with `schedule_at = Some(future_ms)` and `client_supports_tasks = true`, assert the result is the immediate `{call_id, server_time_unix}` shape (not the task/poll flavor). Also assert `server_time_unix` is present in the immediate response.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib --features vendor-openssl mcp::tests::scheduled_call_takes_immediate_branch`
Expected: FAIL to compile (no `schedule_at` field).

- [ ] **Step 3: Implement**

`PlaceCallArgs`:
```rust
    /// Optional UTC epoch ms to place the call at. Past/absent = immediate.
    #[serde(default)]
    schedule_at: Option<u64>,
    /// Optional prior call_id this is a manual retry of.
    #[serde(default)]
    retry_of: Option<String>,
```
`place_call_inner`:
- Compute `let now = now_ms();` (reuse `crate::engine::now_ms` — make it `pub(crate)` if not already). `let eligible = a.schedule_at.filter(|t| *t > now).unwrap_or(now);` `let scheduled = eligible > now;`
- Call `self.engine.place_call_at(a.to_number, scenario, eligible, a.retry_of)` instead of `place_call`.
- **Branch:** `if scheduled || !client_supports_tasks { return immediate; }` — the immediate JSON becomes `{ "call_id": call_id, "server_time_unix": now }`. The task/poll flavor runs ONLY for immediate + task-capable clients (unchanged body, but its final results should also include `server_time_unix`).
- `get_call_status` result JSON gains `server_time_unix: now_ms()`.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl mcp::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs
git commit -m "feat(mcp): schedule_at/retry_of on place_call, bypass task branch, server_time_unix"
```

---

### Task 10: Whole-feature verification

**Files:** none.

- [ ] **Step 1:** `cargo test --lib --features vendor-openssl` — full suite green.
- [ ] **Step 2:** `cargo test --no-run --features "vendor-openssl live-tests" --test live_smoke` — compiles.
- [ ] **Step 3:** `cargo build --lib --features vendor-openssl` — no warnings.
- [ ] **Step 4:** Manual note (no commit): a busy number retries up to 3× at 5 min; a no-answer finalizes `NoAnswer`; `place_call(schedule_at=<future>)` returns immediately with `call_id`+`server_time_unix` and dials at the scheduled time; `/metrics` shows the per-outcome series.
- [ ] **Step 5:** Commit any verification fixups if needed.
