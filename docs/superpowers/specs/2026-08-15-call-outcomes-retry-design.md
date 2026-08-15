# Call outcomes + retry scheduler — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-15
**Scope:** `src/sip/mod.rs` + `src/sip/call.rs` (structured `CallOutcome` from the SIP status code), `src/state.rs` (`CallRecord.outcome`/`retry_of`/`attempt`), `src/engine.rs` (scheduler replacing the raw semaphore; busy internal retry), `src/config.rs` + `src/main_support.rs` (`RetryConfig`), `src/mcp.rs` (`place_call` gains `schedule_at`/`retry_of`; scheduled calls bypass the task-poll branch; response carries `server_time_unix`), `src/mcp_http.rs` (per-outcome Prometheus series).

## Problem

Today every non-answer dial result collapses to `CallState::Failed` with a free-text `error` string (`engine.rs:277,290`). Busy, no-answer, wrong-number, and technical failures are indistinguishable by state or metric — all bump `failed_total`. The `error` string even mislabels a `486 Busy`/`404 Not Found` as `"no answer:"` (`engine.rs:290`, debug-formatted). There is also **no queue and no retry**: "Queued" is emergent — `place_call` spawns `run_call` which parks on a `tokio::Semaphore` (`engine.rs:265`); `queued_position()` (`state.rs:126`) is a cosmetic sort with no effect on dispatch order. A failed call's `run_call` simply returns — nothing re-enqueues it.

For an outbound calling system, the retry policy must differ by outcome: **busy** and **no-answer** are transient (retryable); **wrong-number**/reject is permanent. Busy should retry on a simple internal schedule; no-answer's callback time is a business decision owned by the external agent/CRM.

## Decisions

- **Structured outcome, not string.** `MakeCallError::Failed(StatusLine)` (ezk-sip-ua 0.9.1) carries `StatusLine.code: StatusCode` — the numeric SIP code is available directly (no Debug-string parsing). `MakeCallError::Core(RequestTimedOut)` is the ring-timeout. We map these to a `CallOutcome` enum.
- **Scheduler lives in the engine, never on the MCP Tasks extension.** The Tasks extension (`mcp.rs`, SEP-2663) is a *per-call client-polling* wrapper and is **capability-gated** (`client_supports_tasks`, `mcp.rs:132`); building retry there would make retry work only for task-capable clients. Retry is a server-side operational concern that must be client-independent.
- **Hybrid ownership.** KUTSU owns **busy** retry internally (requeue to tail, min interval, capped attempts). **No-answer** is terminal for KUTSU; the external agent picks the callback time and re-submits via a plain tool parameter.
- **In-memory now, behind a `QueueStore` trait** so a persistent backend (SQLite) can be added later without touching the scheduler logic.
- **Absolute time only.** `schedule_at` is UTC epoch milliseconds (agent converts its local/business-hours logic to UTC). Clock agreement is made checkable: every `place_call` response and `get_call_status` carries `server_time_unix`. NTP is assumed (standard for a telephony host; sub-second drift is negligible against minute-granularity scheduling). No relative `delay_secs` field (dropped for API simplicity).
- **`retry_of` from the start** — the attempt chain is first-class, not a later addition.

## Component 1 — `CallOutcome` (structured dial result)

New enum in `src/sip/mod.rs`, replacing the free-text carried by `TermReason`:

```
CallOutcome:
  Completed        // model/caller ended a connected call (maps from the in-call terminal states)
  Busy             // 486
  NoAnswer         // 408 request timeout, 480 temporarily unavailable, ring-timeout (Core(RequestTimedOut))
  Rejected         // 603 decline, 403 forbidden, 6xx
  NotFound         // 404, 484 address incomplete
  Unavailable      // 503 service unavailable, 500-599 (server-side)
  Failed           // technical: media, bridge panic, gemini connect, or unmapped code
```

`TermReason` (`sip/mod.rs:65`) stops carrying a `String` and carries the SIP status code (or a `NoAnswer`/`Failed` marker). `map_make_err` (`call.rs:260`) and the `wait_for_completion`/`finish` error arms (`call.rs:143,153`) classify `StatusLine.code` into the enum. A helper `outcome_from_status(code: StatusCode) -> CallOutcome` holds the numeric mapping and is unit-tested per code.

`CallOutcome` also decides **retry class**: `Busy` and `NoAnswer` are `retryable()`; `Rejected`/`NotFound`/`Unavailable`/`Failed`/`Completed` are not (their retry, if any, is a fresh externally-scheduled call, not an automatic one).

## Component 2 — record fields + observability

`state::CallRecord` gains:
- `outcome: Option<CallOutcome>` — set at finalize; `None` while in-flight. Serialized in `get_call_status` JSON.
- `attempt: u32` — 1 for the first dial, incremented on each internal busy-retry.
- `retry_of: Option<String>` — the predecessor `call_id` this attempt continues (internal busy-retry or external `retry_of`).

`engine::Counters` + `MetricsSnapshot` gain per-outcome cumulative counters; `render_prometheus` emits `kutsu_calls_busy_total`, `kutsu_calls_no_answer_total`, `kutsu_calls_rejected_total`, `kutsu_calls_not_found_total`, `kutsu_calls_unavailable_total` (the existing `failed_total` now counts only technical `Failed`). The per-call log line gains `outcome` + `attempt`.

## Component 3 — the scheduler (replaces the semaphore)

A `Scheduler` owned by the engine replaces spawn-and-park-on-semaphore:

- **Pending entry:** `{ call_id, number, scenario, eligible_at_ms, attempt, retry_of }`.
- **`QueueStore` trait** (seam for persistence): `push(entry)`, `pop_eligible(now_ms) -> Option<entry>` (earliest `eligible_at ≤ now`, FIFO tie-break by `(eligible_at, call_id)`), `peek_next_eligible_at() -> Option<u64>`, `remove(call_id)` (for cancel). Default impl: in-memory `BinaryHeap`/`BTreeMap` keyed by `(eligible_at_ms, call_id)`. Not persisted yet.
- **Dispatcher loop:** a single engine task holding a `running: usize` count (≤ `max_concurrent_channels`). It wakes on (a) a new push, (b) a running slot freeing, or (c) a timer set to `peek_next_eligible_at()`. On wake, while `running < cap` and an entry is eligible, it pops and spawns `run_call` for it, `running += 1`; `run_call` signals completion to decrement `running`.
- `place_call` enqueues (immediate = `eligible_at = now`; scheduled = `eligible_at = schedule_at`) instead of spawning directly. `queued_position()` is reimplemented against the `QueueStore` order so it reflects **real** dispatch order.
- Cancel (`end_call`) removes a not-yet-dispatched entry from the store, or signals the running call as today.

`max_concurrent_channels` semantics are preserved (now enforced by the dispatcher's `running` cap rather than the semaphore).

## Component 4 — busy internal retry

When `run_call` reaches `CallOutcome::Busy` and `attempt < busy_max_attempts`:
- it does **not** finalize; it calls `scheduler.requeue(entry)` with `eligible_at = now + busy_retry_interval_ms`, `attempt += 1`, `retry_of = <this call_id>`, and a **new** `call_id` for the new attempt (the old record finalizes as `Busy` with its `outcome`, linked forward by the new record's `retry_of`).
- Placing the requeued entry at the tail with an `eligible_at` floor satisfies "to the end of the queue, but not before the interval."
- On `attempt == busy_max_attempts`, the call finalizes `Busy` terminally (no requeue).

`RetryConfig` (`config.rs`, env like `QualityConfig`): `busy_max_attempts` (default **3**), `busy_retry_interval_ms` (default **300000** = 5 min). Env `KUTSU_BUSY_MAX_ATTEMPTS`, `KUTSU_BUSY_RETRY_INTERVAL_MS`.

No-answer is **not** auto-retried: it finalizes `NoAnswer` terminally. The external agent schedules the callback (Component 5).

## Component 5 — MCP surface: scheduled calls + calibration

`place_call` args gain:
- `schedule_at: Option<u64>` — UTC epoch ms. If set and in the future, the call is enqueued with `eligible_at = schedule_at`.
- `retry_of: Option<String>` — the prior `call_id` this is a manual retry of (recorded on the new call).

**Scheduled calls bypass the task-poll branch.** In `place_call_inner` (`mcp.rs:140`), if `schedule_at` is set (future), take the **immediate-return** branch (return `call_id` now) **regardless of `client_supports_tasks`** — a call that starts in 30 min must not be modeled as an MCP task the harness polls to completion (the session won't outlive the wait). The task/poll flavor remains only for immediate calls, where it now polls across any internal busy-retries until a truly terminal outcome.

Every `place_call` response and `get_call_status` result carries `server_time_unix: u64` (server's UTC epoch ms at response time) so the client can measure and correct clock skew before computing a `schedule_at`.

## Error handling & edge cases

- `schedule_at` in the past → treat as immediate (`eligible_at = now`); do not reject (clock skew tolerance).
- Unmapped/unexpected SIP code → `CallOutcome::Failed` (technical), not a silent drop; the numeric code is logged.
- A requeued busy call that is cancelled while pending → removed from the `QueueStore` (never dispatched), finalized `Cancelled`.
- `retry_of` pointing at an unknown `call_id` → accepted and recorded as-is (the chain is advisory metadata, not a validated FK); logged at debug.
- Dispatcher must not busy-spin: when nothing is eligible it sleeps until `peek_next_eligible_at()` (or indefinitely until the next push/slot-free wake).
- Shutdown drains: pending scheduled entries are dropped on process exit (in-memory) — documented as the known limitation the `QueueStore` seam exists to remove later.

## Testing

- `outcome_from_status`: table test mapping 486→Busy, 404/484→NotFound, 408/480→NoAnswer, 603/403→Rejected, 503→Unavailable, 200→Completed, an unmapped code→Failed.
- `QueueStore` in-memory impl: eligibility ordering (earliest eligible first), FIFO tie-break, `pop_eligible` respects `now`, `peek_next_eligible_at`, `remove`.
- Scheduler dispatch: respects `max_concurrent_channels` (N eligible, cap M → only M running); a not-yet-eligible entry is not dispatched until its time; `queued_position` reflects store order.
- Busy retry: an entry hitting `Busy` requeues with `eligible_at = now + interval`, `attempt += 1`, `retry_of` set; stops at `busy_max_attempts`; the pure requeue-decision (`should_retry_busy(attempt, cfg)`) is unit-tested without a live call.
- MCP: `schedule_at` in the future takes the immediate branch even for a task-capable client (mirrors the existing capability-branch test); `server_time_unix` present in responses; `schedule_at` in the past → immediate.

## Extension seams (out of scope now)

- **Persistent `QueueStore`** (SQLite) — the trait is defined for it; not implemented.
- **No-answer smart scheduling inside KUTSU** (business hours, contact preferences) — deliberately external; KUTSU only accepts `schedule_at`.
- **Per-number rate limiting / campaign concurrency** — not modeled here.
- **Callback/webhook on terminal outcome** — the external agent polls `get_call_status`; push notification is a later concern.
