# Changelog

All notable changes to kutsu are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
semantic versioning (pre-1.0: minor = notable/breaking, patch = fixes).

## [0.2.0] - 2026-08-23

### Added
- A call **wrap-up** phase that nudges the model to `end_call` on dead air or
  after an abrupt callee hangup (downlink muted), harvesting the disposition and
  goal; configurable via `dead_air_nudge_ms` and `wrap_up_grace_ms`.
- A `model-immediate-response` `_meta` hint on the `place_call` task result:
  task-capable clients get call context (number, call_id, queue position) to hand
  the model immediately while the task runs, instead of a generic host stub.
- Unified call **`Disposition`** model (12 outcomes: `completed`, `voicemail`,
  `announcement`, `ivr`, `hold`, `busy`, `no_answer`, `rejected`, `not_found`,
  `unavailable`, `failed`, `cancelled`), resolved from AMD → SIP → call-shape and
  surfaced on `get_call_status`, `get_call_transcript`, the `place_call` task
  result, and per-disposition Prometheus counters.
- In-session **prompt AMD** (answering-machine detection): an `amd` field is
  injected into the goal schema so the agent reports what it reached; plus an
  offline AMD harness, a Silero neural-VAD backend (`amd-silero` feature), and
  the `amd-eval` dev binary.
- **Setup-aware network preflight** — validates the real Gemini setup (not just
  ping RTT) before dialing — and mid-call uplink/downlink RTP-loss abort gates.
- `attempt` dial count and a configurable, **queue-adaptive `place_call` task
  poll interval** (`mcp_poll_interval_ms` / `mcp_poll_interval_max_ms`).
- `log`→`tracing` bridge exposing SIP call-progress (`sip_progress` target,
  incl. 18x provisionals and early-media presence).
- A ready-to-install **`place-call` agent skill** and an **MCP client
  integration guide** (Claude Code/Desktop, Cursor, VS Code, Windsurf, Cline).

### Changed
- **Breaking (MCP surface):** `get_call_status` now returns `disposition`,
  `goal`, and `attempt`; `get_call_transcript` adds `disposition`; `end_call`
  returns `{call_id, signalled}` (the stale pre-teardown `state` snapshot was
  dropped); `place_call` dropped the `retry_of` argument.
- `CallState` slimmed to the lifecycle `queued`/`ringing`/`in_progress`/`ended`;
  the outcome now lives in `disposition`.
- `busy` calls auto-retry under the **same `call_id`** (with `attempt`
  incrementing) instead of minting a new one. Only `busy` is auto-retried; other
  outcomes are the caller's decision (via `place_call` + `schedule_at`).
- `kutsu call` exits non-zero for any dial failure (busy/no_answer/rejected/…).

### Fixed
- The WS-1007 "gemini setup rejected" class: kutsu now sanitizes `goal_schema`
  to Gemini's function-parameter Schema subset — forces `type: object`, strips
  unsupported keys (`additionalProperties`, `$schema`, …), parses a stringified
  schema, and folds `type: [T, "null"]` to `type` + `nullable`.
- The crate's reconnect loop terminates on a non-retryable setup close
  (1002/1003/1007) instead of storming forever.
- `announcement` disposition for a carrier fast-disconnect that hangs up first
  (caller-hangup), and a stale abort-reason error on calls that raced to
  `completed`.

## [0.1.0]

- Initial release: outbound SIP calling MCP server bridging phone calls to
  Gemini Live (place/status/transcript/end tools, streamable-http + stdio,
  ops endpoints, real Novofon trunk validated).
