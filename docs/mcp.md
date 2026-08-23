# MCP server

This guide is for **agent integrators** driving kutsu over the Model Context
Protocol. kutsu is an MCP server that places outbound phone calls and bridges
them to Gemini Live.

For config, see [configuration.md](configuration.md); for the call objective
model, see [prompts.md](prompts.md).

## Running the server

```bash
kutsu mcp                                   # stdio (default)
kutsu mcp --transport streamable-http       # HTTP on 127.0.0.1:8090
kutsu mcp --transport streamable-http --bind 0.0.0.0:8090 --auth-token "$TOKEN"
```

Equivalent environment variables: `KUTSU_MCP_TRANSPORT`, `KUTSU_MCP_BIND`,
`KUTSU_MCP_TOKEN`. The server needs `GEMINI_API_KEY` and, for real calls, the
SIP settings and `KUTSU_SIP_PASS` (see [configuration.md](configuration.md)).

## Tools

| Tool | Arguments | Returns |
|------|-----------|---------|
| `place_call` | `to_number`, `goal_schema`, `context?`, `prompt_override?`, `schedule_at?` | `call_id` (immediately) |
| `get_call_status` | `call_id` | State, disposition, filled goal, attempt count (no transcript) |
| `get_call_transcript` | `call_id` | Transcript, filled goal, disposition |
| `end_call` | `call_id` | `{call_id, signalled}` (outcome via `get_call_status`) |

Enumerating all calls is intentionally **not** a tool: the client that placed a
call holds its `call_id` and polls that one call (mirroring the MCP Tasks
extension, which dropped `tasks/list`). A global listing is an operational
concern — see [ops endpoints](#ops-endpoints).

### `place_call`

| Argument | Type | Required | Meaning |
|----------|------|----------|---------|
| `to_number` | string | Yes | Callee number in E.164, e.g. `+15551234567`. |
| `goal_schema` | JSON Schema | Yes | The call's objective **and** the `end_call` output shape. Injected into the prompt — write good field `description`s. See [prompts.md](prompts.md). |
| `context` | object | No | Lead/contact data merged into the prompt. |
| `prompt_override` | string | No | Replace the base persona for this call only. Absent = the deployment's `base_system_prompt`. |
| `schedule_at` | epoch ms | No | Place the call at this time; past/absent = immediately. |

`place_call` returns a `call_id` immediately — the call runs in the background
(calls last minutes; a synchronous tool call would time out). For **task-capable
clients** it runs as an MCP task (poll `tasks/get`, which carries a queue-adapted
`pollIntervalMs` hint, for the result on completion); for plain clients, poll
`get_call_status` until `ended` — it carries the disposition and filled goal, so
`get_call_transcript` is only needed for the full transcript.

Example arguments:

```jsonc
{
  "to_number": "+15551234567",
  "goal_schema": {
    "type": "object",
    "required": ["disposition"],
    "properties": {
      "disposition": { "type": "string", "enum": ["confirmed", "declined"],
        "description": "Did the customer confirm the appointment?" }
    }
  },
  "context": { "name": "Alex Carter", "appointment": "2026-09-01 14:00" }
}
```

### Call lifecycle

`state` is the coarse lifecycle; the resolved **`disposition`** (set once the
call ends) is the actual outcome:

```
place_call -> queued -> ringing -> in_progress -> ended
```

`get_call_status` returns `state`, `disposition` (null until `ended`), the
filled `goal`, and the dial `attempt` count. Poll until `state` is `ended`, then
read `disposition`; fetch `get_call_transcript` only when you need the full
turn-by-turn transcript.

**Dispositions:** `completed` (talked to a person, goal collected); `voicemail`
/ `announcement` / `ivr` / `hold` (answered a machine / carrier recording / menu
/ hold); `busy`, `no_answer`, `rejected`, `not_found`, `unavailable` (dial
outcomes); `failed` (technical); `cancelled`.

**Retries** are the caller's decision, except `busy` — kutsu retries that
automatically under the **same `call_id`** (`attempt` increments). For other
outcomes, call `place_call` again (optionally with `schedule_at`) if you want a
retry.

**Wrap-up** — If the model falls silent while the callee remains on the line, or
if the callee hangs up mid-conversation before the model submits the result, kutsu
injects a cue asking the model to finish via `end_call` (harvesting the disposition
and goal). The model's audio to the callee is muted during wrap-up. This phase is
bounded by `wrap_up_grace_ms` and gated by `dead_air_nudge_ms` (0 disables the
dead-air nudge).

## Ops endpoints

On the `streamable-http` transport, three unauthenticated endpoints are exposed
alongside the token-gated `/mcp` (they carry only counts, no PII, so ops tooling
can scrape them without a header):

| Endpoint | Purpose |
|----------|---------|
| `/health` | Liveness — always `200 ok`. |
| `/ready` | Readiness — `200 ready`. |
| `/metrics` | Prometheus exposition (call counts incl. per-disposition, active/queued, audio quality, uplink RTP). |

Only `/mcp` is gated by `KUTSU_MCP_TOKEN` (bearer, constant-time compared).

## Client integration

To register kutsu in Claude Code, Claude Desktop, Cursor, VS Code, Windsurf,
Cline, or a generic MCP client — and to install the `place-call` skill — see
[mcp-integration.md](mcp-integration.md).
