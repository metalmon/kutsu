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
| `place_call` | `to_number`, `goal_schema`, `context?`, `prompt_override?`, `schedule_at?`, `retry_of?` | `call_id` (immediately) |
| `get_call_status` | `call_id` | Current call state |
| `get_call_transcript` | `call_id` | Transcript + filled goal |
| `end_call` | `call_id` | Signals teardown |

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
| `retry_of` | string | No | Prior `call_id` this is a manual retry of. |

`place_call` returns a `call_id` immediately — the call runs in the background
(calls last minutes; a synchronous tool call would time out). For **task-capable
clients** it runs as an MCP task (poll `tasks/get` for the transcript + goal on
completion); for plain clients, poll `get_call_status` until terminal, then
`get_call_transcript`.

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
  "context": { "name": "Ivan", "appointment": "2026-09-01 14:00" }
}
```

### Call lifecycle

```
place_call -> Queued -> Ringing -> InProgress -> Completed | HungUp | Failed | Cancelled
```

Poll `get_call_status` until the state is terminal
(`Completed`/`HungUp`/`Failed`/`Cancelled`), then `get_call_transcript` for the
transcript and the filled `goal`.

## Ops endpoints

On the `streamable-http` transport, three unauthenticated endpoints are exposed
alongside the token-gated `/mcp` (they carry only counts, no PII, so ops tooling
can scrape them without a header):

| Endpoint | Purpose |
|----------|---------|
| `/health` | Liveness — always `200 ok`. |
| `/ready` | Readiness — `200 ready`. |
| `/metrics` | Prometheus exposition (call counts, active/queued, audio quality, uplink RTP). |

Only `/mcp` is gated by `KUTSU_MCP_TOKEN` (bearer, constant-time compared).

## Local IDE config

stdio server for a local client (Claude Desktop, Cursor, …):

```json
{
  "mcpServers": {
    "kutsu": {
      "command": "kutsu",
      "args": ["mcp"],
      "env": {
        "GEMINI_API_KEY": "...",
        "KUTSU_CONFIG": "/etc/kutsu/kutsu.toml",
        "KUTSU_SIP_PASS": "..."
      }
    }
  }
}
```
