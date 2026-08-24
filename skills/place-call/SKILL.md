---
name: place-call
description: Use when you need to phone a real person — place an outbound voice call through the kutsu MCP server, which dials over SIP and bridges the call to a Gemini Live voice agent, then collect the result. Real, paid calls.
---

# place-call — outbound phone calls via kutsu

Place a real phone call: kutsu dials over a SIP trunk and bridges a Gemini Live
voice agent onto the call to carry the conversation toward a goal. Tools exposed
by the kutsu MCP server: `place_call`, `get_call_status`, `get_call_transcript`,
`end_call`.

**Calls are real and cost money.** Numbers are E.164 (`+<country><number>`).
Only call when the task genuinely requires it.

## Place a call

`place_call`:
- `to_number` — E.164, e.g. `+14155550123`.
- `goal_schema` — a JSON Schema of what to collect. **Field descriptions are the
  call's objective** (the agent sees them and steers the conversation). The
  top-level type must be `object`.
- `context` (optional) — an object of lead/context data (name, reason) merged
  into the prompt.
- `prompt_override` (optional) — per-call persona override.
- `schedule_at` (optional) — UTC epoch ms to place the call at (past/absent =
  now).
- `client_ref` (optional, **use it by default**) — an opaque tag echoed back
  verbatim in `get_call_status`, `get_call_transcript`, and the task result. Set
  it to your own identifier for this call (lead id, order id, or intent) so you
  can match each result back to its request **without relying on `call_id`**.
  Essential when placing several calls at once — especially to the same number
  with different goals — where `call_id` order is not guaranteed. kutsu never
  interprets it.

Example `goal_schema`:
```json
{ "type": "object",
  "properties": {
    "interested": { "type": "boolean", "description": "Whether the person is interested in the offer" },
    "callback_at": { "type": "string", "description": "When to call back if they are busy now" } },
  "required": ["interested"] }
```
Returns a `call_id`.

## Track the call

Poll `get_call_status(call_id)` until `state` is `ended`:
- `state`: `queued` → `ringing` → `in_progress` → `ended` (lifecycle).
- `disposition`: the resolved outcome (see below); `null` while live.
- `goal`: the filled goal (collected data); `null` until the agent submits it.
- `attempt`: how many dials happened (kutsu auto-retries `busy` under the same
  `call_id`).
- `client_ref`: the tag you passed to `place_call`, echoed back verbatim — match
  the result to its request by this, not by `call_id` order.

The full turn-by-turn transcript is `get_call_transcript(call_id)` (heavier;
fetch only when you need it).

Task-capable MCP clients may instead run `place_call` as an MCP task and read
its result on completion, rather than polling.

## Dispositions

- `completed` — talked to a person, goal collected.
- `voicemail` / `announcement` / `ivr` / `hold` — reached voicemail / a carrier
  recording / an auto-attendant / hold.
- `busy` — line busy. `no_answer` — not picked up. `unavailable` —
  network/service unavailable.
- `rejected` — the callee declined. `not_found` — the number does not exist.
- `cancelled` — the call was cancelled. `failed` — a technical failure.

## Retries are your decision

kutsu **auto-retries only `busy`** (under the same `call_id`). Everything else
is your call:
- `no_answer` / `unavailable` / `voicemail` — you may retry later: `place_call`
  with `schedule_at` (UTC epoch ms).
- `rejected` / `not_found` — do **not** retry (declined / no such number).
- `completed` — done.

The number of dials so far is in `attempt`.

## Stop a call

`end_call(call_id)` — hang up / drop from the queue. Returns `signalled`; read
the outcome from `get_call_status`.

## Rules
- Numbers must be E.164; an empty/invalid number is rejected.
- Do not call without a clear task-driven need — calls are billed.
- Take the outcome and collected data from `disposition` + `goal` (one
  `get_call_status`); fetch the transcript only when needed.
