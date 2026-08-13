<p align="center">
  <img src="docs/assets/logo.svg" alt="kutsu" width="300"/>
</p>

<p align="center">
  Phone calls as an MCP tool: any SIP trunk, a realtime voice model on the line.<br/>
  One Rust binary. <a href="https://ai.google.dev/gemini-api/docs/live">Gemini Live</a> first, OpenAI Realtime next.
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#mcp-tools">MCP tools</a> ·
  <a href="#status">Status</a> ·
  <a href="LICENSE">MIT</a>
</p>

*Kutsu* — Finnish for "a call / an invitation." Any MCP client (an agent, an IDE, another tool) calls `place_call` to have kutsu dial a number and run a scripted conversation end to end — the model owns turn-taking, barge-in, and its own tool-calling for the duration of the call — then polls for status and collects the [call outcome](#call-outcomes): transcript, a filled goal JSON, and the audio recording.

## Why kutsu

- **Generic SIP, not a vendor API** — works with any SIP trunk provider or self-hosted PBX; no Twilio-style lock-in.
- **Async by design** — `place_call` returns a `call_id` immediately; the call runs in the background and separate tools poll/control it. No MCP tool-call timeouts (often ~60s) on calls that run minutes.
- **The model drives the call** — a realtime speech-to-speech model handles turn-taking, barge-in, and its own tool calls; kutsu bridges audio and state.
- **Pluggable voice model** — providers sit behind one `RealtimeProvider` trait: Gemini Live in v1, OpenAI Realtime next, Amazon Nova Sonic possible.
- **One binary** — Rust, stdio MCP for local agents, streamable HTTP for deployment.

## Quickstart

> Kutsu is an early scaffold — the commands below show the intended interface, not working software yet. See [Status](#status).

```bash
cargo build --release
./target/release/kutsu mcp --transport stdio
# or over HTTP:
./target/release/kutsu mcp --transport streamable-http --bind 127.0.0.1:8090
```

### Building

The SIP media stack pulls in native C code: `ezk-rtc` → `ezk-srtp` builds a bundled **libsrtp2** and needs OpenSSL's libcrypto for SRTP. That makes a few native toolchain dependencies mandatory on **every** platform, plus a choice of how OpenSSL is provided.

#### 1. Base build requirements (all platforms, all paths)

`ezk-srtp` compiles libsrtp2 via CMake and generates its Rust FFI with `bindgen`, so you always need:

| Tool | Why |
|------|-----|
| A C compiler | MSVC (`cl.exe`, via VS Build Tools) on Windows; `gcc`/`clang` on Linux; Xcode CLT on macOS |
| **CMake** | builds the bundled libsrtp2 |
| **LLVM / libclang** | `bindgen` needs it to parse libsrtp headers — set `LIBCLANG_PATH` to LLVM's `bin` |

#### 2. OpenSSL — pick one

**System OpenSSL** (default `cargo build`) — fastest where a dev OpenSSL is present:

- Linux: `apt install libssl-dev` (or distro equivalent).
- macOS: `brew install openssl@3` (set `OPENSSL_DIR` if not auto-detected).
- Windows (MSVC): vcpkg — `vcpkg install openssl:x64-windows-static-md` + set `VCPKG_ROOT`.

**Vendored OpenSSL** (portable) — compiles OpenSSL from source and links it statically, so the binary needs no OpenSSL on the deploy host. Recommended for release binaries targeting multiple systems from CI:

```bash
cargo build --release --features vendor-openssl
```

This path adds two more build-host tools: **`perl`** (on Windows a *native* Windows perl — Strawberry Perl or ActivePerl, **not** the MSYS/Git-bundled perl, since it drives the `VC-WIN64A` build) and **`nasm`** on Windows for asm-optimized OpenSSL.

#### Windows toolchain summary

For a vendored build on Windows, the full set is: VS Build Tools (`cl.exe`) · CMake · LLVM/libclang (`LIBCLANG_PATH`) · Strawberry Perl · NASM. Install the last three via winget: `winget install NASM.NASM StrawberryPerl.StrawberryPerl LLVM.LLVM` (CMake and VS Build Tools as usual). The output binary is `target\release\kutsu.exe`; with vendored OpenSSL it is self-contained apart from the standard MSVC runtime (VC++ Redistributable).

## Architecture

```mermaid
flowchart LR
  client[MCP client / agent]
  mcp[MCP layer rmcp]
  engine[Call engine + state]
  sip[SIP trunk ezk-sip-ua]
  bridge[Audio bridge]
  model[Realtime voice model]
  phone[Callee's phone]

  client --> mcp --> engine
  engine --> sip --> phone
  sip <--> bridge <--> model
```

- **SIP leg** (`ezk-sip-ua` / `ezk-rtc`): outbound `INVITE`, RTP media, G.711 8kHz mu-law/PCM.
- **Audio bridge**: transcodes G.711 8kHz ↔ PCM16 16k/24k both ways in realtime.
- **Realtime provider** (`RealtimeProvider` trait): open a session, stream audio both ways, surface events (transcript, tool calls, barge-in, turn complete), return tool results. Implementations: Gemini Live (`BidiGenerateContent` WebSocket, session resumption) in v1; OpenAI Realtime next. Provider quirks — resumption mechanics, async-tool semantics like Gemini's `scheduling` — stay inside the implementation.
- **Call engine**: owns `CallRecord` / `TranscriptEntry` / `CallState`; one call at a time in v1.

## MCP tools

| Tool | What it does |
|------|--------------|
| `place_call` | Dial a number with a conversation script; returns `call_id` immediately |
| `get_call_status` | Poll call state (dialing, in-progress, completed, failed) |
| `get_call_transcript` | Fetch the running or final transcript |
| `end_call` | Force hangup |

## Call outcomes

Every completed call must yield three artifacts, not just a transcript (planned — see [Status](#status)):

| Artifact | What it is |
|----------|------------|
| **Transcript** | Timestamped `TranscriptEntry` list: both sides of the conversation plus tool calls the model made |
| **Goal JSON** | A structured result filled in during the call. `place_call` accepts a goal schema (contact fields, appointment, disposition, scenario-specific flags); the model fills it via tool calls (`save_contact`, `set_appointment`, `end_call(reason)`, …), and kutsu merges those calls into the final JSON |
| **Recording** | Audio of the full call (both legs), saved to disk and retrievable after hangup |

How the goal JSON gets filled: the scenario declares tools and a goal schema, tool-call arguments are merged into the goal object as the call progresses, and `end_call(reason)` sets the final disposition (appointment, callback, refused, wrong contact, …).

## In-call tool bridge (webhook)

Planned. Beyond goal-tracking tools, `place_call` accepts declarations of **external tools** plus a `tool_webhook` URL. When the model calls such a tool mid-conversation (e.g. `send_email` — "I've just sent it, could you check your inbox?"), kutsu does not execute it; it bridges the call out:

1. kutsu POSTs the tool call (`call_id`, tool call `id`, name, arguments) to `tool_webhook`.
2. The receiver **acks immediately** (2xx) and executes in the background — a fast, quality implementation on the receiving side is part of the contract; kutsu never holds the call hostage to a slow endpoint.
3. When done, the receiver POSTs the result back to kutsu's callback endpoint; kutsu forwards it to the model as a `FunctionResponse` with a `scheduling` hint (`INTERRUPT` / `WHEN_IDLE` / `SILENT`).

External tools are declared `NON_BLOCKING`, so the model keeps talking while the tool runs — no dead air on the phone. If the callee barges in and the model's pending tool calls get cancelled, kutsu notifies the webhook receiver with a cancellation event so in-flight work can be aborted or its result discarded.

## Status

Early scaffold. Nothing works yet. Build phases:

1. SIP spike (`ezk-sip-ua`/`ezk-rtc`) — outbound `INVITE`, raw RTP frames.
2. `RealtimeProvider` trait + Gemini Live implementation (`BidiGenerateContent`, session resumption, tool bridging).
3. Audio bridge (G.711 8kHz mu-law/PCM ↔ PCM16 16k/24k).
4. Call engine + state (`CallRecord`/`TranscriptEntry`/`CallState`).
5. MCP layer (`rmcp`): the five tools above.
6. Call outcomes: goal JSON (schema in `place_call`, merged from model tool calls) + call recording to disk.
7. In-call tool bridge: webhook out, async result callback in, `NON_BLOCKING` + `scheduling`, barge-in cancellation.
8. Config, docs, tests.
9. Second provider: OpenAI Realtime — validates the `RealtimeProvider` trait doesn't leak Gemini specifics.
10. Inbound calls: `REGISTER` on the trunk, DID → scenario mapping, busy policy, webhook notification of incoming calls.

### Dev harness: kutsu live

The `kutsu live` command runs an end-to-end session against the real Gemini Live API, bridging a scenario script to audio I/O:

```bash
GEMINI_API_KEY=your-api-key cargo run -- live docs/examples/scenario.json
```

**Environment:**
- `GEMINI_API_KEY` (required) — authentication token for the Gemini API.

**Scenario file format** (`docs/examples/scenario.json`):
```json
{
  "system_prompt": "Your system message to the model",
  "goal_schema": {
    "type": "object",
    "properties": { "field": { "type": "string" } },
    "required": ["field"]
  },
  "context": { "optional": "data", "passed": "to the model" }
}
```

**Audio format:**
- Input (microphone / stdin): mono PCM16 (signed 16-bit little-endian), 16 kHz sample rate.
- Output (speaker / stdout): mono PCM16, 24 kHz sample rate.

**Exit codes:**
- `0` — conversation completed (end_call or clean session end).
- `1` — session error.
- `2` — network unusable (preflight failed; call refused).

### Design decisions

- **Telephony**: generic SIP trunk (any provider or self-hosted PBX), not a specific vendor API.
- **Execution model**: async — `place_call` returns a `call_id` immediately; the call runs in the background; separate tools poll/control it.
- **Scope for v1**: one call at a time. No campaign/queue/DNC list — that is an explicit, separate follow-up.
- **External actions via webhook, not built-in**: kutsu never implements email/SMS/CRM itself. Mid-call actions go through the tool bridge; the webhook receiver acks instantly and owns execution quality.
- **Provider-agnostic voice model**: all realtime speech-to-speech APIs (Gemini Live, OpenAI Realtime, Amazon Nova Sonic) share the same shape — bidirectional session, PCM16 audio, tool calls with ids, barge-in events — so they sit behind one trait. kutsu owns the telephony; the brain is swappable. Gemini Live ships first (cheapest, proven flow); OpenAI Realtime second.
- **Outbound first, inbound is a declared goal**: the expensive parts — audio bridge, `RealtimeProvider`, call engine, outcomes — are direction-agnostic. Inbound adds `REGISTER` on the trunk, DID → scenario routing, and a busy policy; kutsu answers autonomously with the pre-configured scenario and notifies the orchestrator over webhook (the same mechanism as the tool bridge), no polling required.
- **Proven conversation flow**: the conversation logic (scenario tools, goal merging, dispositions, turn-taking against Gemini Live) was validated in an earlier Python prototype; kutsu ports that flow to Rust and puts a real SIP leg and MCP interface around it.

## License

MIT — see [LICENSE](LICENSE).
