# Gemini Live client — design spec

Date: 2026-08-13
Phase: 2 of the kutsu build plan (see README "Status").
Status: design, pending implementation.

## Goal

Build a working, Gemini-concrete realtime voice client for kutsu that speaks the
Gemini Live `BidiGenerateContent` WebSocket protocol, plus a headless dev harness
(`kutsu live`) to exercise it end to end without SIP. This ports the proven
conversation flow from the Python prototype (`e:/voice-cloud`) into Rust.

Everything in this repo is English (clean OSS). Conversation content is data and
may be Russian; the codebase is not.

## Scope

**In scope**
- `gemini_live` client: connect, stream PCM16 audio both ways, surface events,
  session resumption + reconnect, one model tool `end_call` with a dynamic schema.
- `config`: `ServerConfig` (server-level defaults) and `ScenarioConfig`
  (per-task inputs; mirror of future `place_call` args).
- `net_check`: fail-closed network preflight.
- `kutsu live` dev subcommand: file-fed harness with artifacts.
- Unit tests over config mapping, message parsing, reconnect logic, audio files,
  net-check verdicts (all offline); one opt-in live smoke test.

**Out of scope (other phases)**
- SIP/RTP leg (phase 1), audio bridge G.711↔PCM16 (phase 3), call engine + state
  store (phase 4), MCP layer on rmcp Tasks (phase 5), recording (phase 6),
  in-call tool webhook (phase 7), OpenAI provider + `RealtimeProvider` trait
  extraction (phase 9). The client already works in PCM16; the bridge converts
  later. The trait is deliberately NOT introduced yet (extract from two real
  providers, not guess from one).

## Decisions (context)

- **Gemini-concrete now, `RealtimeProvider` trait extracted later** (phase 9).
- **No OpenRouter.** Only the direct Gemini Live WS. The prototype's
  OpenRouter-proxied pieces (background STT, end-decider, TTS) are not ported.
- **Only `end_call` as a model tool.** The goal JSON is what the model passes as
  `end_call`'s arguments at conversation end — no incremental merge from multiple
  tool calls. `end_call`'s parameter schema is dynamic: it *is* the task's
  `goal_schema`. No other tools: on native-audio, mid-call tool calls work poorly
  and often drop the session.
- **Server-level defaults, not per-call args:** `model` (default half-cascade),
  `voice`, `proxy`, `language` (`ru-RU`). Per-call args (future `place_call`):
  `to_number`, `system_prompt`, `goal_schema`, `context?`.
- **Network preflight is fail-closed:** refuse the call on an unusable network
  rather than dialing.
- **Transcript is a first-class output to the agent**, streamed both sides.
- **Concurrency cap + queue:** free-tier Gemini allows ≤ 3 concurrent Live
  sessions. Server config `max_concurrent_channels` (default 3); on overflow,
  queue rather than reject. Enforcement is the engine/MCP layer (phase 4/5); the
  phase-2 client is single-session and defines the field but does not enforce it.

## Module boundaries

| File | Responsibility | Notes |
|------|----------------|-------|
| `src/gemini_live.rs` | Live client: protocol, session, events, reconnect | main body; knows nothing about SIP or MCP |
| `src/config.rs` (new) | `ServerConfig`, `ScenarioConfig`, `Model` | mirror of server config + `place_call` args |
| `src/net_check.rs` (new) | Network preflight probe + verdict | WSS ping to Gemini endpoint through proxy |
| `src/audio_file.rs` (new) | WAV/raw PCM16 read/write for the harness | via `hound` |
| `src/main.rs` | add `live` subcommand next to `mcp` | dev harness wiring |
| `tests/` | offline unit tests + fixtures; opt-in live smoke | |

Isolation principle: `gemini_live` deals only in PCM16 + events + config, easing
the later `RealtimeProvider` extraction.

## Public API

```rust
// config.rs
pub struct ServerConfig {          // set at server start
    pub api_key: String,
    pub proxy: Option<Proxy>,
    pub model: Model,              // default HalfCascade
    pub voice: String,
    pub language: String,         // "ru-RU"; half-cascade only
    pub net_check: NetCheckConfig,
    pub max_concurrent_channels: usize,   // default 3 (free-tier cap); enforced in phase 4/5
}
pub enum Model { HalfCascade, NativeAudio }

pub struct ScenarioConfig {         // mirror of place_call args
    pub system_prompt: String,
    pub goal_schema: serde_json::Value,   // JSON Schema for end_call parameters
    pub context: Option<serde_json::Value>,
}

// gemini_live.rs
pub async fn start(server: &ServerConfig, scenario: &ScenarioConfig)
    -> Result<Session>;

pub struct Session {
    pub audio_in: tokio::sync::mpsc::Sender<Vec<i16>>, // PCM16 16k mono, ~20-32ms frames
    pub events:   tokio::sync::mpsc::Receiver<Event>,
}
impl Session {
    pub async fn hangup(&self);             // our-side end (SIP BYE analog)
    pub async fn join(self) -> CallOutcome; // await completion, take outcome
}

pub enum Role { User, Model }

pub enum Event {
    OutputAudio(Vec<i16>),                                  // PCM16 24k
    Transcript { role: Role, text: String, final_: bool },
    Interrupted,                                            // barge-in
    TurnComplete,
    EndCall { goal: serde_json::Value },                    // args of end_call
    Warning(String),                                        // e.g. reconnecting
}

pub struct TranscriptEntry { pub role: Role, pub text: String, pub ts_ms: u64 }

pub enum EndedBy { ModelEndCall, CallerHangup, RemoteClose, Error }
pub struct CallOutcome {
    pub ended_by: EndedBy,
    pub goal: Option<serde_json::Value>,
    pub transcript: Vec<TranscriptEntry>,
}
```

- Reconnect/session-resumption is internal to `start`'s task; only surfaced as
  `Warning`. The caller's audio/event streams are seamless across reconnects.
- `end_call` flow: model calls it → client sends `toolResponse` ack →
  emits `Event::EndCall { goal }` → closes → `join()` returns the outcome.
  `goal` is the raw arguments; schema validation is the engine's job (later).
- `TranscriptEntry` lives here for now; the engine (phase 4) reuses/extends it.

## Protocol mapping (`BidiGenerateContent`)

No Rust SDK for Gemini Live exists; implement the protocol by hand over
`tokio-tungstenite`.

- Endpoint:
  `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.{ver}.GenerativeService.BidiGenerateContent?key=API_KEY`
  - `ver`: native → `v1alpha` (needed for affective/proactive); half → `v1beta`.
  - Proxy applied at WS connect (same path as the preflight probe).

Setup message (`BidiGenerateContentSetup`) mapping:

| Protocol field | Source | half | native |
|---|---|---|---|
| `model` | `ServerConfig.model` | `gemini-3.1-flash-live-preview` | `gemini-2.5-flash-native-audio-preview-12-2025` |
| `generationConfig.responseModalities` | const | `["AUDIO"]` | `["AUDIO"]` |
| `generationConfig.temperature` | const | `0.8` | `0.8` |
| `…speechConfig.voiceConfig.prebuiltVoiceConfig.voiceName` | `ServerConfig.voice` | ✓ | ✓ |
| `…speechConfig.languageCode` | `ServerConfig.language` | `ru-RU` | omit |
| `systemInstruction` | assembled prompt | ✓ | ✓ |
| `tools[0].functionDeclarations` | `[end_call]` | ✓ | ✓ (`behavior: NON_BLOCKING`) |
| `realtimeInputConfig.automaticActivityDetection` | VAD branch | startSens LOW, prefix 1000ms | startSens HIGH, prefix 300ms, silence 100ms |
| `sessionResumption` | `{ handle }` | ✓ | ✓ |
| `inputAudioTranscription` / `outputAudioTranscription` | `{}` / `{}` | ✓ | ✓ |
| `enableAffectiveDialog` / `proactivity.proactiveAudio` | — | — | `true` / `true` |

`end_call` tool declaration:
`{ name: "end_call", description: <"call at end of conversation with collected
data and disposition">, parameters: <ScenarioConfig.goal_schema verbatim> }`.
Disposition/reason is a field inside the goal schema (client stays agnostic).

System prompt assembly: `system_prompt` + rendered `context` + a current-time
block (time goes into the prompt because `get_current_time` returns 1008 on
2.5/native). No other tools.

Runtime server-message → `Event`:
- `serverContent.modelTurn…inlineData` (audio/pcm 24k) → `OutputAudio`
- `serverContent.outputTranscription` / `inputTranscription` → `Transcript{role}`
- `serverContent.interrupted` → `Interrupted` (drain output audio)
- `serverContent.turnComplete` → `TurnComplete`
- `toolCall.functionCalls[end_call]` → ack + `EndCall`
- `sessionResumptionUpdate.newHandle` → store in state (on every message)
- `goAway` / WS close (incl. 1008) → trigger reconnect

Input: `Vec<i16>` PCM16 16k → `realtimeInput` with
`mimeType "audio/pcm;rate=16000"`, sent as a continuous stream (native-audio
requires continuity).

Reconnect (analog of the prototype's `run_resumable`): loop connect with the
current handle → run session → on a resumable error (WS close/1008, timeout, API
error) back off 0.3s → ×2 → max 5s; after 4 consecutive failures drop the handle
(stale) and start fresh. Store the latest `newHandle` on every message.

## Network preflight (`net_check`)

- `preflight(server: &ServerConfig) -> NetworkHealth`, called before `start()`.
- Method: open a real WSS connection to the Gemini endpoint through the same
  proxy, send N WebSocket ping frames, measure pong RTT.
- `NetworkHealth { rtt_p50_ms, rtt_p95_ms, jitter_ms, loss_pct, verdict }`.
- `NetCheckConfig { max_rtt_ms, max_jitter_ms, max_loss_pct, samples, enabled }`.
  Defaults tuned for realtime voice: RTT p95 ≤ 300ms, jitter ≤ 50ms, loss ≤ 2%.
- Fail closed: on `Unusable`, do not start the session; the call is refused with a
  structured reason (`network_unstable` + metrics). In the product this becomes a
  fast `failed` task; in the harness, a printed error + non-zero exit.
- Extensible: the product will also probe the SIP/RTP leg (phases 1/3); phase 2
  covers the Gemini leg only.

## Concurrency & queueing (design note; enforced in phase 4/5)

Free-tier Gemini allows at most 3 concurrent Live sessions. `ServerConfig`
carries `max_concurrent_channels` (default 3). The engine/MCP layer will enforce
it with a `Semaphore(max_concurrent_channels)`: `place_call` returns a task id
immediately; the background worker acquires a permit before starting the Gemini
session; while waiting it keeps the task in `working` with a "queued (position
N)" status message. Overflow queues, never rejects. Network preflight runs after
the permit is acquired, so probes never exceed the cap. The phase-2 client is
single-session: it defines the config field but does not enforce the cap (that is
the engine's job).

## Dev harness (`kutsu live`)

CLI:
```
kutsu live --scenario scenario.json --audio-in user.wav [--audio-out out.wav]
           [--transcript out.jsonl] [--goal-out goal.json]
           [--model half|native] [--voice NAME] [--language ru-RU]
           [--tail SECONDS] [--no-net-check]
```
Secrets/server config from env (`GEMINI_API_KEY`, proxy) or flags; scenario data
from the file. `--model/--voice/...` override server defaults for convenience.

Scenario file (mirror of future `place_call` args), English example:
```json
{
  "system_prompt": "You are an assistant calling about ...",
  "goal_schema": {
    "type": "object",
    "properties": {
      "interested": { "type": "boolean" },
      "callback_at": { "type": "string" },
      "disposition": { "type": "string" }
    },
    "required": ["disposition"]
  },
  "context": { "name": "Ivan", "city": "Kazan" }
}
```

Audio formats (`audio_file.rs`, via `hound`):
- Input: WAV PCM16 mono 16 kHz (or raw `.pcm` at the same rate). Other rates → a
  clear error (resampling is phase 3).
- Output: WAV PCM16 mono 24 kHz — the model's speech.

Flow:
1. Network preflight. On `Unusable`: print metrics, do NOT start, exit code 2.
   (`--no-net-check` for offline message-format debugging.)
2. `start(server, scenario)` → session.
3. Read `audio-in`, send it in 32 ms (512-sample) frames paced in real time
   (VAD/turn-taking depend on timing).
4. Consume `events` concurrently:
   - `Transcript` → live print (user/model distinct) + line in `transcript.jsonl`
     (`{ts, role, text, final}`).
   - `OutputAudio` → accumulate into `out.wav`.
   - `Interrupted`/`TurnComplete`/`Warning` → log.
   - `EndCall { goal }` → write `goal.json`, finish.
5. EOF policy: after input ends, keep the session open `--tail` seconds
   (default ~8s) to catch the final reply; if the model never calls `end_call`,
   `hangup()`.
6. `join()` → `CallOutcome`; print summary (ended_by, disposition, duration,
   network metrics).

Exit codes: `0` conversation completed (end_call or clean hangup); `2` network
unusable; `1` session error.

## Error handling

Typed errors (`thiserror`): `ConfigError`, `NetworkUnstable { metrics }`,
`ConnectError`, `ProtocolError`, `SessionClosed`. The reconnect loop swallows
transient errors (surfaced only as `Warning`); terminal failure arrives as
`CallOutcome.ended_by = Error`. No panics on the happy path. The harness maps
outcomes/errors to exit codes.

## Testing

Offline unit tests (core):
1. Config → `BidiGenerateContentSetup` JSON: half/native branches, endpoint
   `v1beta`/`v1alpha`, voice/language, `goal_schema` injection into `end_call`
   parameters, VAD params, affective/proactive only on native.
2. Server message parsing → `Event`, against recorded JSON fixtures in
   `tests/fixtures/` (setupComplete, serverContent audio/transcription, toolCall
   `end_call`, sessionResumptionUpdate, interrupted, turnComplete, goAway).
3. Reconnect logic as a pure function with a fake connector: handle capture,
   backoff schedule, reset after 4 failures.
4. `audio_file` WAV round-trip; rate/format validation.
5. `net_check` verdict thresholds.

Live smoke test: behind a `live-tests` feature + `GEMINI_API_KEY`, marked
`#[ignore]`; a short real session with a tiny WAV asserting we receive
audio+transcript and end cleanly. Not in CI by default.

Mock WS server (like the prototype's `live_server_mock.py`) for an offline
end-to-end harness run: optional/stretch, not in v1 (fixtures + one ignored live
test cover the core). Add later if fixtures become insufficient.

Implementation follows TDD: red → green per module.

## New dependencies

- `hound` (WAV I/O for the harness).
- `thiserror` (typed error enums).
- Everything else (`tokio-tungstenite`, `serde`/`serde_json`, `anyhow`,
  `tracing`, `clap`, `chrono`) is already in the tree.
