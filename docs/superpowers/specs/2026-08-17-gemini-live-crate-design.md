# `gemini-live` Rust crate — Design

**Status:** draft for review
**Date:** 2026-08-17
**Author:** kutsu team
**Related:** [[voice-cloud-reference]] (proven wire params + the gotchas this crate encodes), kutsu `src/proto.rs` / `src/gemini_live.rs` (the ad-hoc implementation being extracted)

## Problem

kutsu talks to the Gemini Live API over a hand-rolled WebSocket client split
across `src/proto.rs` (setup serialization + message parsing) and
`src/gemini_live.rs` (transport, session loop, reconnect). This ad-hoc partial
reimplementation of the wire protocol has repeatedly shipped wire-format bugs
that only surfaced on live calls:

- `realtimeInput` camelCase silently ignored → no audio reached the model.
- Server frames are binary, not text → a text-only recv loop dropped everything.
- `enableAffectiveDialog` placed at `setup` top-level → close **1007**, a
  reconnect storm.
- `enableAffectiveDialog` emotion tokens (`<ctrl95>`-framed) leaked into the
  transcript and spoken audio because nothing parsed them.
- A dated native-audio preview model id → close/instability; `-latest` fixes it.

Each was a day of live debugging. The wire contract is real and unforgiving;
we keep relearning it. The Python `google-genai` SDK encodes it correctly (its
`_live_converters.py` is the authority we keep consulting). We want that
correctness once, in one tested place.

## Goal

Extract a focused, independently-testable **`gemini-live`** Rust crate that owns
everything about *talking to Gemini Live correctly* — the wire types, setup
serialization, server-message parsing (including affective tokens), the
WebSocket transport (proxy + TLS + close diagnostics), and the
reconnect/resumption driver. kutsu consumes it through a clean async event API
and keeps only call-orchestration concerns (greeting gate, energy VAD, the
audio bridge, the engine).

Non-goal: porting the whole `google-genai` SDK (Vertex, all model APIs,
embeddings, files, tuning). Only the **Live** subset kutsu uses.

## Repository & integration

- The crate lives in its **own git repository**
  (`https://github.com/metalmon/gemini-live.git`), included in kutsu as a
  **git submodule** at `crates/gemini-live`.
- kutsu depends on it by path: `gemini-live = { path = "crates/gemini-live" }`.
  The submodule pins a specific crate commit; bump the pointer when the crate
  changes.
- **Empty-repo bootstrap:** the remote starts with zero commits, so
  `git submodule add` cannot check it out ("branch yet to be born"). The first
  task creates the crate skeleton, makes the initial commit on `main`, pushes
  it to bootstrap the remote, then registers the submodule. (Pushing the crate
  repo is explicitly authorized by the repo owner having created it for this.)
- The crate builds standalone (`cargo test` inside `crates/gemini-live`) and as
  part of kutsu (`cargo test --lib --features vendor-openssl` still green).

## Scope boundary — crate vs kutsu

**The crate owns (all "connection"):**
- Wire types for the Live setup config and server messages (Live subset).
- Setup serialization matching the SDK converters exactly.
- Server-message parsing: binary frames, transcription, tool calls,
  `sessionResumptionUpdate`, `goAway`, and affective `<ctrl95>` tokens.
- WebSocket transport: HTTP CONNECT proxy tunnel, TLS (rustls provider install),
  binary+text frames, close-frame code+reason, ping/pong.
- The reconnect + session-resumption driver: keep the latest handle, backoff,
  drop a stale handle after N consecutive failures, and expose a **single event
  stream that survives reconnects**.

**kutsu keeps (all "call semantics"):**
- The hybrid greeting gate (`greet_after_silence_ms`, `greeted_ever`).
- The energy VAD (`src/vad.rs`) and greeting suppression on callee speech.
- The reactions to a reconnect: drain the buffered uplink, flush the pacer
  (barge-in), send `RESUME_CUE` — driven off the crate's reconnect event.
- The audio bridge (`src/bridge`), SIP (`src/sip`), engine, MCP, state.
- Prompt assembly (system prompt, gender, closing, language pin).

## Crate architecture

Four layers, inner to outer:

### 1. `types`
Rust types for the Live subset, hand-ported from the SDK's `types.py`:
- `SetupConfig` (model, `GenerationConfig{ response_modalities, temperature,
  speech_config, thinking_config, enable_affective_dialog }`, system
  instruction, tools, `realtime_input_config{ automatic_activity_detection }`,
  session resumption, in/out transcription, `proactivity`).
- `Model { HalfCascade, NativeAudio }` with the resolved model ids and api
  versions.
- Server-side: `ServerMessage` variants (setup complete, server content with
  model turn / transcription / interrupted / turn complete, tool call,
  resumption update, go away).
- `Role`, `AffectLabel`, `CloseReason { code, reason }`.

### 2. `wire`
Pure serialization/parsing, no I/O — the highest-value tested surface:
- `build_setup(&SetupConfig) -> serde_json::Value`, encoding the **exact** wire
  paths (see "Wire contract" below).
- `parse_server_message(&[u8]) -> Vec<ServerEvent>`, handling binary JSON and
  the affective token frames.
- **Affective parser:** recognizes the `<ctrl95>`-framed
  `emotion_user`/`emotion_model` annotations, strips them from the text/audio
  content, and yields `ServerEvent::Affect { role, label }` alongside the clean
  `Transcript`. (Re-enables `enable_affective_dialog`, which
  [[voice-cloud-reference]] confirms the prototype ships on for native.)

### 3. `transport`
- `Transport` trait: `send_text`, `recv() -> Option<Result<Message>>`,
  `close()`.
- `WsTransport`: TCP → HTTP CONNECT (Basic auth) → `client_async_tls` → the
  Gemini endpoint (v1alpha/v1beta by model). Installs the rustls ring provider
  once. Logs the WS Close frame `code` + `reason` (never discards it).
- `FakeTransport`: scripted frames for tests, replacing the ad-hoc fake in
  `gemini_live.rs`.

### 4. `session`
The reconnect/resumption driver and the crate's public API:
```rust
pub struct Session { /* transport + reconnect state + latest handle */ }

impl Session {
    /// Connect (first open). Emits SessionOpened{ is_reconnect: false } first.
    pub async fn connect(cfg: ClientConfig) -> Result<Session>;

    /// Unified event stream across reconnects. Drives the transport, performs
    /// resumption + backoff internally, and surfaces reconnect boundaries as
    /// events so the caller can react (drain/RESUME_CUE) without owning the
    /// reconnect loop.
    pub async fn next_event(&mut self) -> Option<Event>;

    /// Send one uplink audio frame (PCM16 @ 16 kHz).
    pub async fn send_audio(&mut self, pcm16_16k: &[i16]) -> Result<()>;

    /// Send a client text turn (used by kutsu for GREET_CUE / RESUME_CUE).
    pub async fn send_client_text(&mut self, text: &str) -> Result<()>;

    /// Acknowledge a tool call (kutsu handles end_call semantics).
    pub async fn send_tool_response(&mut self, call_id: &str) -> Result<()>;
}

pub enum Event {
    SessionOpened { is_reconnect: bool },
    SessionClosed { reason: CloseReason },   // terminal; stream ends after
    OutputAudio(Vec<i16>),                    // 24 kHz PCM16
    Transcript { role: Role, text: String, final_: bool },
    Affect { role: Role, label: AffectLabel },
    Interrupted,
    TurnComplete,
    ToolCall { name: String, id: String, args: serde_json::Value },
}
```
Session resumption handles are managed internally (stored on each
`sessionResumptionUpdate`, replayed on reconnect, dropped as stale after N
failed connects). `is_reconnect` on `SessionOpened` is the signal kutsu needs
for its drain/RESUME_CUE reaction.

## Wire contract (encoded once, tested)

From [[voice-cloud-reference]] and the SDK `_live_converters.py`:
- Top-level client→server oneof keys are **snake_case**: `setup`,
  `client_content`, `realtime_input`, `tool_response`. Inner fields are
  **camelCase** (`generationConfig`, `turnComplete`, `mimeType`).
- Server→client frames are **binary** WS frames (JSON bytes).
- Model ids: half-cascade `gemini-3.1-flash-live-preview` (v1beta);
  native-audio `gemini-2.5-flash-native-audio-latest` (v1alpha) — use `-latest`,
  never a dated preview.
- native-audio: **no** `languageCode` in `speechConfig` (it picks its own; kutsu
  pins language via the prompt); `generationConfig.thinkingConfig.thinkingBudget
  = 0`.
- **`enableAffectiveDialog` nests under `generationConfig`**; **`proactivity`
  is top-level `setup`**. (Top-level `enableAffectiveDialog` → close 1007.)
- Audio realtime input:
  `{"realtime_input":{"audio":{"data":"<b64>","mimeType":"audio/pcm;rate=16000"}}}`.
- Endpoint is geo-restricted → HTTP CONNECT proxy required.

## Migration plan (what moves)

- `src/proto.rs` → crate `wire` + `types` (build_setup, parse_server_message,
  model/endpoint mapping, server-event types). kutsu keeps prompt assembly
  (`build_system_prompt`, gender/closing/language directives) — that is call
  content, not wire.
- `src/gemini_live.rs` transport (proxy connect, TLS, WS recv/close) → crate
  `transport`. The reconnect/resumption loop (`start`) → crate `session`.
- `src/gemini_live.rs` `run_session` greeting/VAD/audio-forward/reconnect-drain
  logic → **stays in kutsu**, rewritten as a loop over `Session::next_event()`
  + `send_audio`/`send_client_text`, reacting to `SessionOpened{is_reconnect}`.
- `Event` (kutsu's bridge/engine event type) is mapped from the crate's `Event`
  at the kutsu boundary (kutsu keeps its own bridge-facing enum, or re-exports).

## Testing

- Crate unit tests (standalone): `wire` round-trips (setup shape per model,
  affective token stripping → clean text + Affect events, all the wire-contract
  rules as assertions), `session` reconnect/resumption over `FakeTransport`.
- kutsu integration: the existing 133 lib tests must stay green. The
  `gemini_live` greeting/VAD/reconnect tests are rebuilt on the crate's
  `FakeTransport` + `Session` API.
- `cargo build --tests --features "vendor-openssl live-tests"` still compiles.

## Global constraints

- All in-repo text (code, comments, log messages) is **English** (both repos).
- kutsu builds/tests only with `--features vendor-openssl` on this Windows host.
- The crate must not regress any current behavior: greeting suppression,
  reconnect-safe no-re-greet, RESUME_CUE, the goodbye drain, audio byte-
  transparency of the uplink.
- Affective dialog is **re-enabled** in this crate (with the token parser), not
  before — master currently ships it off (`fix(gemini): disable affective…`).

## Risks / open points

- **API reshape:** kutsu's `run_session` is channel-driven today; moving to a
  `Session::next_event()` method API is the largest single change. Mitigation:
  keep kutsu's bridge-facing `Event` enum stable; only the source changes.
- **Affective frame format:** the exact `<ctrl95>` byte framing must be captured
  from a live frame (debug-logged raw) before the parser is finalized — a spec
  task, not a guess.
- **Submodule workflow friction:** two-repo commits, pin bumps. Accepted for the
  independence benefit.
- **Cross-repo CI:** out of scope now; the crate is path-dep'd and built in-tree.

## Out of scope

- Non-Live genai APIs; Vertex; RED/FEC RTP resilience (separate audio-layer
  work); the audio bridge; SIP.
