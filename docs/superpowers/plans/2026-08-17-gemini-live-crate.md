# `gemini-live` Crate Extraction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a standalone `gemini-live` Rust crate (separate repo, git submodule at `crates/gemini-live`) that owns all Gemini Live connection concerns, and migrate kutsu onto its async event API without regressing any behavior.

**Architecture:** Four crate layers — `types` (Live-subset wire types), `wire` (setup serialization + server-message/affective parsing, no I/O), `transport` (proxy+TLS WebSocket + FakeTransport), `session` (reconnect/resumption driver exposing `next_event()`). kutsu keeps greeting gate, energy VAD, audio bridge, engine, and prompt assembly, rewritten over the crate API.

**Tech Stack:** Rust 2024, tokio, tokio-tungstenite, rustls (ring provider), serde_json, base64. Submodule + path dependency.

**Spec:** `docs/superpowers/specs/2026-08-17-gemini-live-crate-design.md`

## Global Constraints

- All in-repo text (code, comments, log messages) in **both** repos is English.
- kutsu builds/tests only with `cargo test --lib --features vendor-openssl`; integration compiles with `cargo build --tests --features "vendor-openssl live-tests"`.
- The crate builds and tests standalone (`cargo test` in `crates/gemini-live`).
- No behavior regression: greeting suppression on callee speech, reconnect-safe no-re-greet, `RESUME_CUE` on lost-context reconnect, the goodbye drain, and byte-transparent uplink audio all still hold. All 133 current kutsu lib tests stay green (adapted to the new API, not deleted).
- **The wire layer (`build_setup`, `parse_server_message`, affective handling) MIRRORS the google-genai SDK converters as the source of truth** — `_live_converters.py` (the MLDev / Gemini Developer API path, not Vertex) and `types.py`. kutsu's `src/proto.rs` is only a cross-check (it was hand-derived and fixed against the SDK; where they disagree, the SDK wins). Do not re-derive the wire format; encode the SDK's exact `to_object`/`from_object` field paths, casing, and part filtering (e.g. `thought` parts). The crate's own *types* (SetupConfig/ServerEvent) may be ergonomic Rust APIs; only the wire serialization/parsing must match the SDK byte-for-byte.
- Wire contract quick-reference (all derived from the SDK, see spec "Wire contract"): snake_case top-level oneof keys, camelCase inner; binary server frames; model `-latest` on native (v1alpha) / `gemini-3.1-flash-live-preview` (v1beta); `enableAffectiveDialog` under `generationConfig`; `proactivity` top-level; no `languageCode` on native; `thinkingBudget=0` on native.
- Affective dialog is **on** in the crate, paired with the token-stripping parser. Never leak `<ctrl95>` frames into `Transcript` text or `OutputAudio`.
- English cue/prompt text stays in kutsu (prompt assembly is not part of the crate).

---

### Task 1: Bootstrap the crate repo + submodule (controller-run)

**Files:**
- Create in the crate repo: `Cargo.toml`, `src/lib.rs`, `.gitignore`, `README.md`.
- Modify kutsu: `Cargo.toml` (workspace + path dep), `.gitmodules` (submodule).

**Interfaces:**
- Produces: a compiling empty `gemini-live` crate at `crates/gemini-live`, depended on by kutsu via path; `pub` module stubs `types`, `wire`, `transport`, `session`.

- [ ] **Step 1:** In a clone of `https://github.com/metalmon/gemini-live.git`, create `Cargo.toml` (`[package] name = "gemini-live", edition = "2021"`, deps: serde, serde_json, base64, tokio, tokio-tungstenite, rustls, futures-util, tracing — versions matching kutsu's lockfile), `src/lib.rs` with `pub mod types; pub mod wire; pub mod transport; pub mod session;` and empty module files, `.gitignore` (`/target`), `README.md` (one-paragraph purpose).
- [ ] **Step 2:** `cargo build` in the crate — compiles empty.
- [ ] **Step 3:** Commit on `main` and `git push -u origin main` (bootstrap the empty remote — authorized).
- [ ] **Step 4:** In kutsu: `git submodule add https://github.com/metalmon/gemini-live.git crates/gemini-live`; add `gemini-live = { path = "crates/gemini-live" }` to kutsu `[dependencies]`; make kutsu a workspace root if needed so the submodule builds in-tree.
- [ ] **Step 5:** `cargo build --features vendor-openssl` in kutsu — compiles with the new (unused) dep. `cargo test --lib --features vendor-openssl` still 133 green.
- [ ] **Step 6:** Commit kutsu (`.gitmodules`, `Cargo.toml`, submodule pointer).

### Task 2: `types` — Live-subset wire types

**Files:** Create crate `src/types.rs`.

**Interfaces:**
- Produces: `Model { HalfCascade, NativeAudio }` with `fn model_id(&self) -> &'static str` and `fn api_version(&self) -> &'static str`; `Role { User, Model }`; `CloseReason { code: u16, reason: String }`; `AffectLabel(String)`; config structs (`SetupConfig`, `GenerationConfig`, `SpeechConfig`, `Aad`, etc.) mirroring the fields kutsu's `proto::build_setup` sends; server-side `ServerEvent` enum (SetupComplete, OutputAudio(Vec<i16>), Transcript{role,text,final_}, Affect{role,label}, Interrupted, TurnComplete, ToolCall{name,id,args}, ResumptionHandle(String), GoAway).

- [ ] **Step 1:** Port the enums/structs from `src/proto.rs` + `src/config.rs` (Model, api-version/model-id mapping) and `src/gemini_live.rs` (ServerEvent). Add `Affect`. Derive serde where serialized.
- [ ] **Step 2:** Test: `Model::NativeAudio.model_id() == "gemini-2.5-flash-native-audio-latest"`, `api_version() == "v1alpha"`; half-cascade → `gemini-3.1-flash-live-preview` / `v1beta`.
- [ ] **Step 3:** `cargo test` in crate — green.

### Task 3: `wire::build_setup` — setup serialization

**Files:** Create crate `src/wire.rs` (part 1). Test in-module.

**Interfaces:**
- Consumes: `types`. Produces: `pub fn build_setup(cfg: &SetupConfig) -> serde_json::Value`.

- [ ] **Step 1:** Port `build_setup` from `src/proto.rs:36-97` verbatim (it is already correct post-fixes), operating on the crate's `SetupConfig` instead of kutsu's `ServerConfig`/`ScenarioConfig`. The system-instruction *text* is passed in as a `String` field on `SetupConfig` (kutsu assembles it; the crate does not build prompts).
- [ ] **Step 2:** Port the existing proto tests as wire tests and ADD wire-contract assertions: native → `enableAffectiveDialog` under `generationConfig` (not top-level), `proactivity.proactiveAudio` top-level, `thinkingConfig.thinkingBudget == 0`, no `speechConfig.languageCode`; half-cascade → `languageCode` present, v1beta model; both → snake_case wrapper `setup`.
- [ ] **Step 3:** `cargo test` in crate — green.

### Task 4: `wire::parse_server_message` — server frames

**Files:** Modify crate `src/wire.rs` (part 2).

**Interfaces:**
- Produces: `pub fn parse_server_message(bytes: &[u8]) -> Result<Vec<ServerEvent>>` (accepts binary JSON).

- [ ] **Step 1:** Mirror the SDK's `_LiveServerMessage_from_mldev` converter (`_live_converters.py`) as the authority — including how it walks `serverContent.modelTurn.parts` and the `thought`/`thought_signature` part flags (thought parts are internal annotations, NOT content — exclude them, which is how the SDK's output stays clean of affective/annotation tokens). Handle setupComplete, modelTurn audio (`inlineData`) + non-thought text, input/outputTranscription → Transcript, interrupted, turnComplete, toolCall → ToolCall, sessionResumptionUpdate → ResumptionHandle, goAway. Cross-check against `src/proto.rs` (where they disagree, the SDK wins). Accept `&[u8]`, decode UTF-8 JSON.
- [ ] **Step 2:** Port the existing proto parse tests; assert binary-bytes input parses identically to the string form.
- [ ] **Step 3:** `cargo test` in crate — green.

### Task 5: `wire` — affective handling (verify, not reverse-engineer)

**Files:** Modify crate `src/wire.rs` (part 3) if needed.

**Premise:** The prototype runs `enableAffectiveDialog` ON through the SDK and does NOT parse any tokens, with no leakage — because the SDK excludes internal `thought` parts from content (handled in Task 4). So Task 4's SDK-faithful parse should already keep the output clean with affective on; there is likely NOTHING bespoke to do here. This task VERIFIES that, and only adds handling if a real residual remains.

**Verify-step (human-run, only if needed):** If, after Task 4, a live call with `enableAffectiveDialog` on still leaks `<ctrl95>`/`emotion_*` tokens into a `Transcript`, capture one raw server frame (transport debug log) to see which field/part actually carries them, and extend the parser to mirror how the SDK drops them. Do not reverse-engineer speculatively; only if the SDK-faithful port proves insufficient.

- [ ] **Step 1:** Test (using the captured frame shape): a model-turn text containing the affective frame parses into a clean `Transcript` (tokens removed) plus one or more `Affect { role, label }` events. A frame with no affective tokens is unchanged.
- [ ] **Step 2:** Implement the stripper in `parse_server_message`: detect the `<ctrl95>`-framed `emotion_user`/`emotion_model` annotations, emit `Affect`, and pass the cleaned text to `Transcript`. Never emit the raw tokens.
- [ ] **Step 3:** `cargo test` in crate — green.

### Task 6: `transport` — WsTransport + FakeTransport

**Files:** Create crate `src/transport.rs`.

**Interfaces:**
- Produces: `trait Transport { async fn send_text(&mut self, s: String) -> Result<()>; async fn recv(&mut self) -> Option<Result<Vec<u8>>>; }`; `WsTransport` (real); `FakeTransport` (scripted, `#[cfg(any(test, feature="test-util"))]` or always-public for kutsu tests).

- [ ] **Step 1:** Port the connect path from `src/gemini_live.rs` (TCP → HTTP CONNECT with Basic auth → `client_async_tls`; rustls ring provider install-once; endpoint URL by api-version). Port the recv loop INCLUDING the close-frame `code`+`reason` log (the diagnostic added in kutsu) and binary+text handling.
- [ ] **Step 2:** Port `FakeTransport` from `gemini_live.rs` tests into the crate as a reusable test double (scripted incoming frames, captured outgoing).
- [ ] **Step 3:** Tests: FakeTransport round-trips; a scripted Close frame surfaces `CloseReason{code,reason}`.
- [ ] **Step 4:** `cargo test` in crate — green.

### Task 7: `session` — reconnect/resumption driver + event API

**Files:** Create crate `src/session.rs`.

**Interfaces:**
- Produces the public API from the spec: `Session::connect(cfg) -> Result<Session>`, `next_event() -> Option<Event>`, `send_audio(&[i16])`, `send_client_text(&str)`, `send_tool_response(&str)`; `enum Event { SessionOpened{is_reconnect}, SessionClosed{reason}, OutputAudio, Transcript, Affect, Interrupted, TurnComplete, ToolCall }`.

- [ ] **Step 1:** Port the reconnect/resumption loop from `src/gemini_live.rs::start` (latest handle stored on ResumptionHandle, backoff 0.3s→×2→max 5s, drop stale handle after N failed connects). On each (re)open, send setup, then emit `SessionOpened{is_reconnect}` as the first event; on transport close/goAway, emit nothing and transparently reconnect (the stream continues); on terminal failure, emit `SessionClosed{reason}` and end the stream.
- [ ] **Step 2:** `send_audio` serializes realtime_input; `send_client_text` sends a client_content turn; `send_tool_response` acks a tool call. Uplink bytes must be byte-identical to `build_realtime_input` today.
- [ ] **Step 3:** Tests over FakeTransport: first open emits `SessionOpened{is_reconnect:false}`; a scripted drop + reopen emits `SessionOpened{is_reconnect:true}` and preserves the handle; OutputAudio/Transcript/ToolCall/TurnComplete surface in order; `send_audio` produces the exact realtime_input JSON.
- [ ] **Step 4:** `cargo test` in crate — green.

### Task 8: kutsu migration onto the crate

**Files:** Modify kutsu `src/gemini_live.rs` (rewrite `run_session`/`start` over the crate), `src/proto.rs` (delete migrated wire code; keep prompt assembly), `src/engine.rs`/`src/bridge` (event mapping only if the enum moves), kutsu `Cargo.toml`.

**Interfaces:**
- Consumes: `gemini_live::session::{Session, Event}`. kutsu keeps its bridge-facing `Event` enum and maps from the crate's `Event`.

- [ ] **Step 1:** Replace kutsu's transport + reconnect loop with `gemini_live::Session`. Rewrite `run_session` as a `select!` over `session.next_event()` + `audio_in.recv()` (→ `session.send_audio`) + the greeting timer, preserving: `greet_armed`/`greeted`/`greeted_ever`/`callee_active` gate, the energy VAD (`crate::vad`), the `SessionOpened{is_reconnect:true}` reaction (drain `audio_in`, emit bridge `Interrupted`, send `RESUME_CUE` when the handle was lost + `resume_needed`), `resume_needed` clear on `TurnComplete`, and the user-transcript belt-and-suspenders. Map crate `Event` → kutsu bridge `Event`.
- [ ] **Step 2:** kutsu `build_system_prompt` + language/gender/closing directives stay; `build_setup`/`parse_server_message`/transport are deleted from `proto.rs`/`gemini_live.rs` and sourced from the crate. `SetupConfig` is assembled in kutsu from `ServerConfig` + the prompt text.
- [ ] **Step 3:** Rebuild the `gemini_live` greeting/VAD/reconnect tests on the crate `FakeTransport` + `Session`. Keep every assertion (no-greeting-before-answered, greeting-suppressed-when-callee-speaks, greets-when-silent, reconnect no-re-greet, RESUME_CUE cases, drain).
- [ ] **Step 4:** `cargo test --lib --features vendor-openssl` — 133 green (adapted). `cargo build --tests --features "vendor-openssl live-tests"` compiles.
- [ ] **Step 5:** Commit kutsu + bump the submodule pointer.

### Task 9: Whole-feature verification

**Files:** none.

- [ ] **Step 1:** `cargo test` in `crates/gemini-live` — crate suite green, no warnings.
- [ ] **Step 2:** kutsu `cargo test --lib --features vendor-openssl` — 133 green; `cargo build --lib --features vendor-openssl` no warnings; integration build compiles.
- [ ] **Step 3:** Manual live call (human): native call greets in Russian, single reactive greeting, sustains a scenario conversation, plays the goodbye out, no `<ctrl95>` leakage (affective back on, parsed), and reconnects never re-greet.
- [ ] **Step 4:** Commit any fixups; final submodule pin bump.
