//! Call orchestration on top of the `gemini-live` crate's session driver.
//!
//! The Gemini Live connection lifecycle — wire protocol, WebSocket transport
//! (proxy + TLS), server-message parsing, and reconnect/resumption — is owned by
//! the [`gemini_live`](::gemini_live) crate's [`Session`](::gemini_live::session::Session).
//! This module keeps only kutsu's *call* concerns on top of that stream:
//! the hybrid greeting gate, the energy VAD ([`crate::vad`]), the reconnect
//! reactions (drain stale uplink / flush the pacer / RESUME_CUE), `end_call`
//! handling, and transcript accumulation. It exposes a bridge-facing [`Event`]
//! and a [`Session`] handle (audio sink + event stream + hangup/join) that the
//! engine and audio bridge consume unchanged.

use serde_json::Value;
use tokio::sync::{mpsc, watch};

use ::gemini_live::session::{ClientConfig, Event as CrateEvent, Session as CrateSession};
use ::gemini_live::transport::ProxyConfig;
use ::gemini_live::types::{Model as GeminiModel, SetupConfig};

pub use ::gemini_live::types::Role;
use crate::config::{ScenarioConfig, ServerConfig};
use crate::error::Result;

/// Bridge-facing event stream produced by a running session. This is kutsu's
/// own enum (the bridge/engine consume it); its *source* is the crate's
/// `Event`, mapped in [`run_driver`].
#[derive(Debug)]
pub enum Event {
    OutputAudio(Vec<i16>),
    Transcript { role: Role, text: String, final_: bool },
    Interrupted,
    TurnComplete,
    EndCall { goal: Value },
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TranscriptEntry {
    pub role: Role,
    pub text: String,
    pub ts_ms: u64,
}

/// Accumulates streaming transcription deltas per role.
///
/// Gemini streams `outputTranscription`/`inputTranscription` as incremental
/// text pieces with no per-piece "finished" flag; a turn's full transcript is
/// the concatenation of its deltas, emitted at `TurnComplete` (this mirrors the
/// proven voice-cloud client, which appends deltas to per-role buffers and
/// joins them on `turn_complete`). Relying on a `finished` flag drops
/// everything, since real messages never set it.
#[derive(Default)]
struct TranscriptAccumulator {
    user: String,
    model: String,
}

impl TranscriptAccumulator {
    fn on_delta(&mut self, role: Role, text: &str) {
        match role {
            Role::User => self.user.push_str(text),
            Role::Model => self.model.push_str(text),
        }
    }

    /// Emit the completed turn's entries (user first, then model) and reset the
    /// buffers. Call on `TurnComplete`, and once more at session end to capture a
    /// turn cut short by end_call/hangup. Whitespace-only buffers yield nothing.
    fn flush(&mut self, ts_ms: u64) -> Vec<TranscriptEntry> {
        let mut out = Vec::new();
        let u = self.user.trim();
        if !u.is_empty() {
            out.push(TranscriptEntry { role: Role::User, text: u.to_string(), ts_ms });
        }
        let m = self.model.trim();
        if !m.is_empty() {
            out.push(TranscriptEntry { role: Role::Model, text: m.to_string(), ts_ms });
        }
        self.user.clear();
        self.model.clear();
        out
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndedBy {
    ModelEndCall,
    CallerHangup,
    RemoteClose,
    Error,
}

#[derive(Debug)]
pub struct CallOutcome {
    pub ended_by: EndedBy,
    pub goal: Option<Value>,
    pub transcript: Vec<TranscriptEntry>,
}

/// Kickoff cue sent as a user turn when the callee stays silent. It only hands
/// the turn to the model — the greeting wording comes from the system prompt.
const GREET_CUE: &str =
    "The call has connected and the other party has not spoken yet. \
     Greet them now and begin the conversation as instructed.";

/// Map kutsu's config-level model onto the crate's model enum. `pub(crate)` so
/// the network preflight (`net_check`) builds the same endpoint URL the session
/// uses via the crate's `endpoint_url`.
pub(crate) fn map_model(m: crate::config::Model) -> GeminiModel {
    match m {
        crate::config::Model::HalfCascade => GeminiModel::HalfCascade,
        crate::config::Model::NativeAudio => GeminiModel::NativeAudio,
    }
}

/// Assemble the crate's `ClientConfig` from kutsu's `ServerConfig` + the
/// fully-built system prompt. The structured `language` is only meaningful on
/// half-cascade (native-audio ignores it — the language directive is carried in
/// the system prompt instead); `resume_handle` is always `None` (kutsu opens a
/// fresh session per call and lets the crate manage resumption thereafter).
fn client_config(server: &ServerConfig, scenario: &ScenarioConfig) -> ClientConfig {
    let model = map_model(server.model);
    let native = model.is_native();
    let setup = SetupConfig {
        model,
        voice: server.voice.clone(),
        language: if native { None } else { Some(server.language.clone()) },
        system_instruction: crate::proto::assemble_system_instruction(server, scenario),
        temperature: 0.8,
        goal_schema: scenario.goal_schema.clone(),
        resume_handle: None,
    };
    ClientConfig {
        model,
        api_key: server.api_key.clone(),
        proxy: server.proxy.as_ref().map(|p| ProxyConfig {
            url: p.url.clone(),
            user: p.user.clone(),
            password: p.password.clone(),
        }),
        setup,
        // Unbounded reconnect (backoff caps at 5s); the call-level hangup /
        // max_call_secs is the only give-up gate, matching the old loop.
        max_reconnect_attempts: None,
    }
}

/// Drive one call over an already-connected crate [`Session`]. This is the whole
/// of kutsu's per-call orchestration: a single `select!` over the crate's event
/// stream, the uplink audio, the greeting timer, the answer signal, and hangup —
/// preserving the old `run_session` semantics with the connection lifecycle now
/// sourced from the crate.
///
/// The crate `Session` owns its transport + reconnect loop in an internal task,
/// so this reads `recv_event()` (cancel-safe) and enqueues via `send_*(&self)`.
/// Tests drive it over a scripted `FakeTransport`-backed `Session`.
async fn run_driver(
    mut session: CrateSession,
    server: &ServerConfig,
    mut answered: watch::Receiver<bool>,
    mut audio_in: mpsc::Receiver<Vec<i16>>,
    events: mpsc::Sender<Event>,
    mut hangup_rx: mpsc::Receiver<()>,
) -> CallOutcome {
    use std::time::Duration;

    let clock = tokio::time::Instant::now();
    let mut transcript: Vec<TranscriptEntry> = Vec::new();
    let mut acc = TranscriptAccumulator::default();

    // Hybrid greeting: on a fresh session, if neither side produces anything
    // within `greet_after_silence_ms`, prompt the model to greet first. `0`
    // disables the proactive greeting (purely reactive). A fresh call never
    // resumes, so `greeted` starts true only when greeting is disabled.
    let greet_enabled = server.greet_after_silence_ms > 0;
    let mut greeted = !greet_enabled;
    let mut had_activity = false;
    // Shared-across-a-call state is now just locals (one driver == one call);
    // the crate owns reconnection, so these need no cross-attempt `Arc`s.
    let mut callee_active = false;
    let mut resume_needed = false;
    let mut is_reconnect_session = false;

    // Greeting arms only after `answered` — never during ring. `greet_armed`
    // gates the timer; `greet_deadline` is meaningless until armed.
    let mut greet_armed = *answered.borrow();
    let mut greet_deadline = if greet_armed {
        tokio::time::Instant::now() + Duration::from_millis(server.greet_after_silence_ms)
    } else {
        // Far-future placeholder; the `greet_armed` guard is what keeps the
        // timer inert until the call is answered.
        tokio::time::Instant::now() + Duration::from_secs(86_400)
    };
    // Latches once the `answered` sender is dropped without ever answering, so
    // the arming arm's guard turns permanently false (no busy-spin).
    let mut answered_closed = false;

    // Per-call energy VAD over incoming callee audio; the shared `callee_active`
    // flag persists across the call.
    let mut vad = crate::vad::Vad::new(server.vad);
    let mut audio_frames_sent: u64 = 0;

    let (ended_by, goal): (EndedBy, Option<Value>) = loop {
        tokio::select! {
            // Arm the greeting the moment the call is answered (once).
            changed = answered.changed(), if !greet_armed && !greeted && !answered_closed => {
                match changed {
                    Ok(()) if *answered.borrow() => {
                        greet_armed = true;
                        greet_deadline = tokio::time::Instant::now()
                            + Duration::from_millis(server.greet_after_silence_ms);
                    }
                    Ok(()) => {} // changed but still false (harmless)
                    Err(_) => { answered_closed = true; } // sender dropped: never greet
                }
            }
            // Callee silent past the greet delay: hand the model the first turn.
            _ = tokio::time::sleep_until(greet_deadline),
                if greet_armed && !greeted && !had_activity && !callee_active && !is_reconnect_session => {
                greeted = true;
                tracing::info!("gemini: callee silent — sending greeting kickoff");
                if session.send_client_text(GREET_CUE).await.is_err() {
                    break (EndedBy::RemoteClose, None);
                }
            }
            // Uplink: VAD only READS the frame; the audio forwarded to Gemini is
            // byte-transparent (no alter/delay/drop).
            frame = audio_in.recv() => {
                match frame {
                    Some(pcm) => {
                        if vad.observe(&pcm) {
                            callee_active = true;
                            resume_needed = true;
                            tracing::info!(
                                rms = vad.last_rms(),
                                floor = vad.noise_floor(),
                                "gemini: callee speech onset — greeting suppressed"
                            );
                        }
                        if session.send_audio(&pcm).await.is_err() {
                            break (EndedBy::RemoteClose, None);
                        }
                        audio_frames_sent += 1;
                        if audio_frames_sent % 100 == 0 {
                            tracing::debug!(audio_frames_sent, "gemini: audio streaming");
                        }
                    }
                    None => break (EndedBy::CallerHangup, None), // caller dropped the sender
                }
            }
            // Downlink: the crate's unified event stream (spans reconnects).
            // `recv_event` is a cancel-safe mpsc receive — racing it here can
            // never disturb the reconnect loop (which runs in the crate's task).
            ev = session.recv_event() => {
                match ev {
                    Some(CrateEvent::SessionOpened { is_reconnect, resumed }) => {
                        if is_reconnect {
                            is_reconnect_session = true;
                            // 1. Drop the stale uplink backlog (frames queued
                            //    during the outage are old callee audio the
                            //    reopened model must never hear).
                            let mut drained = 0u32;
                            while audio_in.try_recv().is_ok() {
                                drained += 1;
                            }
                            tracing::info!(drained, "gemini reconnect: dropped stale uplink frames");
                            // 2. Flush the pacer: the bridge clears the downlink
                            //    buffer on `Interrupted`, discarding mid-played audio.
                            let _ = events.send(Event::Interrupted).await;
                            // 3. RESUME_CUE only when context was genuinely lost:
                            //    the reopen did NOT resume from a stored handle
                            //    AND a turn was pending mid-exchange.
                            if !resumed && resume_needed {
                                tracing::info!(
                                    resumed,
                                    resume_needed = true,
                                    "gemini reconnect: context lost mid-exchange — sending RESUME_CUE"
                                );
                                if session.send_client_text(&server.resume_cue).await.is_err() {
                                    break (EndedBy::RemoteClose, None);
                                }
                            }
                            // A reopened session must never re-greet.
                            greeted = true;
                        }
                    }
                    Some(CrateEvent::OutputAudio(pcm)) => {
                        had_activity = true;
                        let _ = events.send(Event::OutputAudio(pcm)).await;
                    }
                    Some(CrateEvent::Transcript { role, text, final_ }) => {
                        had_activity = true;
                        // Belt-and-suspenders: an ASR user turn is a callee-active
                        // signal even if energy VAD missed a from-frame-one onset.
                        if role == Role::User {
                            callee_active = true;
                            resume_needed = true;
                        }
                        acc.on_delta(role, &text);
                        let _ = events.send(Event::Transcript { role, text, final_ }).await;
                    }
                    // Affective structured events are not consumed yet.
                    Some(CrateEvent::Affect { role, label }) => {
                        tracing::debug!(?role, ?label, "gemini: affect event (ignored)");
                    }
                    Some(CrateEvent::Interrupted) => {
                        had_activity = true;
                        let _ = events.send(Event::Interrupted).await;
                    }
                    Some(CrateEvent::TurnComplete) => {
                        had_activity = true;
                        resume_needed = false;
                        let ts_ms = clock.elapsed().as_millis() as u64;
                        let entries = acc.flush(ts_ms);
                        if !entries.is_empty() {
                            transcript.extend(entries);
                        }
                        let _ = events.send(Event::TurnComplete).await;
                    }
                    Some(CrateEvent::ToolCall { name, id, args }) => {
                        if name == "end_call" {
                            let _ = session.send_tool_response(&id).await;
                            let _ = events.send(Event::EndCall { goal: args.clone() }).await;
                            break (EndedBy::ModelEndCall, Some(args));
                        }
                        // Other tools: ignored (kutsu only wires end_call).
                        tracing::debug!(%name, "gemini: unhandled tool call (ignored)");
                    }
                    // A terminal close (reconnection exhausted / unrecoverable)
                    // or the end of the stream ends the call.
                    Some(CrateEvent::SessionClosed { .. }) | None => {
                        break (EndedBy::RemoteClose, None);
                    }
                }
            }
            _ = hangup_rx.recv() => break (EndedBy::CallerHangup, None),
        }
    };

    // Flush whatever the last, unterminated turn accumulated (a turn cut short
    // by end_call/hangup).
    let ts_ms = clock.elapsed().as_millis() as u64;
    let entries = acc.flush(ts_ms);
    if !entries.is_empty() {
        transcript.extend(entries);
    }

    CallOutcome { ended_by, goal, transcript }
}

/// Connect (warm) to Gemini Live and drive the call. `Session::connect` is the
/// warm connect the engine overlaps with the SIP ring window.
async fn connect_and_drive(
    server: ServerConfig,
    scenario: ScenarioConfig,
    answered: watch::Receiver<bool>,
    audio_in: mpsc::Receiver<Vec<i16>>,
    events: mpsc::Sender<Event>,
    hangup_rx: mpsc::Receiver<()>,
) -> CallOutcome {
    let cfg = client_config(&server, &scenario);
    tracing::debug!(model = ?server.model, "gemini: connecting");
    let session = match CrateSession::connect(cfg).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "gemini: initial connect failed");
            return CallOutcome { ended_by: EndedBy::Error, goal: None, transcript: Vec::new() };
        }
    };
    run_driver(session, &server, answered, audio_in, events, hangup_rx).await
}

/// Connect + drive one call. The event/audio channels stay stable for the life
/// of the call; the crate handles reconnect/resumption transparently underneath.
///
/// `start` returns immediately with the [`Session`] handle (the connect runs in
/// the spawned task), so the engine's warm-start overlaps the connect with the
/// SIP ring. `answered` arms the greeting only once the call is answered.
pub async fn start(
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    answered: watch::Receiver<bool>,
) -> Result<Session> {
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<i16>>(64);
    let (event_tx, event_rx) = mpsc::channel::<Event>(256);
    let (hangup_tx, hangup_rx) = mpsc::channel::<()>(1);

    let server = server.clone();
    let scenario = scenario.clone();

    let join = tokio::spawn(connect_and_drive(server, scenario, answered, audio_rx, event_tx, hangup_rx));

    Ok(Session { audio_in: audio_tx, events: event_rx, join, hangup_tx })
}

/// Public session handle returned by `start`.
pub struct Session {
    pub audio_in: mpsc::Sender<Vec<i16>>,
    pub events: mpsc::Receiver<Event>,
    join: tokio::task::JoinHandle<CallOutcome>,
    hangup_tx: mpsc::Sender<()>,
}

impl Session {
    pub async fn hangup(&self) {
        let _ = self.hangup_tx.send(()).await;
    }
    pub async fn join(self) -> CallOutcome {
        self.join.await.unwrap_or(CallOutcome {
            ended_by: EndedBy::Error, goal: None, transcript: Vec::new(),
        })
    }

    /// Decompose into a control handle + the audio sink + the event stream.
    pub fn split(self) -> (SessionHandle, mpsc::Sender<Vec<i16>>, mpsc::Receiver<Event>) {
        (
            SessionHandle { join: self.join, hangup_tx: self.hangup_tx },
            self.audio_in,
            self.events,
        )
    }
}

/// Control handle for a split-off `Session`: hang up + await the outcome.
pub struct SessionHandle {
    join: tokio::task::JoinHandle<CallOutcome>,
    hangup_tx: mpsc::Sender<()>,
}

impl SessionHandle {
    pub async fn hangup(&self) {
        let _ = self.hangup_tx.send(()).await;
    }
    pub async fn join(self) -> CallOutcome {
        self.join.await.unwrap_or(CallOutcome {
            ended_by: EndedBy::Error,
            goal: None,
            transcript: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use ::gemini_live::session::{Reconnector, SessionError};
    use ::gemini_live::transport::{FakeTransport, TransportError};
    use ::gemini_live::types::Model as GModel;
    use tokio::sync::mpsc;

    fn server() -> ServerConfig {
        ServerConfig { api_key: "K".into(), proxy: None, model: Model::HalfCascade,
            voice: "Autonoe".into(), voice_gender: crate::config::Gender::Female, language: "en-US".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 3,
            greet_after_silence_ms: 0, transcript_dir: None, dump_uplink_dir: None, dump_downlink_dir: None,
            max_call_secs: 600, quality: crate::config::QualityConfig::default(),
            retry: crate::config::RetryConfig::default(),
            vad: VadConfig::default(), resume_cue: RESUME_CUE.into() }
    }

    /// A crate `SetupConfig` for tests (its content is irrelevant to the driver
    /// assertions; RESUME_CUE/GREET_CUE are sourced from `ServerConfig`).
    fn setup() -> SetupConfig {
        SetupConfig {
            model: GModel::HalfCascade, voice: "Autonoe".into(), language: Some("en-US".into()),
            system_instruction: "hi".into(), temperature: 0.8,
            goal_schema: serde_json::json!({"type":"object"}), resume_handle: None,
        }
    }

    fn cfg() -> ClientConfig {
        ClientConfig { model: GModel::HalfCascade, api_key: "K".into(), proxy: None,
            setup: setup(), max_reconnect_attempts: None }
    }

    /// A reconnector that never yields a transport (reconnection fails).
    fn no_reconnect() -> Reconnector<FakeTransport> {
        Box::new(|| {
            Box::pin(async {
                Err(SessionError::Transport(TransportError::Connect("no reconnect".into())))
            })
        })
    }

    /// A reconnector that hands back `next` exactly once, then fails.
    fn once(next: FakeTransport) -> Reconnector<FakeTransport> {
        let mut slot = Some(next);
        Box::new(move || {
            let t = slot.take();
            Box::pin(async move {
                t.ok_or(SessionError::Transport(TransportError::Connect("drained".into())))
            })
        })
    }

    /// Build a crate `Session` over a scripted transport + reconnector.
    async fn session_over(
        fake: FakeTransport,
        reconnect: Reconnector<FakeTransport>,
    ) -> CrateSession {
        CrateSession::connect_with_transport(cfg(), fake, reconnect).await.unwrap()
    }

    // --- Session handle plumbing ---------------------------------------------

    #[tokio::test]
    async fn session_split_yields_control_and_channels() {
        let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<i16>>(4);
        let (event_tx, event_rx) = mpsc::channel::<Event>(4);
        let (hangup_tx, mut hangup_rx) = mpsc::channel::<()>(1);
        let join = tokio::spawn(async {
            CallOutcome { ended_by: EndedBy::RemoteClose, goal: None, transcript: vec![] }
        });
        let session = Session { audio_in: audio_tx, events: event_rx, join, hangup_tx };

        let (handle, gemini_in, mut gemini_events) = session.split();
        gemini_in.send(vec![0i16; 4]).await.unwrap();
        assert!(audio_rx.recv().await.is_some());
        event_tx.send(Event::TurnComplete).await.unwrap();
        assert!(matches!(gemini_events.recv().await, Some(Event::TurnComplete)));
        handle.hangup().await;
        assert!(hangup_rx.recv().await.is_some());
        let outcome = handle.join().await;
        assert!(matches!(outcome.ended_by, EndedBy::RemoteClose));
    }

    #[test]
    fn transcript_accumulator_concatenates_deltas_and_flushes_per_turn() {
        let mut acc = TranscriptAccumulator::default();
        acc.on_delta(Role::User, "Да");
        acc.on_delta(Role::User, ", удобно");
        acc.on_delta(Role::Model, "Здравствуй");
        acc.on_delta(Role::Model, "те!");
        let t1 = acc.flush(100);
        assert_eq!(t1.len(), 2);
        assert_eq!(t1[0].role, Role::User);
        assert_eq!(t1[0].text, "Да, удобно");
        assert_eq!(t1[1].role, Role::Model);
        assert_eq!(t1[1].text, "Здравствуйте!");
        assert!(acc.flush(200).is_empty());
        acc.on_delta(Role::Model, "До свидания");
        let t2 = acc.flush(300);
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].role, Role::Model);
        assert_eq!(t2[0].text, "До свидания");
    }

    // --- end_call + transcript flow (was run_session's transcript_and_end_call) --

    #[tokio::test]
    async fn transcript_and_end_call_flow() {
        // Live connection (pending_when_empty): after the scripted frames the
        // transport stays open, mirroring production — so the end_call ack is
        // written in the live phase, not discarded by a premature reconnect.
        let mut fake = FakeTransport::new(true);
        fake.push_data(br#"{"setupComplete":{}}"#.to_vec());
        fake.push_data(
            br#"{"serverContent":{"outputTranscription":{"text":"Hi there","finished":true}}}"#.to_vec());
        fake.push_data(
            br#"{"toolCall":{"functionCalls":[{"id":"c1","name":"end_call","args":{"disposition":"done"}}]}}"#.to_vec());
        let sent = fake.sent.clone();
        let session = session_over(fake, no_reconnect()).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(true);

        let outcome = run_driver(session, &server(), answered_rx, arx, etx, hrx).await;

        // First sent frame is the setup message (sent by the crate on connect).
        assert!(sent.lock().unwrap()[0].contains("\"setup\""));
        let mut saw_transcript = false;
        let mut saw_end = false;
        while let Ok(ev) = erx.try_recv() {
            match ev {
                Event::Transcript { role: Role::Model, text, .. } if text == "Hi there" => saw_transcript = true,
                Event::EndCall { goal } => { assert_eq!(goal["disposition"], "done"); saw_end = true; }
                _ => {}
            }
        }
        assert!(saw_transcript);
        assert!(saw_end);
        assert!(matches!(outcome.ended_by, EndedBy::ModelEndCall));
        assert_eq!(outcome.goal.as_ref().unwrap()["disposition"], "done");
        // The model turn was folded into the outcome transcript at end.
        assert!(outcome.transcript.iter().any(|e| e.role == Role::Model && e.text == "Hi there"));
        // The end_call ack is enqueued to the crate's driver task and written
        // asynchronously; after run_driver drops the session the detached task
        // drains the queued command and writes it before exiting.
        let mut acked = false;
        for _ in 0..1000 {
            if sent.lock().unwrap().iter().any(|s| s.contains("tool_response")) {
                acked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(acked, "the end_call tool ack must be written before teardown");
    }

    // --- answered gate: greeting must never fire before the call is answered. --

    #[tokio::test(start_paused = true)]
    async fn no_greeting_before_answered() {
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        // pending_when_empty: a live connection where the callee never talks (not
        // one that closes), so the greet timer can be reached without the session
        // ending via a remote close first.
        let fake = FakeTransport::new(true);
        let sent = fake.sent.clone();
        let session = session_over(fake, no_reconnect()).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, _erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(false);

        let run = tokio::spawn(async move {
            run_driver(session, &srv, answered_rx, arx, etx, hrx).await
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        assert!(
            !sent.lock().unwrap().iter().any(|s| s.contains(GREET_CUE)),
            "no GREET_CUE must be sent while answered is false"
        );
        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn greets_after_answered_when_callee_silent() {
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        let fake = FakeTransport::new(true);
        let sent = fake.sent.clone();
        let session = session_over(fake, no_reconnect()).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, _erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (answered_tx, answered_rx) = watch::channel(false);

        let run = tokio::spawn(async move {
            run_driver(session, &srv, answered_rx, arx, etx, hrx).await
        });

        tokio::task::yield_now().await;
        answered_tx.send(true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let greet_count = sent.lock().unwrap().iter().filter(|s| s.contains(GREET_CUE)).count();
        assert_eq!(greet_count, 1, "exactly one GREET_CUE must be sent after answered");
        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn greeting_suppressed_when_callee_speaks() {
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        let fake = FakeTransport::new(true);
        let sent = fake.sent.clone();
        let session = session_over(fake, no_reconnect()).await;

        let (atx, arx) = mpsc::channel::<Vec<i16>>(16);
        let (etx, _erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (answered_tx, answered_rx) = watch::channel(false);

        let run = tokio::spawn(async move {
            run_driver(session, &srv, answered_rx, arx, etx, hrx).await
        });

        tokio::task::yield_now().await;
        answered_tx.send(true).unwrap();
        // Quiet frames first to seed the VAD floor, then a loud burst (onset).
        for _ in 0..2 { atx.send(vec![0i16; 320]).await.unwrap(); }
        for _ in 0..6 { atx.send(vec![6000i16; 320]).await.unwrap(); }
        tokio::task::yield_now().await;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !sent.lock().unwrap().iter().any(|s| s.contains(GREET_CUE)),
            "no GREET_CUE must be sent once the callee has spoken"
        );
        run.abort();
    }

    // --- reconnect recovery: drain / flush / RESUME_CUE, driven by the crate's
    // transparent reopen (SessionOpened { is_reconnect: true, resumed }). --------

    #[tokio::test(start_paused = true)]
    async fn reconnect_lost_context_sends_resume_cue() {
        // First transport: setupComplete, a user turn (sets resume_needed), then
        // a resumable close WITHOUT a stored handle -> the reopen is fresh
        // (resumed=false), so context was lost -> RESUME_CUE must be sent.
        let mut first = FakeTransport::new(false);
        first.push_data(br#"{"setupComplete":{}}"#.to_vec());
        first.push_data(br#"{"serverContent":{"inputTranscription":{"text":"hi","finished":false}}}"#.to_vec());
        first.push_close(1011, "server restart");
        // Second transport stays open so the session doesn't immediately end.
        let second = FakeTransport::new(true);
        let second_sent = second.sent.clone();
        let session = session_over(first, once(second)).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(true);

        let run = tokio::spawn(async move {
            run_driver(session, &server(), answered_rx, arx, etx, hrx).await
        });

        // Wait for the reconnect flush (Interrupted) to arrive, then assert the
        // RESUME_CUE landed on the reopened transport.
        let mut interrupted = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(50), erx.recv()).await {
                Ok(Some(Event::Interrupted)) => { interrupted += 1; break; }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert_eq!(interrupted, 1, "exactly one Interrupted flush on reconnect");
        let expected = client_content(&server().resume_cue);
        assert!(
            second_sent.lock().unwrap().iter().any(|s| *s == expected),
            "RESUME_CUE must be sent on lost context"
        );
        assert!(
            !second_sent.lock().unwrap().iter().any(|s| s.contains(GREET_CUE)),
            "no GREET_CUE on reconnect"
        );
        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_with_handle_sends_no_cue() {
        // A stored resumption handle means the reopen resumes (resumed=true):
        // the model restores context itself, so no cue of any kind is sent.
        let mut first = FakeTransport::new(false);
        first.push_data(br#"{"setupComplete":{}}"#.to_vec());
        first.push_data(br#"{"serverContent":{"inputTranscription":{"text":"hi","finished":false}}}"#.to_vec());
        first.push_data(br#"{"sessionResumptionUpdate":{"newHandle":"H1"}}"#.to_vec());
        first.push_close(1011, "server restart");
        let second = FakeTransport::new(true);
        let second_sent = second.sent.clone();
        let session = session_over(first, once(second)).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(true);

        let run = tokio::spawn(async move {
            run_driver(session, &server(), answered_rx, arx, etx, hrx).await
        });

        let mut interrupted = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(50), erx.recv()).await {
                Ok(Some(Event::Interrupted)) => { interrupted += 1; break; }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert_eq!(interrupted, 1, "the reconnect still flushes the pacer once");
        let cue = client_content(&server().resume_cue);
        // The reopened setup carries the handle, but no client-content cue is sent.
        assert!(
            !second_sent.lock().unwrap().iter().any(|s| *s == cue),
            "no RESUME_CUE when the reopen resumed from a stored handle"
        );
        assert!(
            !second_sent.lock().unwrap().iter().any(|s| s.contains(GREET_CUE)),
            "no GREET_CUE on reconnect"
        );
        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_without_pending_turn_sends_no_cue() {
        // No handle, but no turn was pending (no user activity): resume_needed
        // stays false, so nothing was lost -> no cue.
        let mut first = FakeTransport::new(false);
        first.push_data(br#"{"setupComplete":{}}"#.to_vec());
        first.push_close(1011, "server restart");
        let second = FakeTransport::new(true);
        let second_sent = second.sent.clone();
        let session = session_over(first, once(second)).await;

        let (_atx, arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(true);

        let run = tokio::spawn(async move {
            run_driver(session, &server(), answered_rx, arx, etx, hrx).await
        });

        let mut interrupted = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(50), erx.recv()).await {
                Ok(Some(Event::Interrupted)) => { interrupted += 1; break; }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert_eq!(interrupted, 1, "the reconnect still flushes the pacer once");
        let cue = client_content(&server().resume_cue);
        assert!(
            !second_sent.lock().unwrap().iter().any(|s| *s == cue),
            "no RESUME_CUE without a pending turn"
        );
        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_drains_stale_uplink() {
        // Frames queued at reopen time must be drained (never forwarded as
        // realtime input on the reopened transport), and exactly one Interrupted
        // flush emitted. Feed the stale frames only AFTER the reopen, gated on
        // the Interrupted signal, so the drain window is deterministic.
        let mut first = FakeTransport::new(false);
        first.push_data(br#"{"setupComplete":{}}"#.to_vec());
        first.push_close(1011, "server restart");
        let second = FakeTransport::new(true);
        let second_sent = second.sent.clone();
        let session = session_over(first, once(second)).await;

        let (atx, arx) = mpsc::channel::<Vec<i16>>(16);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        let (_htx, hrx) = mpsc::channel::<()>(1);
        let (_answered_tx, answered_rx) = watch::channel(true);

        let run = tokio::spawn(async move {
            run_driver(session, &server(), answered_rx, arx, etx, hrx).await
        });

        // The Interrupted flush is emitted synchronously in the reopen handler,
        // immediately after the uplink drain. Any frames still queued at that
        // point were dropped; so we assert none were forwarded as realtime input.
        // (The drain runs on the buffered backlog at reopen; here the backlog is
        // whatever was queued before the reopen was processed.)
        for _ in 0..5 { atx.send(vec![1234i16; 320]).await.unwrap(); }

        let mut interrupted = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(50), erx.recv()).await {
                Ok(Some(Event::Interrupted)) => { interrupted += 1; }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => if interrupted > 0 { break },
            }
        }
        assert_eq!(interrupted, 1, "exactly one Interrupted flush on reconnect");
        // Any frame that was drained must NOT appear as realtime_input on the
        // reopened transport.
        drop(atx);
        run.abort();
        assert!(
            !second_sent.lock().unwrap().iter().any(|s| s.contains("realtime_input")),
            "stale uplink frames must be drained, not forwarded on the reopened transport"
        );
    }

    /// Local mirror of the crate's `build_client_content` wire shape, for
    /// asserting exactly what RESUME_CUE serializes to.
    fn client_content(text: &str) -> String {
        serde_json::json!({
            "client_content": {
                "turns": [ { "role": "user", "parts": [ { "text": text } ] } ],
                "turnComplete": true
            }
        })
        .to_string()
    }
}
