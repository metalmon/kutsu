//! Gemini Live (`BidiGenerateContent`) WebSocket client.
//!
//! A hand-rolled `tokio-tungstenite` client speaking the `BidiGenerateContent` protocol:
//! streams PCM16 audio both ways in realtime, surfaces events (transcript, audio, tool calls, barge-in),
//! handles session resumption and reconnect, and exposes a single `end_call` tool whose parameters
//! are the scenario's dynamic goal schema.
//!
//! Kept free of any project-specific dependencies so it could later be
//! shared with zeroclaw's proposed `speech_to_speech` channel
//! (<https://github.com/zeroclaw-labs/zeroclaw/issues/8780>), which needs
//! the same protocol implementation.
//!

use serde_json::Value;
use tokio::sync::mpsc;

pub use crate::proto::Role;
use crate::config::{ScenarioConfig, ServerConfig};
use crate::error::Result;
use crate::proto::{self, ServerEvent};

#[derive(Debug)]
pub enum Event {
    OutputAudio(Vec<i16>),
    Transcript { role: Role, text: String, final_: bool },
    Interrupted,
    TurnComplete,
    EndCall { goal: Value },
    Warning(String),
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

/// How a single session attempt ended (drives the reconnect loop).
#[derive(Debug)]
pub enum SessionEnd {
    EndCall(Value),
    Hangup,
    RemoteClose,
    Resumable(String), // reason; retry with the current handle
}

/// Pure reconnect bookkeeping policy for one finished attempt: given how it
/// ended and whether it made real progress (reached `SetupComplete` or
/// produced events), decide whether the outer loop should record a success
/// (reset backoff) or a failure (advance `ReconnectState`, which may signal
/// dropping the resumption handle) before retrying.
///
/// Returns `None` for the two terminal endings (`EndCall`/`Hangup`); the
/// caller stops the loop for those without touching reconnect state at all.
/// `Resumable(_)` is unconditionally a failure — a WS session that dies with
/// a protocol/transport error never counts as progress-bearing success, even
/// if some prior activity happened. A `RemoteClose` after real progress (a
/// session that was actually live) is a success — it should reconnect fast;
/// a `RemoteClose` with no progress at all (e.g. the connection closes
/// immediately) is treated the same as any other non-progressing failure, so
/// a consistently-broken endpoint still escalates backoff and eventually
/// drops a possibly-stale handle instead of retrying at the base delay
/// forever.
pub(crate) fn reconnect_outcome(end: &SessionEnd, progressed: bool) -> Option<bool> {
    match end {
        SessionEnd::EndCall(_) | SessionEnd::Hangup => None,
        SessionEnd::Resumable(_) => Some(false),
        SessionEnd::RemoteClose => Some(progressed),
    }
}

/// A text-frame transport. Real impl = tokio-tungstenite; tests use a fake.
pub(crate) trait Transport: Send {
    fn send_text(&mut self, s: String) -> impl std::future::Future<Output = Result<()>> + Send;
    fn recv(&mut self) -> impl std::future::Future<Output = Option<Result<String>>> + Send;
}

/// Run one session attempt over `transport`. Sends setup, then pumps audio in and
/// server events out until end_call / hangup / stream close.
///
/// `latest_handle` is set whenever the server sends a fresh session-resumption
/// handle, so the caller's reconnect loop can pick it up regardless of how the
/// attempt ends (including a plain `RemoteClose`). `setup_ok` is set once the
/// server acknowledges `setupComplete`, so the caller can distinguish "the
/// session was actually live" from "it never got off the ground" when a
/// non-clean end needs to feed [`reconnect_outcome`].
/// Kickoff cue sent as a user turn when the callee stays silent. It only hands
/// the turn to the model — the greeting wording comes from the system prompt.
const GREET_CUE: &str =
    "The call has connected and the other party has not spoken yet. \
     Greet them now and begin the conversation as instructed.";

pub(crate) async fn run_session<T: Transport>(
    mut transport: T,
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    resume_handle: Option<String>,
    audio_in: &mut mpsc::Receiver<Vec<i16>>,
    events: &mpsc::Sender<Event>,
    latest_handle: &mut Option<String>,
    setup_ok: &mut bool,
    mut answered: tokio::sync::watch::Receiver<bool>,
    callee_active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    greeted_ever: std::sync::Arc<std::sync::atomic::AtomicBool>,
    resume_needed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    is_reconnect: bool,
) -> SessionEnd {
    use std::sync::atomic::Ordering::Relaxed;
    // 1. Setup.
    let setup = proto::build_setup(server, scenario, resume_handle.as_deref());
    tracing::debug!(model = ?server.model, "gemini: sending setup");
    if transport.send_text(setup.to_string()).await.is_err() {
        return SessionEnd::Resumable("send setup failed".into());
    }

    // Reconnect recovery: a session that reopened after a drop must not carry
    // over the stale uplink backlog nor a half-played downlink, and — only if
    // context was truly lost mid-exchange — must ask the callee to repeat.
    if is_reconnect {
        // 1. Drop the stale uplink backlog (frames queued during the outage
        //    are old callee audio the reopened model must never hear).
        let mut drained = 0u32;
        while audio_in.try_recv().is_ok() {
            drained += 1;
        }
        tracing::info!(drained, "gemini reconnect: dropped stale uplink frames");
        // 2. Flush the pacer: the bridge's barge-in path clears the downlink
        //    buffer on `Interrupted`, discarding any mid-played audio.
        let _ = events.send(Event::Interrupted).await;
        // 3. RESUME_CUE, only when context was genuinely lost: no server-side
        //    resumption handle (the model can't restore context itself) AND a
        //    turn was pending mid-exchange. A resumed handle or no pending turn
        //    needs nothing — the wording comes from the scenario prompt.
        if resume_handle.is_none() && resume_needed.load(Relaxed) {
            tracing::info!("gemini reconnect: context lost mid-exchange — sent RESUME_CUE");
            if transport.send_text(build_client_content(&server.resume_cue)).await.is_err() {
                return SessionEnd::Resumable("send resume cue failed".into());
            }
        }
    }

    // Hybrid greeting: on a fresh session, if neither side has produced anything
    // within `greet_after_silence_ms`, prompt the model to greet first. A value
    // of 0 disables the proactive greeting (purely reactive).
    let greet_enabled = server.greet_after_silence_ms > 0;
    // Never greet on resume, when disabled, on a reconnect, or if this call
    // already greeted in an earlier attempt (persistent `greeted_ever`).
    let mut greeted =
        resume_handle.is_some() || !greet_enabled || greeted_ever.load(Relaxed) || is_reconnect;
    let mut had_activity = false;

    // Greeting arms only after `answered` — never during ring. `greet_armed`
    // gates the timer; `greet_deadline` is meaningless until armed.
    let mut greet_armed = *answered.borrow(); // already answered (e.g. reconnect path) -> arm now
    let mut greet_deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(server.greet_after_silence_ms);
    if !greet_armed {
        // Belt-and-suspenders: the `greet_armed` guard on the select arm below
        // is what actually keeps this timer inert; this far-future placeholder
        // just avoids leaving `greet_deadline` at a meaningless "now" value
        // before it is armed.
        greet_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(86_400);
    }

    // `watch::Receiver::changed()` resolves `Ready(Err(_))` immediately (not
    // pending) once the sender is dropped, and keeps doing so on every
    // subsequent call. If the arming arm below stayed enabled after that, the
    // select loop would busy-spin polling it forever. `answered_closed` latches
    // that outcome so the arm's guard turns permanently false the moment the
    // sender goes away without ever having sent `true`.
    let mut answered_closed = false;

    let mut audio_frames_sent: u64 = 0;
    // Per-attempt energy VAD over the incoming callee audio. A fresh instance
    // per `run_session` is correct: its noise floor / onset counter reset on
    // every reconnect, while the shared `callee_active` flag persists.
    let mut vad = crate::vad::Vad::new(server.vad);
    loop {
        tokio::select! {
            // Arm the greeting the moment the call is answered (once). Once
            // `greet_armed`/`greeted` is true, or the `answered` sender has
            // been dropped without ever answering, this arm's guard is false
            // forever, so it can't busy-loop.
            changed = answered.changed(), if !greet_armed && !greeted && !answered_closed => {
                match changed {
                    Ok(()) if *answered.borrow() => {
                        greet_armed = true;
                        greet_deadline = tokio::time::Instant::now()
                            + std::time::Duration::from_millis(server.greet_after_silence_ms);
                    }
                    Ok(()) => {} // changed but still false (shouldn't happen for a bool that only goes false->true, but harmless)
                    Err(_) => { answered_closed = true; } // sender dropped without answering: never greet
                }
            }
            _ = tokio::time::sleep_until(greet_deadline), if greet_armed && !greeted && !had_activity && !callee_active.load(Relaxed) && !is_reconnect => {
                greeted = true;
                greeted_ever.store(true, Relaxed);
                tracing::info!("gemini: callee silent — sending greeting kickoff");
                if transport.send_text(build_client_content(GREET_CUE)).await.is_err() {
                    return SessionEnd::Resumable("send greeting failed".into());
                }
            }
            frame = audio_in.recv() => {
                match frame {
                    Some(pcm) => {
                        // VAD only READS the frame for RMS; it never alters,
                        // delays, or drops the audio forwarded to Gemini below.
                        if vad.observe(&pcm) {
                            callee_active.store(true, Relaxed);
                            resume_needed.store(true, Relaxed);
                            tracing::info!("gemini: callee speech onset — greeting suppressed");
                        }
                        let msg = build_realtime_input(&pcm);
                        if transport.send_text(msg).await.is_err() {
                            return SessionEnd::Resumable("send audio failed".into());
                        }
                        audio_frames_sent += 1;
                        if audio_frames_sent % 100 == 0 {
                            tracing::debug!(audio_frames_sent, "gemini: audio streaming");
                        }
                    }
                    None => return SessionEnd::Hangup, // caller dropped the sender
                }
            }
            incoming = transport.recv() => {
                match incoming {
                    Some(Ok(text)) => {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            // Collapse the server's pretty-printed JSON to one line
                            // and elide the huge base64 audio blob.
                            let compact: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
                            let shown = match compact.find("\"data\"") {
                                Some(i) => format!("{}\"data\":<+{} b elided>}}", &compact[..i], text.len()),
                                None => compact,
                            };
                            tracing::debug!(len = text.len(), frame = %shown, "gemini: recv frame");
                        }
                        let parsed = match proto::parse_server_message(&text) {
                            Ok(p) => p,
                            Err(e) => { let _ = events.send(Event::Warning(e.to_string())).await; continue; }
                        };
                        for se in parsed {
                            match se {
                                ServerEvent::SetupComplete => { *setup_ok = true; }
                                ServerEvent::OutputAudio(pcm) =>
                                    { had_activity = true; let _ = events.send(Event::OutputAudio(pcm)).await; }
                                ServerEvent::Transcript { role, text, final_ } => {
                                    had_activity = true;
                                    // Belt-and-suspenders: an ASR user turn is a
                                    // callee-active signal even if energy VAD
                                    // missed a from-frame-one speech onset.
                                    if role == Role::User {
                                        callee_active.store(true, Relaxed);
                                        resume_needed.store(true, Relaxed);
                                    }
                                    let _ = events.send(Event::Transcript { role, text, final_ }).await;
                                }
                                ServerEvent::Interrupted =>
                                    { had_activity = true; let _ = events.send(Event::Interrupted).await; }
                                ServerEvent::TurnComplete =>
                                    { had_activity = true; resume_needed.store(false, Relaxed); let _ = events.send(Event::TurnComplete).await; }
                                ServerEvent::ResumptionHandle(h) => { *latest_handle = Some(h); }
                                ServerEvent::GoAway => return SessionEnd::Resumable("goAway".into()),
                                ServerEvent::ToolCallEndCall { call_id, goal } => {
                                    let _ = transport.send_text(build_tool_response(&call_id)).await;
                                    let _ = events.send(Event::EndCall { goal: goal.clone() }).await;
                                    return SessionEnd::EndCall(goal);
                                }
                            }
                        }
                    }
                    Some(Err(_)) => return SessionEnd::Resumable("recv error".into()),
                    None => return SessionEnd::RemoteClose,
                }
            }
        }
    }
}

fn build_realtime_input(pcm: &[i16]) -> String {
    use base64::Engine as _;
    let mut bytes = Vec::with_capacity(pcm.len() * 2);
    for &s in pcm {
        bytes.extend_from_slice(&s.to_le_bytes());
    }
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    serde_json::json!({
        "realtime_input": { "audio": { "mimeType": "audio/pcm;rate=16000", "data": data } }
    })
    .to_string()
}

/// Build a `client_content` message with a single completed user turn. Used as
/// the greeting kickoff so the model takes the first turn when the callee is
/// silent (the greeting wording itself comes from the system prompt).
fn build_client_content(text: &str) -> String {
    serde_json::json!({
        "client_content": {
            "turns": [ { "role": "user", "parts": [ { "text": text } ] } ],
            "turnComplete": true
        }
    })
    .to_string()
}

fn build_tool_response(call_id: &str) -> String {
    serde_json::json!({
        "tool_response": { "functionResponses": [ { "id": call_id, "response": { "ok": true } } ] }
    })
    .to_string()
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

/// Real tokio-tungstenite transport.
struct WsTransport {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Transport for WsTransport {
    async fn send_text(&mut self, s: String) -> Result<()> {
        use futures_util::SinkExt;
        self.ws.send(tokio_tungstenite::tungstenite::Message::Text(s.into()))
            .await.map_err(|e| crate::error::Error::Connect(e.to_string()))
    }
    async fn recv(&mut self) -> Option<Result<String>> {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message;
        loop {
            match self.ws.next().await? {
                // Gemini Live sends server messages as binary WS frames (JSON
                // bytes), not text — accept both.
                Ok(Message::Text(t)) => return Some(Ok(t.to_string())),
                Ok(Message::Binary(b)) => return Some(Ok(String::from_utf8_lossy(&b).into_owned())),
                Ok(Message::Close(_)) => return None,
                Ok(_) => continue, // ignore ping/pong
                Err(e) => return Some(Err(crate::error::Error::Connect(e.to_string()))),
            }
        }
    }
}

/// Connect + run with reconnect. The event/audio channels stay stable across reconnects.
///
/// The transcript is accumulated here (not inside `run_session`) by tapping the
/// event channel with a small forwarding task: every `Event` produced by a
/// session attempt passes through unchanged to the caller, while transcription
/// deltas are accumulated per role and folded into the outcome's transcript one
/// turn at a time (on `TurnComplete`). This keeps `run_session`'s
/// signature/tests focused purely on protocol behavior.
///
/// `hangup()` must interrupt an in-progress call, not just be checked between
/// attempts: every wait point in this loop — the pre-connect gap, the connect
/// attempt itself, a live `run_session`, and the backoff sleep — is raced
/// against the hangup channel via `tokio::select!`. `mpsc::Receiver::recv` is
/// cancel-safe, so re-polling the same `hangup_rx` across many `select!`s over
/// the life of the loop never drops a pending hangup.
///
/// Reconnect bookkeeping (`Backoff`/`ReconnectState`) only escalates on a
/// *non-graceful* attempt, decided by [`reconnect_outcome`]: a plain WS
/// handshake failure or a `SessionEnd::Resumable(_)` (setup/send/recv error,
/// stale-handle `GoAway`, ...) is always a failure; a `RemoteClose` only
/// counts as success if the attempt made real progress (reached
/// `setupComplete`, per `run_session`'s `setup_ok` out-param, or was issued a
/// fresh resumption handle) — otherwise it also escalates, so an endpoint
/// that keeps closing the socket before anything useful happens still ramps
/// backoff and eventually drops a possibly-stale handle instead of retrying
/// at the 300ms base delay forever.
pub async fn start(
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    answered: tokio::sync::watch::Receiver<bool>,
) -> Result<Session> {
    let (audio_tx, audio_rx) = mpsc::channel::<Vec<i16>>(64);
    let (event_tx, event_rx) = mpsc::channel::<Event>(256);
    let (hangup_tx, mut hangup_rx) = mpsc::channel::<()>(1);

    let server = server.clone();
    let scenario = scenario.clone();

    let join = tokio::spawn(async move {
        let mut audio_rx = audio_rx;
        let mut backoff = crate::reconnect::Backoff::new(300, 5000);
        let mut rstate = crate::reconnect::ReconnectState::new(4);
        let mut handle: Option<String> = None;
        let mut transcript: Vec<TranscriptEntry> = Vec::new();
        let clock = tokio::time::Instant::now();

        // Shared across every reconnect attempt of THIS call: whether the
        // callee has spoken, whether we already greeted, and whether the model
        // owes a reply. Each `run_session` gets a clone; the flags outlive
        // individual attempts so a reconnect never re-greets.
        let callee_active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let greeted_ever = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resume_needed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // false on the first attempt, true on every subsequent loop iteration.
        let mut is_reconnect = false;

        loop {
            if hangup_rx.try_recv().is_ok() {
                return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
            }

            let url = proto::endpoint_url(&server);
            let connected = tokio::select! {
                r = crate::proxy::connect_ws(server.proxy.as_ref(), &url) => r,
                _ = hangup_rx.recv() => {
                    return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
                }
            };

            match connected {
                Ok(ws) => {
                    let transport = WsTransport { ws };
                    let prior_handle = handle.clone();
                    let mut latest = handle.clone();
                    let mut setup_ok = false;

                    // Tap: forward every event to the caller unchanged, while
                    // accumulating transcription deltas per role and flushing a
                    // turn's transcript into the local buffer on TurnComplete
                    // (Gemini streams deltas with no per-piece "finished" flag).
                    let (tap_tx, mut tap_rx) = mpsc::channel::<Event>(256);
                    let out_tx = event_tx.clone();
                    let ts_base = clock;
                    let tap_transcript: std::sync::Arc<std::sync::Mutex<Vec<TranscriptEntry>>> =
                        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
                    let tap_transcript_writer = tap_transcript.clone();
                    let forward = tokio::spawn(async move {
                        let mut acc = TranscriptAccumulator::default();
                        while let Some(ev) = tap_rx.recv().await {
                            match &ev {
                                Event::Transcript { role, text, .. } => acc.on_delta(*role, text),
                                Event::TurnComplete => {
                                    let ts_ms = ts_base.elapsed().as_millis() as u64;
                                    let entries = acc.flush(ts_ms);
                                    if !entries.is_empty() {
                                        tap_transcript_writer.lock().unwrap().extend(entries);
                                    }
                                }
                                _ => {}
                            }
                            if out_tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        // Session ended (possibly mid-turn via end_call/hangup):
                        // flush whatever the last, unterminated turn accumulated.
                        let ts_ms = ts_base.elapsed().as_millis() as u64;
                        let entries = acc.flush(ts_ms);
                        if !entries.is_empty() {
                            tap_transcript_writer.lock().unwrap().extend(entries);
                        }
                    });

                    // Race the session attempt against hangup: if hangup wins,
                    // `run_session`'s future (and the `transport`/socket it owns)
                    // is dropped immediately instead of waiting for the call to
                    // end on its own.
                    let end = tokio::select! {
                        e = run_session(
                            transport, &server, &scenario, handle.clone(),
                            &mut audio_rx, &tap_tx, &mut latest, &mut setup_ok,
                            answered.clone(),
                            callee_active.clone(), greeted_ever.clone(), resume_needed.clone(),
                            is_reconnect,
                        ) => Some(e),
                        _ = hangup_rx.recv() => None,
                    };

                    // Same cleanup either way: stop feeding the tap, drain
                    // whatever the forwarder already had, and fold it into the
                    // outcome's transcript.
                    drop(tap_tx);
                    let _ = forward.await;
                    transcript.extend(tap_transcript.lock().unwrap().drain(..));

                    let Some(end) = end else {
                        return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
                    };

                    let got_new_handle = latest.is_some() && latest != prior_handle;
                    let progressed = setup_ok || got_new_handle;
                    handle = latest.or(handle);

                    match end {
                        SessionEnd::EndCall(goal) =>
                            return CallOutcome { ended_by: EndedBy::ModelEndCall, goal: Some(goal), transcript },
                        SessionEnd::Hangup =>
                            return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript },
                        other => {
                            match reconnect_outcome(&other, progressed) {
                                Some(true) => { rstate.on_success(); backoff.reset(); }
                                Some(false) => { if rstate.on_failure() { handle = None; } }
                                None => unreachable!("EndCall/Hangup already handled above"),
                            }
                            let _ = event_tx.send(Event::Warning("reconnecting".into())).await;
                        }
                    }
                }
                Err(_) => {
                    if rstate.on_failure() { handle = None; }
                }
            }

            // Any further loop iteration is a reconnect: never re-greet.
            is_reconnect = true;

            tokio::select! {
                _ = tokio::time::sleep(backoff.next_delay()) => {}
                _ = hangup_rx.recv() => {
                    return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
                }
            }
        }
    });

    Ok(Session { audio_in: audio_tx, events: event_rx, join, hangup_tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use tokio::sync::mpsc;

    struct FakeTransport {
        incoming: std::collections::VecDeque<String>,
        pub sent: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        /// When the queue is drained: `false` (default) mirrors a closed
        /// connection (`recv()` returns `None`, as all the pre-existing tests
        /// expect); `true` mirrors a live connection with nothing new yet
        /// (`recv()` pends forever), needed by the greet-timer gate tests so
        /// the session doesn't end via `RemoteClose` before the timer fires.
        pending_when_empty: bool,
    }

    impl Transport for FakeTransport {
        async fn send_text(&mut self, s: String) -> Result<()> {
            self.sent.lock().unwrap().push(s);
            Ok(())
        }
        async fn recv(&mut self) -> Option<Result<String>> {
            match self.incoming.pop_front() {
                Some(s) => Some(Ok(s)),
                None if self.pending_when_empty => std::future::pending().await,
                None => None,
            }
        }
    }

    fn server() -> ServerConfig {
        ServerConfig { api_key: "K".into(), proxy: None, model: Model::HalfCascade,
            voice: "Autonoe".into(), voice_gender: crate::config::Gender::Female, language: "en-US".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 3,
            greet_after_silence_ms: 0, transcript_dir: None, dump_uplink_dir: None, dump_downlink_dir: None, max_call_secs: 600,
            quality: crate::config::QualityConfig::default(), retry: crate::config::RetryConfig::default(),
            vad: VadConfig::default(), resume_cue: RESUME_CUE.into() }
    }
    fn scenario() -> ScenarioConfig {
        ScenarioConfig { system_prompt: "hi".into(),
            goal_schema: serde_json::json!({"type":"object"}), context: None }
    }

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
        // gemini_in is the audio sender
        gemini_in.send(vec![0i16; 4]).await.unwrap();
        assert!(audio_rx.recv().await.is_some());
        // events receiver moved out
        event_tx.send(Event::TurnComplete).await.unwrap();
        assert!(matches!(gemini_events.recv().await, Some(Event::TurnComplete)));
        // handle can hang up and join
        handle.hangup().await;
        assert!(hangup_rx.recv().await.is_some());
        let outcome = handle.join().await;
        assert!(matches!(outcome.ended_by, EndedBy::RemoteClose));
    }

    #[test]
    fn transcript_accumulator_concatenates_deltas_and_flushes_per_turn() {
        let mut acc = TranscriptAccumulator::default();
        // Turn 1: user speaks in deltas, model replies in deltas.
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
        // Buffers reset after a flush -> an empty flush yields nothing.
        assert!(acc.flush(200).is_empty());
        // Turn 2: model only.
        acc.on_delta(Role::Model, "До свидания");
        let t2 = acc.flush(300);
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].role, Role::Model);
        assert_eq!(t2[0].text, "До свидания");
    }

    #[tokio::test]
    async fn transcript_and_end_call_flow() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"setupComplete":{}}"#.to_string(),
            r#"{"serverContent":{"outputTranscription":{"text":"Hi there","finished":true}}}"#.to_string(),
            r#"{"toolCall":{"functionCalls":[{"id":"c1","name":"end_call","args":{"disposition":"done"}}]}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent: sent.clone(), pending_when_empty: false };

        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);

        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok, answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            false,
        ).await;

        // The server acknowledged setup, so this attempt made real progress.
        assert!(setup_ok);
        // First sent frame is the setup message.
        assert!(sent.lock().unwrap()[0].contains("\"setup\""));
        // A model transcript event was emitted.
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
        assert!(matches!(end, SessionEnd::EndCall(g) if g["disposition"] == "done"));
        // We acked the tool call.
        assert!(sent.lock().unwrap().iter().any(|s| s.contains("tool_response")));
    }

    #[tokio::test]
    async fn captures_resumption_handle_and_remote_close() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"sessionResumptionUpdate":{"newHandle":"H9","resumable":true}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent, pending_when_empty: false };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut latest_handle = None;
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut latest_handle, &mut setup_ok,
            answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            false,
        ).await;
        // No more frames after the update -> remote close.
        assert!(matches!(end, SessionEnd::RemoteClose));
        // The resumption handle from the update was captured for the caller's
        // reconnect loop, even though this attempt ended in a plain close.
        assert_eq!(latest_handle.as_deref(), Some("H9"));
        // No setupComplete frame arrived in this fixture, so the "did this
        // attempt actually get anywhere" signal correctly stays false; the
        // reconnect loop still has the resumption handle as a separate signal.
        assert!(!setup_ok);
    }

    // --- answered gate: the greeting must never fire before the call is
    // answered. `answered` is a level-triggered watch<bool>, so a session that
    // starts before answer (warm-start) must wait for it to flip true before
    // arming the greet timer at all.

    #[tokio::test(start_paused = true)]
    async fn no_greeting_before_answered() {
        // greet_after_silence_ms small; answered stays false the whole time.
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        let incoming = std::collections::VecDeque::new();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        // pending_when_empty: this fixture models a live connection where the
        // callee just never talks, not one that closes — otherwise the
        // session would end via RemoteClose before the greet timer could
        // ever fire, and the test would pass for the wrong reason.
        let transport = FakeTransport { incoming, sent: sent.clone(), pending_when_empty: true };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(false);

        let run = tokio::spawn(async move {
            run_session(
                transport, &srv, &scenario(), None, &mut arx, &etx, &mut None,
                &mut setup_ok, answered_rx,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                false,
            ).await
        });

        // Let the spawned task register its (disarmed) wait, then sleep well
        // past the greet delay; the callee stays silent and `answered` never
        // flips, so no greeting must be sent. `sleep().await` under a paused
        // clock auto-advances to unblock itself once every other task is
        // stalled, which reliably drives the spawned task forward (a bare
        // `tokio::time::advance()` from the driving task does not).
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
        // answered flips true at t0; no model output arrives; after
        // greet_after_silence_ms, expect exactly one GREET_CUE message.
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        let incoming = std::collections::VecDeque::new();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent: sent.clone(), pending_when_empty: true };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (answered_tx, answered_rx) = tokio::sync::watch::channel(false);

        let srv_clone = srv.clone();
        let run = tokio::spawn(async move {
            run_session(
                transport, &srv_clone, &scenario(), None, &mut arx, &etx, &mut None,
                &mut setup_ok, answered_rx,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                false,
            ).await
        });

        // Let the spawned task reach its first await point (registering the
        // `answered.changed()` wait) before we flip the signal.
        tokio::task::yield_now().await;
        answered_tx.send(true).unwrap();
        // `sleep().await` under a paused clock auto-advances time to unblock
        // itself once every other task is stalled — this drives the spawned
        // task's `changed()` wakeup and its subsequently-armed greet timer,
        // unlike a bare `tokio::time::advance()` call from the driving task
        // (which does not reliably re-poll a sibling spawned task's freshly
        // registered timer in this harness).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let greet_count = sent.lock().unwrap().iter().filter(|s| s.contains(GREET_CUE)).count();
        assert_eq!(greet_count, 1, "exactly one GREET_CUE must be sent after answered");

        run.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn greeting_suppressed_when_callee_speaks() {
        // answered flips true at t0, but the callee starts talking BEFORE the
        // greet delay elapses: energy VAD trips `callee_active`, so the greet
        // timer's guard goes false and no GREET_CUE is ever sent.
        let mut srv = server();
        srv.greet_after_silence_ms = 50;
        let incoming = std::collections::VecDeque::new();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent: sent.clone(), pending_when_empty: true };
        let (atx, mut arx) = mpsc::channel::<Vec<i16>>(16);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (answered_tx, answered_rx) = tokio::sync::watch::channel(false);

        let srv_clone = srv.clone();
        let run = tokio::spawn(async move {
            run_session(
                transport, &srv_clone, &scenario(), None, &mut arx, &etx, &mut None,
                &mut setup_ok, answered_rx,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                false,
            ).await
        });

        // Let the spawned task register its waits, then answer the call.
        tokio::task::yield_now().await;
        answered_tx.send(true).unwrap();

        // Feed callee audio BEFORE advancing past the greet delay. The VAD
        // seeds its floor from the FIRST frame, so send quiet frames first to
        // establish a low baseline, then a loud burst (amplitude ~6000) long
        // enough to confirm onset (onset_frames = 3). These frames are queued
        // in the channel and drained before the paused clock auto-advances to
        // the greet deadline, so `callee_active` is set in time to gate it.
        for _ in 0..2 { atx.send(vec![0i16; 320]).await.unwrap(); }
        for _ in 0..6 { atx.send(vec![6000i16; 320]).await.unwrap(); }
        tokio::task::yield_now().await;

        // Now advance well past the greet delay; the guard must stay false.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            !sent.lock().unwrap().iter().any(|s| s.contains(GREET_CUE)),
            "no GREET_CUE must be sent once the callee has spoken"
        );

        run.abort();
    }

    #[tokio::test]
    async fn setup_failure_never_sets_setup_ok() {
        // send_text always succeeds on FakeTransport, so drive the "no frames
        // at all" case: the connection drops before even setupComplete.
        let incoming = std::collections::VecDeque::new();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent, pending_when_empty: false };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok, answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            false,
        ).await;
        assert!(matches!(end, SessionEnd::RemoteClose));
        assert!(!setup_ok);
    }

    // --- reconnect_outcome: pure policy driving the reconnect loop's
    // backoff/handle-drop bookkeeping. Covers the two defects fixed here:
    // (1) a session that dies right after connect (no progress) must
    // escalate exactly like a bare connect failure, not retry forever at the
    // base delay; (2) a session that was actually live before a graceful
    // close must NOT be punished with backoff escalation.

    #[test]
    fn reconnect_outcome_policy() {
        let goal = serde_json::json!({});
        // Terminal endings: caller stops the loop, no bookkeeping at all.
        assert_eq!(reconnect_outcome(&SessionEnd::EndCall(goal), true), None);
        assert_eq!(reconnect_outcome(&SessionEnd::Hangup, true), None);
        // Resumable is unconditionally a failure, regardless of prior progress.
        assert_eq!(reconnect_outcome(&SessionEnd::Resumable("send setup failed".into()), false), Some(false));
        assert_eq!(reconnect_outcome(&SessionEnd::Resumable("goAway".into()), true), Some(false));
        // RemoteClose: success only if the attempt made real progress.
        assert_eq!(reconnect_outcome(&SessionEnd::RemoteClose, true), Some(true));
        assert_eq!(reconnect_outcome(&SessionEnd::RemoteClose, false), Some(false));
    }

    #[test]
    fn repeated_non_progressing_resumable_ends_drop_the_handle_after_four() {
        // Simulates the bug: setup keeps failing (e.g. a stale/rejected
        // resumption handle) with the WS handshake itself always succeeding,
        // so only the post-connect session outcome can ever signal failure.
        let mut rstate = crate::reconnect::ReconnectState::new(4);
        let mut handle: Option<String> = Some("STALE".into());
        for _ in 0..3 {
            match reconnect_outcome(&SessionEnd::Resumable("send setup failed".into()), false) {
                Some(false) => { if rstate.on_failure() { handle = None; } }
                other => panic!("expected Some(false), got {other:?}"),
            }
            assert!(handle.is_some(), "handle must survive fewer than 4 consecutive failures");
        }
        match reconnect_outcome(&SessionEnd::Resumable("send setup failed".into()), false) {
            Some(false) => { if rstate.on_failure() { handle = None; } }
            other => panic!("expected Some(false), got {other:?}"),
        }
        assert!(handle.is_none(), "handle must be dropped on the 4th consecutive non-progressing failure");
    }

    #[test]
    fn progressing_remote_close_resets_reconnect_state() {
        let mut rstate = crate::reconnect::ReconnectState::new(4);
        // A few failures first...
        for _ in 0..3 {
            assert!(!rstate.on_failure());
        }
        // ...then an attempt that reached setupComplete before the socket
        // closed: must be treated as success, resetting the failure count.
        match reconnect_outcome(&SessionEnd::RemoteClose, true) {
            Some(true) => rstate.on_success(),
            other => panic!("expected Some(true), got {other:?}"),
        }
        // Failure count reset -> takes another 4 non-progressing failures to
        // drop the handle again, not just one more.
        assert!(!rstate.on_failure());
        assert!(!rstate.on_failure());
        assert!(!rstate.on_failure());
        assert!(rstate.on_failure());
    }

    // --- reconnect recovery: on a reopened session (`is_reconnect = true`),
    // `run_session` drops the stale uplink backlog, flushes the pacer with an
    // `Interrupted` event, and sends the RESUME_CUE only when context was truly
    // lost mid-exchange (no resumption handle AND a turn was pending).

    #[tokio::test]
    async fn reconnect_lost_context_sends_resume_cue() {
        // No handle + a turn was pending -> the model can't restore context, so
        // it must ask the callee to repeat via the scenario's resume cue.
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport {
            incoming: std::collections::VecDeque::new(),
            sent: sent.clone(),
            pending_when_empty: false,
        };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok,
            answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)), // resume_needed
            true, // is_reconnect
        ).await;
        assert!(matches!(end, SessionEnd::RemoteClose));
        let expected = build_client_content(&server().resume_cue);
        let sent = sent.lock().unwrap();
        assert!(sent.iter().any(|s| *s == expected), "RESUME_CUE must be sent on lost context");
        assert!(!sent.iter().any(|s| s.contains(GREET_CUE)), "no GREET_CUE on reconnect");
    }

    #[tokio::test]
    async fn reconnect_with_handle_sends_no_cue() {
        // A server-side resumption handle means the model restores context
        // itself: no cue of any kind is sent.
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport {
            incoming: std::collections::VecDeque::new(),
            sent: sent.clone(),
            pending_when_empty: false,
        };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let _ = run_session(
            transport, &server(), &scenario(), Some("h".into()), &mut arx, &etx, &mut None,
            &mut setup_ok, answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)), // resume_needed (irrelevant: handle present)
            true, // is_reconnect
        ).await;
        let expected = build_client_content(&server().resume_cue);
        let sent = sent.lock().unwrap();
        assert!(!sent.iter().any(|s| *s == expected), "no RESUME_CUE when a handle is present");
        assert!(!sent.iter().any(|s| s.contains(GREET_CUE)), "no GREET_CUE on reconnect");
    }

    #[tokio::test]
    async fn reconnect_without_pending_turn_sends_no_cue() {
        // No handle, but no turn was pending mid-exchange: nothing was lost, so
        // no cue is sent.
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport {
            incoming: std::collections::VecDeque::new(),
            sent: sent.clone(),
            pending_when_empty: false,
        };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let _ = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok,
            answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)), // resume_needed = false
            true, // is_reconnect
        ).await;
        let expected = build_client_content(&server().resume_cue);
        let sent = sent.lock().unwrap();
        assert!(!sent.iter().any(|s| *s == expected), "no RESUME_CUE without a pending turn");
        assert!(!sent.iter().any(|s| s.contains(GREET_CUE)), "no GREET_CUE on reconnect");
    }

    #[tokio::test]
    async fn reconnect_drains_stale_uplink() {
        // Frames queued during the outage must be dropped, never forwarded as
        // realtime input, and exactly one Interrupted flush must be emitted.
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport {
            incoming: std::collections::VecDeque::new(),
            sent: sent.clone(),
            pending_when_empty: false,
        };
        let (atx, mut arx) = mpsc::channel::<Vec<i16>>(16);
        let (etx, mut erx) = mpsc::channel::<Event>(64);
        // Pre-queue stale frames, then drop the sender so that after the drain
        // the loop's `audio_in.recv()` yields None -> Hangup, ending the session
        // deterministically.
        for _ in 0..5 { atx.send(vec![1234i16; 320]).await.unwrap(); }
        drop(atx);
        let mut setup_ok = false;
        let (_answered_tx, answered_rx) = tokio::sync::watch::channel(true);
        let _ = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok,
            answered_rx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            true, // is_reconnect
        ).await;
        // No realtime-input frame from the drained stale audio was forwarded.
        assert!(
            !sent.lock().unwrap().iter().any(|s| s.contains("realtime_input")),
            "stale uplink frames must be drained, not forwarded"
        );
        // Exactly one Interrupted flush was emitted to the bridge.
        let mut interrupted = 0;
        while let Ok(ev) = erx.try_recv() {
            if matches!(ev, Event::Interrupted) { interrupted += 1; }
        }
        assert_eq!(interrupted, 1, "exactly one Interrupted flush on reconnect");
    }
}
