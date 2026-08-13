//! Gemini Live (`BidiGenerateContent`) WebSocket client.
//!
//! `tokio-tungstenite` client speaking the `BidiGenerateContent` protocol:
//! setup (`systemInstruction` built from the call's prompt/lead context,
//! fixed `tools` set: `end_call` / `save_lead` / `schedule_callback`),
//! session resumption handle persistence and reconnect, and tool-call
//! dispatch back into [`crate::engine`].
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

#[derive(Clone, Debug)]
pub struct TranscriptEntry {
    pub role: Role,
    pub text: String,
    pub ts_ms: u64,
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
pub(crate) async fn run_session<T: Transport>(
    mut transport: T,
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    resume_handle: Option<String>,
    audio_in: &mut mpsc::Receiver<Vec<i16>>,
    events: &mpsc::Sender<Event>,
    latest_handle: &mut Option<String>,
    setup_ok: &mut bool,
) -> SessionEnd {
    // 1. Setup.
    let setup = proto::build_setup(server, scenario, resume_handle.as_deref());
    if transport.send_text(setup.to_string()).await.is_err() {
        return SessionEnd::Resumable("send setup failed".into());
    }

    loop {
        tokio::select! {
            frame = audio_in.recv() => {
                match frame {
                    Some(pcm) => {
                        let msg = build_realtime_input(&pcm);
                        if transport.send_text(msg).await.is_err() {
                            return SessionEnd::Resumable("send audio failed".into());
                        }
                    }
                    None => return SessionEnd::Hangup, // caller dropped the sender
                }
            }
            incoming = transport.recv() => {
                match incoming {
                    Some(Ok(text)) => {
                        let parsed = match proto::parse_server_message(&text) {
                            Ok(p) => p,
                            Err(e) => { let _ = events.send(Event::Warning(e.to_string())).await; continue; }
                        };
                        for se in parsed {
                            match se {
                                ServerEvent::SetupComplete => { *setup_ok = true; }
                                ServerEvent::OutputAudio(pcm) =>
                                    { let _ = events.send(Event::OutputAudio(pcm)).await; }
                                ServerEvent::Transcript { role, text, final_ } =>
                                    { let _ = events.send(Event::Transcript { role, text, final_ }).await; }
                                ServerEvent::Interrupted =>
                                    { let _ = events.send(Event::Interrupted).await; }
                                ServerEvent::TurnComplete =>
                                    { let _ = events.send(Event::TurnComplete).await; }
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
        "realtimeInput": { "audio": { "mimeType": "audio/pcm;rate=16000", "data": data } }
    })
    .to_string()
}

fn build_tool_response(call_id: &str) -> String {
    serde_json::json!({
        "toolResponse": { "functionResponses": [ { "id": call_id, "response": { "ok": true } } ] }
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
        loop {
            match self.ws.next().await? {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => return Some(Ok(t.to_string())),
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => return None,
                Ok(_) => continue, // ignore ping/pong/binary
                Err(e) => return Some(Err(crate::error::Error::Connect(e.to_string()))),
            }
        }
    }
}

/// Connect + run with reconnect. The event/audio channels stay stable across reconnects.
///
/// The transcript is accumulated here (not inside `run_session`) by tapping the
/// event channel with a small forwarding task: every `Event` produced by a
/// session attempt passes through unchanged to the caller, and finished
/// `Transcript` events are additionally copied into the outcome's transcript
/// buffer. This keeps `run_session`'s signature/tests focused purely on
/// protocol behavior.
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
pub async fn start(server: &ServerConfig, scenario: &ScenarioConfig) -> Result<Session> {
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

        loop {
            if hangup_rx.try_recv().is_ok() {
                return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
            }

            let url = proto::endpoint_url(&server);
            let connected = tokio::select! {
                r = tokio_tungstenite::connect_async(&url) => r,
                _ = hangup_rx.recv() => {
                    return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript };
                }
            };

            match connected {
                Ok((ws, _)) => {
                    let transport = WsTransport { ws };
                    let prior_handle = handle.clone();
                    let mut latest = handle.clone();
                    let mut setup_ok = false;

                    // Tap: forward every event to the caller unchanged, while also
                    // copying finished transcript entries into the local accumulator.
                    let (tap_tx, mut tap_rx) = mpsc::channel::<Event>(256);
                    let out_tx = event_tx.clone();
                    let ts_base = clock;
                    let tap_transcript: std::sync::Arc<std::sync::Mutex<Vec<TranscriptEntry>>> =
                        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
                    let tap_transcript_writer = tap_transcript.clone();
                    let forward = tokio::spawn(async move {
                        while let Some(ev) = tap_rx.recv().await {
                            if let Event::Transcript { role, ref text, final_ } = ev {
                                if final_ {
                                    let ts_ms = ts_base.elapsed().as_millis() as u64;
                                    tap_transcript_writer.lock().unwrap().push(TranscriptEntry {
                                        role, text: text.clone(), ts_ms,
                                    });
                                }
                            }
                            if out_tx.send(ev).await.is_err() {
                                break;
                            }
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
    }

    impl Transport for FakeTransport {
        async fn send_text(&mut self, s: String) -> Result<()> {
            self.sent.lock().unwrap().push(s);
            Ok(())
        }
        async fn recv(&mut self) -> Option<Result<String>> {
            self.incoming.pop_front().map(Ok)
        }
    }

    fn server() -> ServerConfig {
        ServerConfig { api_key: "K".into(), proxy: None, model: Model::HalfCascade,
            voice: "Autonoe".into(), language: "ru-RU".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 3 }
    }
    fn scenario() -> ScenarioConfig {
        ScenarioConfig { system_prompt: "hi".into(),
            goal_schema: serde_json::json!({"type":"object"}), context: None }
    }

    #[tokio::test]
    async fn transcript_and_end_call_flow() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"setupComplete":{}}"#.to_string(),
            r#"{"serverContent":{"outputTranscription":{"text":"Hi there","finished":true}}}"#.to_string(),
            r#"{"toolCall":{"functionCalls":[{"id":"c1","name":"end_call","args":{"disposition":"done"}}]}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent: sent.clone() };

        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut erx) = mpsc::channel::<Event>(64);

        let mut setup_ok = false;
        let end = run_session(transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok).await;

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
        assert!(sent.lock().unwrap().iter().any(|s| s.contains("toolResponse")));
    }

    #[tokio::test]
    async fn captures_resumption_handle_and_remote_close() {
        let incoming = std::collections::VecDeque::from(vec![
            r#"{"sessionResumptionUpdate":{"newHandle":"H9","resumable":true}}"#.to_string(),
        ]);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut latest_handle = None;
        let mut setup_ok = false;
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut latest_handle, &mut setup_ok,
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

    #[tokio::test]
    async fn setup_failure_never_sets_setup_ok() {
        // send_text always succeeds on FakeTransport, so drive the "no frames
        // at all" case: the connection drops before even setupComplete.
        let incoming = std::collections::VecDeque::new();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = FakeTransport { incoming, sent };
        let (_atx, mut arx) = mpsc::channel::<Vec<i16>>(8);
        let (etx, mut _erx) = mpsc::channel::<Event>(64);
        let mut setup_ok = false;
        let end = run_session(
            transport, &server(), &scenario(), None, &mut arx, &etx, &mut None, &mut setup_ok,
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
}
