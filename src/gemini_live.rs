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
/// attempt ends (including a plain `RemoteClose`).
pub(crate) async fn run_session<T: Transport>(
    mut transport: T,
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    resume_handle: Option<String>,
    audio_in: &mut mpsc::Receiver<Vec<i16>>,
    events: &mpsc::Sender<Event>,
    latest_handle: &mut Option<String>,
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
                                ServerEvent::SetupComplete => {}
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
/// session attempt passes through unchanged to the caller, and `Transcript`
/// events are additionally copied into the outcome's transcript buffer. This
/// keeps `run_session`'s signature/tests focused purely on protocol behavior.
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
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => {
                    rstate.on_success();
                    backoff.reset();
                    let transport = WsTransport { ws };
                    let mut latest = handle.clone();

                    // Tap: forward every event to the caller unchanged, while also
                    // copying transcript entries into the local accumulator.
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

                    let end = run_session(
                        transport, &server, &scenario, handle.clone(),
                        &mut audio_rx, &tap_tx, &mut latest,
                    ).await;
                    drop(tap_tx);
                    let _ = forward.await;
                    transcript.extend(tap_transcript.lock().unwrap().drain(..));
                    handle = latest.or(handle);
                    match end {
                        SessionEnd::EndCall(goal) =>
                            return CallOutcome { ended_by: EndedBy::ModelEndCall, goal: Some(goal), transcript },
                        SessionEnd::Hangup =>
                            return CallOutcome { ended_by: EndedBy::CallerHangup, goal: None, transcript },
                        SessionEnd::RemoteClose | SessionEnd::Resumable(_) => {
                            let _ = event_tx.send(Event::Warning("reconnecting".into())).await;
                        }
                    }
                }
                Err(_) => {
                    if rstate.on_failure() { handle = None; }
                }
            }
            tokio::time::sleep(backoff.next_delay()).await;
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

        let end = run_session(transport, &server(), &scenario(), None, &mut arx, &etx, &mut None).await;

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
        let end = run_session(transport, &server(), &scenario(), None, &mut arx, &etx, &mut latest_handle).await;
        // No more frames after the update -> remote close.
        assert!(matches!(end, SessionEnd::RemoteClose));
        // The resumption handle from the update was captured for the caller's
        // reconnect loop, even though this attempt ended in a plain close.
        assert_eq!(latest_handle.as_deref(), Some("H9"));
    }
}
