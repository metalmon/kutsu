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
//! Not yet implemented.
