//! Call state store.
//!
//! Planned shape (mirroring the proven `CallRecord`/`TranscriptEntry`/
//! `CallState` design in zeroclaw's
//! `crates/zeroclaw-channels/src/voice_call.rs`, adapted for a SIP-only,
//! single-server context): `Arc<Mutex<HashMap<String, CallRecord>>>` keyed
//! by `call_id`, with call direction, remote number, lifecycle state
//! (ringing / in_progress / completed / failed / hung_up), and a running
//! transcript.
//!
//! Not yet implemented.
