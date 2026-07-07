//! Call engine — drives one call's full lifecycle.
//!
//! Dial via [`crate::sip`] → wait for answer → bridge audio and tool calls
//! both ways via [`crate::bridge`] and [`crate::gemini_live`] → hang up (SIP
//! `BYE` or the model's `end_call` tool) → finalize the call's
//! [`crate::state::CallRecord`] and persist its transcript to disk.
//!
//! Not yet implemented.
