//! Audio bridge between the SIP RTP session and the Gemini Live WebSocket.
//!
//! Phone-side audio is G.711 mu-law/PCM at 8kHz; Gemini Live expects PCM16
//! at 16kHz in and produces PCM16 at 24kHz out. This module depacketizes
//! RTP, decodes/encodes mu-law, and resamples in both directions.
//!
//! Not yet implemented.
