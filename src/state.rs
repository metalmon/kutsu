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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::gemini_live::TranscriptEntry;

/// Lifecycle state of a call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Ringing,
    InProgress,
    Completed,
    Failed,
    HungUp,
}

/// One call's record.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CallRecord {
    pub call_id: String,
    pub number: String,
    pub state: CallState,
    pub transcript: Vec<TranscriptEntry>,
    pub goal: Option<Value>,
    pub error: Option<String>,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
}

/// In-memory store of call records, keyed by call_id. Cheap to clone (Arc).
#[derive(Clone, Default)]
pub struct CallStore {
    inner: Arc<Mutex<HashMap<String, CallRecord>>>,
}

impl CallStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, rec: CallRecord) {
        self.inner.lock().unwrap().insert(rec.call_id.clone(), rec);
    }

    pub fn set_state(&self, call_id: &str, state: CallState) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.state = state;
        }
    }

    pub fn append_transcript(&self, call_id: &str, entry: TranscriptEntry) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.transcript.push(entry);
        }
    }

    /// Replace the running transcript with the authoritative one from the session.
    pub fn set_transcript(&self, call_id: &str, transcript: Vec<TranscriptEntry>) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.transcript = transcript;
        }
    }

    pub fn finalize(
        &self,
        call_id: &str,
        state: CallState,
        goal: Option<Value>,
        error: Option<String>,
        ended_ms: u64,
    ) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.state = state;
            r.goal = goal;
            r.error = error;
            r.ended_ms = Some(ended_ms);
        }
    }

    pub fn get(&self, call_id: &str) -> Option<CallRecord> {
        self.inner.lock().unwrap().get(call_id).cloned()
    }

    pub fn list(&self) -> Vec<CallRecord> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gemini_live::{Role, TranscriptEntry};

    fn rec(id: &str) -> CallRecord {
        CallRecord {
            call_id: id.into(),
            number: "600".into(),
            state: CallState::Ringing,
            transcript: vec![],
            goal: None,
            error: None,
            started_ms: 1000,
            ended_ms: None,
        }
    }

    #[test]
    fn insert_and_get_roundtrip() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        let got = store.get("c1").unwrap();
        assert_eq!(got.call_id, "c1");
        assert_eq!(got.state, CallState::Ringing);
        assert!(store.get("missing").is_none());
    }

    #[test]
    fn state_and_transcript_mutations() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_state("c1", CallState::InProgress);
        store.append_transcript("c1", TranscriptEntry { role: Role::Model, text: "hi".into(), ts_ms: 5 });
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::InProgress);
        assert_eq!(got.transcript.len(), 1);
        assert_eq!(got.transcript[0].text, "hi");
    }

    #[test]
    fn finalize_sets_terminal_fields() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_transcript("c1", vec![TranscriptEntry { role: Role::User, text: "bye".into(), ts_ms: 9 }]);
        store.finalize("c1", CallState::Completed, Some(serde_json::json!({"ok": true})), None, 2000);
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::Completed);
        assert_eq!(got.ended_ms, Some(2000));
        assert!(got.goal.is_some());
        assert_eq!(got.transcript.len(), 1);
    }

    #[test]
    fn list_returns_all_and_serializes() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.insert(rec("c2"));
        assert_eq!(store.list().len(), 2);
        let json = serde_json::to_string(&store.get("c1").unwrap()).unwrap();
        assert!(json.contains("\"state\":\"ringing\""));
    }
}
