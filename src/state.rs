//! Call state store: an in-memory `Arc<Mutex<HashMap<call_id, CallRecord>>>`
//! recording each outbound call's lifecycle state, running transcript, and
//! outcome. Written by the engine; read (phase 5) by the MCP layer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::gemini_live::TranscriptEntry;

/// Lifecycle state of a call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Queued,
    Ringing,
    InProgress,
    Completed,
    Failed,
    HungUp,
    Cancelled,
}

/// Audio quality metrics for a call.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CallQuality {
    pub underruns: u64,
    pub starved_ms: u64,
    pub max_gap_ms: u64,
    /// Uplink (phone -> us) RTP packets received.
    pub uplink_received: u64,
    /// Uplink RTP packets lost (RFC 3550 span - received).
    pub uplink_lost: u64,
    /// Uplink RTP packets that arrived late/out-of-order.
    pub uplink_reordered: u64,
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
    pub quality: CallQuality,
}

/// Live counts of in-flight calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateCounts {
    pub active: usize,
    pub queued: usize,
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

    pub fn set_quality(&self, call_id: &str, q: CallQuality) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.quality = q;
        }
    }

    pub fn get(&self, call_id: &str) -> Option<CallRecord> {
        self.inner.lock().unwrap().get(call_id).cloned()
    }

    pub fn list(&self) -> Vec<CallRecord> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// 1-based position of a `Queued` call among all queued calls, ordered by
    /// (started_ms, call_id). `None` if absent or not queued.
    pub fn queued_position(&self, call_id: &str) -> Option<usize> {
        let g = self.inner.lock().unwrap();
        let target = g.get(call_id)?;
        if target.state != CallState::Queued {
            return None;
        }
        let key = (target.started_ms, target.call_id.as_str());
        let ahead = g
            .values()
            .filter(|r| r.state == CallState::Queued)
            .filter(|r| (r.started_ms, r.call_id.as_str()) < key)
            .count();
        Some(ahead + 1)
    }

    /// Snapshot of active (Ringing|InProgress) and Queued counts.
    pub fn counts(&self) -> StateCounts {
        let g = self.inner.lock().unwrap();
        let mut c = StateCounts { active: 0, queued: 0 };
        for r in g.values() {
            match r.state {
                CallState::Ringing | CallState::InProgress => c.active += 1,
                CallState::Queued => c.queued += 1,
                _ => {}
            }
        }
        c
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
            quality: CallQuality::default(),
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

    #[test]
    fn queued_and_cancelled_serialize_snake_case() {
        assert_eq!(serde_json::to_string(&CallState::Queued).unwrap(), "\"queued\"");
        assert_eq!(serde_json::to_string(&CallState::Cancelled).unwrap(), "\"cancelled\"");
    }

    #[test]
    fn queued_position_ranks_by_started_then_id() {
        let store = CallStore::new();
        let mut a = rec("call-0"); a.state = CallState::Queued; a.started_ms = 100;
        let mut b = rec("call-1"); b.state = CallState::Queued; b.started_ms = 200;
        let mut c = rec("call-2"); c.state = CallState::InProgress; c.started_ms = 50;
        store.insert(a); store.insert(b); store.insert(c);
        assert_eq!(store.queued_position("call-0"), Some(1));
        assert_eq!(store.queued_position("call-1"), Some(2));
        assert_eq!(store.queued_position("call-2"), None); // not queued
        assert_eq!(store.queued_position("missing"), None);
        let counts = store.counts();
        assert_eq!(counts.queued, 2);
        assert_eq!(counts.active, 1);
    }

    #[test]
    fn set_quality_updates_record() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_quality("c1", CallQuality {
            underruns: 3, starved_ms: 60, max_gap_ms: 220,
            uplink_received: 500, uplink_lost: 7, uplink_reordered: 1,
        });
        let got = store.get("c1").unwrap();
        assert_eq!(got.quality.underruns, 3);
        assert_eq!(got.quality.uplink_lost, 7);
        assert_eq!(got.quality.uplink_received, 500);
    }
}
