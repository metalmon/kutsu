//! Call state store: an in-memory `Arc<Mutex<HashMap<call_id, CallRecord>>>`
//! recording each outbound call's lifecycle state, running transcript, and
//! outcome. Written by the engine; read (phase 5) by the MCP layer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::gemini_live::{AffectEntry, TranscriptEntry};

/// Lifecycle state of a call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Queued,
    Ringing,
    InProgress,
    Ended,
}

/// ~15 s: an answered call that the remote drops this fast with no transcript is
/// a carrier announcement ("subscriber unavailable"), not a real conversation.
pub const FAST_DISCONNECT_MAX_MS: u64 = 15_000;

/// The authoritative terminal result: what the call actually was.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Completed,
    Voicemail,
    Announcement,
    Ivr,
    Hold,
    Busy,
    NoAnswer,
    Rejected,
    NotFound,
    Unavailable,
    Failed,
    Cancelled,
}

/// Shape of a terminated call, fed to [`resolve_disposition`].
#[derive(Clone, Copy, Debug)]
pub struct CallShape {
    pub answered: bool,
    pub duration_ms: u64,
    pub transcript_len: usize,
    pub ended_by: crate::gemini_live::EndedBy,
}

/// Resolve the single authoritative disposition. Precedence: cancelled, then the
/// SIP dial outcome when the call was never answered, then the model's `amd`
/// verdict when answered, then call-shape inference.
pub fn resolve_disposition(
    cancelled: bool,
    amd: Option<&str>,
    sip: Option<crate::sip::CallOutcome>,
    shape: CallShape,
) -> Disposition {
    use crate::sip::CallOutcome;
    if cancelled {
        return Disposition::Cancelled;
    }
    if !shape.answered {
        return match sip {
            Some(CallOutcome::Busy) => Disposition::Busy,
            Some(CallOutcome::NoAnswer) => Disposition::NoAnswer,
            Some(CallOutcome::Rejected) => Disposition::Rejected,
            Some(CallOutcome::NotFound) => Disposition::NotFound,
            Some(CallOutcome::Unavailable) => Disposition::Unavailable,
            _ => Disposition::Failed,
        };
    }
    match amd {
        Some("voicemail") => Disposition::Voicemail,
        Some("announcement") => Disposition::Announcement,
        Some("ivr") => Disposition::Ivr,
        Some("hold") => Disposition::Hold,
        Some("live") => Disposition::Completed,
        _ => {
            // A carrier announcement can end either by the Gemini WS server
            // closing first (RemoteClose) or by our teardown hangup after the
            // callee/carrier drops the call (CallerHangup) — both count.
            let fast_disconnect = matches!(
                shape.ended_by,
                crate::gemini_live::EndedBy::RemoteClose
                    | crate::gemini_live::EndedBy::CallerHangup
            ) && shape.duration_ms < FAST_DISCONNECT_MAX_MS
                && shape.transcript_len == 0;
            if fast_disconnect {
                Disposition::Announcement
            } else if shape.transcript_len > 0 {
                Disposition::Completed
            } else {
                Disposition::Failed
            }
        }
    }
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
    /// Uplink audio level: RMS amplitude (0..32767, linear PCM16) of the
    /// decoded phone audio over the call. A very low value means the callee's
    /// mic/gain was quiet — a plausible ASR-quality factor independent of loss.
    /// Approximate call-average: the uplink task is detached when the bridge is
    /// aborted at teardown, so the last few teardown-window frames may be
    /// excluded (negligible against a whole-call average).
    pub uplink_rms: u64,
    /// Downlink (us -> phone) loss %, as the callee reports via RTCP receiver
    /// reports. `0` when the carrier never sent an RR (no signal, not "perfect").
    pub downlink_loss_pct: f32,
    /// Downlink interarrival jitter (ms) from the callee's RTCP RR.
    pub downlink_jitter_ms: u32,
    /// Estimated round-trip time (ms) from the callee's RTCP RR; `0` if unknown.
    pub downlink_rtt_ms: u32,
}

/// One call's record.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CallRecord {
    pub call_id: String,
    pub number: String,
    pub state: CallState,
    pub transcript: Vec<TranscriptEntry>,
    /// Emotions detected during the call (affective dialog), in order.
    pub affect: Vec<AffectEntry>,
    pub goal: Option<Value>,
    pub error: Option<String>,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
    pub quality: CallQuality,
    /// The authoritative terminal result; None while in-flight.
    pub disposition: Option<Disposition>,
    /// 1 for the first dial; incremented per internal busy-retry (same call_id).
    pub attempt: u32,
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

    /// Append one detected-emotion event to the call's record.
    pub fn append_affect(&self, call_id: &str, entry: AffectEntry) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.affect.push(entry);
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
        disposition: Disposition,
        goal: Option<Value>,
        error: Option<String>,
        ended_ms: u64,
    ) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.state = CallState::Ended;
            r.disposition = Some(disposition);
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

    /// Snapshot of active (Ringing|InProgress) and Queued counts.
    pub fn counts(&self) -> StateCounts {
        let g = self.inner.lock().unwrap();
        let mut c = StateCounts {
            active: 0,
            queued: 0,
        };
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
    use crate::gemini_live::{AffectEntry, Role, TranscriptEntry};

    fn rec(id: &str) -> CallRecord {
        CallRecord {
            call_id: id.into(),
            number: "600".into(),
            state: CallState::Ringing,
            transcript: vec![],
            affect: vec![],
            goal: None,
            error: None,
            started_ms: 1000,
            ended_ms: None,
            quality: CallQuality::default(),
            disposition: None,
            attempt: 1,
        }
    }

    #[test]
    fn append_affect_records_emotion_in_order() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.append_affect(
            "c1",
            AffectEntry {
                role: Role::User,
                label: "interest".into(),
                ts_ms: 7,
            },
        );
        store.append_affect(
            "c1",
            AffectEntry {
                role: Role::Model,
                label: "calmness".into(),
                ts_ms: 9,
            },
        );
        let r = store.get("c1").unwrap();
        assert_eq!(r.affect.len(), 2);
        assert_eq!(r.affect[0].label, "interest");
        assert_eq!(r.affect[1].role, Role::Model);
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
        store.append_transcript(
            "c1",
            TranscriptEntry {
                role: Role::Model,
                text: "hi".into(),
                ts_ms: 5,
            },
        );
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::InProgress);
        assert_eq!(got.transcript.len(), 1);
        assert_eq!(got.transcript[0].text, "hi");
    }

    #[test]
    fn finalize_sets_terminal_fields() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_transcript(
            "c1",
            vec![TranscriptEntry {
                role: Role::User,
                text: "bye".into(),
                ts_ms: 9,
            }],
        );
        store.finalize(
            "c1",
            Disposition::Completed,
            Some(serde_json::json!({"ok": true})),
            None,
            2000,
        );
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::Ended);
        assert_eq!(got.ended_ms, Some(2000));
        assert!(got.goal.is_some());
        assert_eq!(got.transcript.len(), 1);
        assert_eq!(got.disposition, Some(Disposition::Completed));
    }

    #[test]
    fn finalize_sets_disposition_and_record_defaults_attempt() {
        let store = CallStore::new();
        let r = rec("c1");
        assert_eq!(r.attempt, 1);
        assert_eq!(r.disposition, None);
        store.insert(r);
        store.finalize("c1", Disposition::Busy, None, Some("busy".into()), 3000);
        let got = store.get("c1").unwrap();
        assert_eq!(got.disposition, Some(Disposition::Busy));
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
    fn queued_and_ended_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&CallState::Queued).unwrap(),
            "\"queued\""
        );
        assert_eq!(
            serde_json::to_string(&CallState::Ended).unwrap(),
            "\"ended\""
        );
    }

    #[test]
    fn set_quality_updates_record() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_quality(
            "c1",
            CallQuality {
                underruns: 3,
                starved_ms: 60,
                max_gap_ms: 220,
                uplink_received: 500,
                uplink_lost: 7,
                uplink_reordered: 1,
                uplink_rms: 2048,
                ..Default::default()
            },
        );
        let got = store.get("c1").unwrap();
        assert_eq!(got.quality.underruns, 3);
        assert_eq!(got.quality.uplink_lost, 7);
        assert_eq!(got.quality.uplink_received, 500);
    }
}

#[cfg(test)]
mod disposition_tests {
    use super::*;
    use crate::gemini_live::EndedBy;
    use crate::sip::CallOutcome;

    fn shape(answered: bool, dur: u64, tlen: usize, ended: EndedBy) -> CallShape {
        CallShape {
            answered,
            duration_ms: dur,
            transcript_len: tlen,
            ended_by: ended,
        }
    }

    #[test]
    fn cancelled_wins() {
        let d = resolve_disposition(true, None, None, shape(false, 0, 0, EndedBy::Error));
        assert_eq!(d, Disposition::Cancelled);
    }

    #[test]
    fn unanswered_maps_sip_outcome() {
        let d = resolve_disposition(
            false,
            None,
            Some(CallOutcome::NoAnswer),
            shape(false, 0, 0, EndedBy::Error),
        );
        assert_eq!(d, Disposition::NoAnswer);
        let d = resolve_disposition(
            false,
            None,
            Some(CallOutcome::Busy),
            shape(false, 0, 0, EndedBy::Error),
        );
        assert_eq!(d, Disposition::Busy);
    }

    #[test]
    fn amd_wins_when_answered() {
        let s = shape(true, 8000, 5, EndedBy::ModelEndCall);
        assert_eq!(
            resolve_disposition(false, Some("voicemail"), None, s),
            Disposition::Voicemail
        );
        assert_eq!(
            resolve_disposition(false, Some("announcement"), None, s),
            Disposition::Announcement
        );
        assert_eq!(
            resolve_disposition(false, Some("hold"), None, s),
            Disposition::Hold
        );
        assert_eq!(
            resolve_disposition(false, Some("ivr"), None, s),
            Disposition::Ivr
        );
        assert_eq!(
            resolve_disposition(false, Some("live"), None, s),
            Disposition::Completed
        );
    }

    #[test]
    fn fast_disconnect_no_amd_is_announcement() {
        // answered, remote hung up, short, empty transcript.
        let s = shape(true, 4000, 0, EndedBy::RemoteClose);
        assert_eq!(
            resolve_disposition(false, None, None, s),
            Disposition::Announcement
        );
    }

    #[test]
    fn fast_disconnect_via_caller_hangup_is_announcement() {
        // answered, our teardown hangup fired first (carrier already dropped
        // the call), short, empty transcript.
        let s = shape(true, 4000, 0, EndedBy::CallerHangup);
        assert_eq!(
            resolve_disposition(false, None, None, s),
            Disposition::Announcement
        );
    }

    #[test]
    fn real_conversation_without_amd_is_completed() {
        let s = shape(true, 40000, 6, EndedBy::RemoteClose);
        assert_eq!(
            resolve_disposition(false, None, None, s),
            Disposition::Completed
        );
    }

    #[test]
    fn answered_but_empty_and_not_short_is_failed() {
        // answered, no amd, no transcript, but long (media negotiated then silence)
        let s = shape(true, 40000, 0, EndedBy::Error);
        assert_eq!(
            resolve_disposition(false, None, None, s),
            Disposition::Failed
        );
    }
}
