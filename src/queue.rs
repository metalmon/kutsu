//! Pending-call queue for the engine scheduler. In-memory now; the trait is
//! the seam for a persistent (SQLite) backend later.
use crate::config::ScenarioConfig;

#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub call_id: String,
    pub number: String,
    pub scenario: ScenarioConfig,
    pub eligible_at_ms: u64,
    pub attempt: u32,
    pub retry_of: Option<String>,
}

pub trait QueueStore: Send {
    /// Enqueue an entry.
    fn push(&mut self, entry: PendingEntry);
    /// Remove and return the earliest entry whose `eligible_at_ms <= now_ms`,
    /// tie-broken by (eligible_at_ms, call_id). None if nothing is eligible.
    fn pop_eligible(&mut self, now_ms: u64) -> Option<PendingEntry>;
    /// The soonest `eligible_at_ms` of any pending entry (eligible or not).
    fn peek_next_eligible_at(&self) -> Option<u64>;
    /// Remove a pending entry by call_id (cancel before dispatch). Returns it if present.
    fn remove(&mut self, call_id: &str) -> Option<PendingEntry>;
    /// Count of pending entries.
    fn len(&self) -> usize;
    /// 1-based position of `call_id` in dispatch order among pending entries.
    fn position(&self, call_id: &str) -> Option<usize>;
}

/// In-memory `QueueStore`: a `BTreeMap` keyed by (eligible_at_ms, call_id).
#[derive(Default)]
pub struct MemQueue {
    // BTreeMap gives ordered iteration for pop/peek/position.
    entries: std::collections::BTreeMap<(u64, String), PendingEntry>,
}

impl MemQueue {
    pub fn new() -> Self { Self::default() }
}

impl QueueStore for MemQueue {
    fn push(&mut self, entry: PendingEntry) {
        self.entries.insert((entry.eligible_at_ms, entry.call_id.clone()), entry);
    }
    fn pop_eligible(&mut self, now_ms: u64) -> Option<PendingEntry> {
        // BTreeMap is ordered by (eligible_at_ms, call_id), so the first
        // entry is always the earliest — no sentinel/range trick needed
        // (a prior version used a "\u{10FFFF}" upper-bound sentinel, which
        // is not a true universal string bound: a call_id starting with
        // U+10FFFF and having trailing chars would sort after it and be
        // silently skipped).
        match self.entries.first_key_value() {
            Some((k, _)) if k.0 <= now_ms => self.entries.pop_first().map(|(_, v)| v),
            _ => None,
        }
    }
    fn peek_next_eligible_at(&self) -> Option<u64> {
        self.entries.keys().next().map(|(t, _)| *t)
    }
    fn remove(&mut self, call_id: &str) -> Option<PendingEntry> {
        let key = self.entries.iter().find(|(_, v)| v.call_id == call_id).map(|(k, _)| k.clone())?;
        self.entries.remove(&key)
    }
    fn len(&self) -> usize { self.entries.len() }
    fn position(&self, call_id: &str) -> Option<usize> {
        self.entries.values().position(|v| v.call_id == call_id).map(|i| i + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(id: &str, at: u64) -> PendingEntry {
        PendingEntry { call_id: id.into(), number: "1".into(),
            scenario: ScenarioConfig { system_prompt: String::new(), goal_schema: serde_json::json!({}), context: None },
            eligible_at_ms: at, attempt: 1, retry_of: None }
    }
    #[test]
    fn pops_earliest_eligible_only() {
        let mut q = MemQueue::new();
        q.push(entry("b", 200));
        q.push(entry("a", 100));
        assert!(q.pop_eligible(50).is_none());          // nothing eligible yet
        assert_eq!(q.pop_eligible(150).unwrap().call_id, "a"); // only a is eligible
        assert!(q.pop_eligible(150).is_none());          // b not yet
        assert_eq!(q.pop_eligible(250).unwrap().call_id, "b");
    }
    #[test]
    fn pop_eligible_ties_break_by_call_id() {
        let mut q = MemQueue::new();
        q.push(entry("b", 100));
        q.push(entry("a", 100));
        q.push(entry("c", 100));
        assert_eq!(q.pop_eligible(100).unwrap().call_id, "a");
        assert_eq!(q.pop_eligible(100).unwrap().call_id, "b");
        assert_eq!(q.pop_eligible(100).unwrap().call_id, "c");
        assert!(q.pop_eligible(100).is_none());
    }
    #[test]
    fn peek_and_position_and_remove() {
        let mut q = MemQueue::new();
        q.push(entry("a", 100));
        q.push(entry("b", 200));
        assert_eq!(q.peek_next_eligible_at(), Some(100));
        assert_eq!(q.position("b"), Some(2));
        assert_eq!(q.remove("a").unwrap().call_id, "a");
        assert_eq!(q.position("b"), Some(1));
        assert_eq!(q.len(), 1);
    }
}
