use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::Serialize;
use serde_json::Value;

use crate::util::now_ms;

/// Metadata known when a request is captured. Status, tokens and duration
/// arrive later, once the upstream response has been accounted for.
#[derive(Debug, Clone)]
pub struct ContextSeed {
    pub endpoint: String,
    pub inbound: String,
    pub model: String,
    pub provider: String,
    pub route_id: String,
    pub upstream_model: String,
    pub stream: bool,
    pub body: Value,
    /// Size of the request body as received, before JSON normalization. Used
    /// as an upper bound so oversized payloads are not re-serialized.
    pub body_bytes: usize,
}

/// Everything the dashboard needs to list a captured request.
#[derive(Debug, Clone, Serialize)]
pub struct ContextSummary {
    pub id: u64,
    pub ts: i64,
    pub endpoint: String,
    pub inbound: String,
    pub model: String,
    pub provider: String,
    pub route_id: String,
    pub upstream_model: String,
    pub stream: bool,
    /// 0 while the request is still in flight.
    pub status: u16,
    pub settled: bool,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub duration_ms: i64,
    pub error: Option<String>,
    /// Number of chat turns carried by the body.
    pub turns: usize,
    pub bytes: usize,
    /// The body exceeded the per-entry budget and was not retained.
    pub oversized: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextDetail {
    #[serde(flatten)]
    pub meta: ContextSummary,
    pub body: Option<Value>,
}

struct Entry {
    meta: ContextSummary,
    body: Option<Value>,
}

struct Inner {
    map: HashMap<u64, Entry>,
    order: VecDeque<u64>,
    max_entries: usize,
    max_entry_bytes: usize,
    next_id: u64,
}

/// Bounded, in-memory record of the inbound payload of recent requests.
/// Nothing is written to disk: prompts, tool arguments and attachments stay in
/// this process only, and the oldest request is discarded once the ring is
/// full.
pub struct ContextStore {
    inner: Mutex<Inner>,
}

impl ContextStore {
    pub fn new(max_entries: usize, max_entry_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                max_entries,
                max_entry_bytes: max_entry_bytes.max(1),
                next_id: 1,
            }),
        }
    }

    /// Apply new limits immediately. `0` disables capture and drops everything.
    pub fn set_limits(&self, max_entries: usize, max_entry_bytes: usize) {
        let mut inner = self.lock();
        inner.max_entries = max_entries;
        inner.max_entry_bytes = max_entry_bytes.max(1);
        while inner.order.len() > inner.max_entries {
            let Some(old) = inner.order.pop_front() else {
                break;
            };
            inner.map.remove(&old);
        }
    }

    /// Capture one request. Returns `None` when capture is disabled.
    pub fn capture(&self, seed: ContextSeed) -> Option<u64> {
        let mut inner = self.lock();
        if inner.max_entries == 0 {
            return None;
        }
        let bytes = if seed.body_bytes > inner.max_entry_bytes {
            seed.body_bytes
        } else {
            serde_json::to_vec(&seed.body)
                .map(|value| value.len())
                .unwrap_or(usize::MAX)
        };
        let turns = count_turns(&seed.body);
        let oversized = bytes > inner.max_entry_bytes;
        let id = inner.next_id;
        inner.next_id += 1;
        let meta = ContextSummary {
            id,
            ts: now_ms(),
            endpoint: seed.endpoint,
            inbound: seed.inbound,
            model: seed.model,
            provider: seed.provider,
            route_id: seed.route_id,
            upstream_model: seed.upstream_model,
            stream: seed.stream,
            status: 0,
            settled: false,
            prompt_tokens: 0,
            completion_tokens: 0,
            duration_ms: 0,
            error: None,
            turns,
            bytes,
            oversized,
        };
        inner.map.insert(
            id,
            Entry {
                meta,
                body: if oversized { None } else { Some(seed.body) },
            },
        );
        inner.order.push_back(id);
        while inner.order.len() > inner.max_entries {
            let Some(old) = inner.order.pop_front() else {
                break;
            };
            inner.map.remove(&old);
        }
        Some(id)
    }

    /// Attach the final accounting to a captured request. Unknown ids (the
    /// entry aged out while the response was still streaming) are ignored.
    pub fn finish(
        &self,
        id: Option<u64>,
        status: u16,
        prompt_tokens: u64,
        completion_tokens: u64,
        duration_ms: i64,
        error: Option<String>,
    ) {
        let Some(id) = id else {
            return;
        };
        let mut inner = self.lock();
        let Some(entry) = inner.map.get_mut(&id) else {
            return;
        };
        entry.meta.status = status;
        entry.meta.settled = true;
        entry.meta.prompt_tokens = prompt_tokens;
        entry.meta.completion_tokens = completion_tokens;
        entry.meta.duration_ms = duration_ms;
        entry.meta.error = error;
    }

    /// Newest first, capped by both `limit` and the configured ring size.
    pub fn list(&self, limit: Option<usize>) -> Vec<ContextSummary> {
        let inner = self.lock();
        let cap = limit.unwrap_or(usize::MAX).min(inner.max_entries);
        inner
            .order
            .iter()
            .rev()
            .take(cap)
            .filter_map(|id| inner.map.get(id).map(|entry| entry.meta.clone()))
            .collect()
    }

    pub fn get(&self, id: u64) -> Option<ContextDetail> {
        let inner = self.lock();
        inner.map.get(&id).map(|entry| ContextDetail {
            meta: entry.meta.clone(),
            body: entry.body.clone(),
        })
    }

    pub fn capacity(&self) -> usize {
        self.lock().max_entries
    }

    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn count_turns(body: &Value) -> usize {
    if let Some(input) = body.get("input") {
        if let Some(items) = input.as_array() {
            return items.len();
        }
        if input.is_string() {
            return 1;
        }
    }
    body.get("messages")
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seed(body: Value) -> ContextSeed {
        let body_bytes = serde_json::to_vec(&body).map(|value| value.len()).unwrap_or(0);
        ContextSeed {
            endpoint: "chat.completions".into(),
            inbound: "openai".into(),
            model: "m".into(),
            provider: "p".into(),
            route_id: "r".into(),
            upstream_model: "u".into(),
            stream: false,
            body,
            body_bytes,
        }
    }

    #[test]
    fn keeps_only_the_newest_entries_and_settles_them() {
        let store = ContextStore::new(2, 4096);
        let a = store.capture(seed(json!({"messages": [{"role": "user", "content": "a"}]}))).unwrap();
        let b = store.capture(seed(json!({"messages": [{"role": "user", "content": "b"}]}))).unwrap();
        let c = store.capture(seed(json!({"messages": [{"role": "user", "content": "c"}]}))).unwrap();
        assert!(store.get(a).is_none());
        assert_eq!(store.len(), 2);
        let list = store.list(None);
        assert_eq!(list[0].id, c);
        assert_eq!(list[1].id, b);
        assert_eq!(list[0].turns, 1);
        assert!(!list[0].settled);
        store.finish(Some(c), 200, 10, 5, 42, None);
        let detail = store.get(c).unwrap();
        assert!(detail.meta.settled);
        assert_eq!(detail.meta.status, 200);
        assert_eq!(detail.meta.prompt_tokens, 10);
        assert!(detail.body.is_some());
    }

    #[test]
    fn limit_zero_disables_capture_and_drops_entries() {
        let store = ContextStore::new(5, 4096);
        assert!(store.capture(seed(json!({"messages": []}))).is_some());
        assert_eq!(store.len(), 1);
        store.set_limits(0, 4096);
        assert_eq!(store.len(), 0);
        assert!(store.capture(seed(json!({"messages": []}))).is_none());
    }

    #[test]
    fn oversized_bodies_keep_metadata_without_the_body() {
        let store = ContextStore::new(5, 32);
        let id = store
            .capture(seed(json!({"messages": [{"role": "user", "content": "x".repeat(128)}]})))
            .unwrap();
        let detail = store.get(id).unwrap();
        assert!(detail.meta.oversized);
        assert!(detail.body.is_none());
        assert!(detail.meta.bytes > 32);
    }

    #[test]
    fn counts_responses_input_turns() {
        assert_eq!(count_turns(&json!({"input": ["a", "b"]})), 2);
        assert_eq!(count_turns(&json!({"input": "hello"})), 1);
        assert_eq!(count_turns(&json!({"messages": [1, 2, 3]})), 3);
        assert_eq!(count_turns(&json!({})), 0);
    }
}
