use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

const TTL: Duration = Duration::from_secs(3600);

struct Entry {
    at: Instant,
    items: Vec<Value>,
    bytes: usize,
    meta: TurnMeta,
}

#[derive(Debug, Clone, Default)]
pub struct TurnMeta {
    pub native: bool,
    pub route_id: String,
    pub model: String,
    pub provider: String,
    pub upstream_model: String,
    pub provider_base_url: String,
}

#[derive(Debug, Clone)]
pub struct StoredTurn {
    pub items: Vec<Value>,
    pub meta: TurnMeta,
}

/// Bounded, in-memory conversation cache backing Responses API
/// `previous_response_id` when the upstream is Chat Completions or Anthropic
/// Messages. Nothing is persisted to disk.
pub struct ResponseStore {
    inner: Mutex<Inner>,
}

struct Inner {
    map: HashMap<String, Entry>,
    order: VecDeque<String>,
    total_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
}

impl ResponseStore {
    pub fn new(max_entries: usize, max_bytes: usize, max_entry_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                total_bytes: 0,
                max_entries,
                max_bytes,
                max_entry_bytes,
            }),
        }
    }

    /// Apply new cache limits immediately, evicting entries if needed.
    pub fn set_limits(&self, max_entries: usize, max_bytes: usize, max_entry_bytes: usize) {
        let mut inner = self.lock();
        inner.max_entries = max_entries.max(1);
        inner.max_bytes = max_bytes.max(1);
        inner.max_entry_bytes = max_entry_bytes.max(1);
        let oversized: Vec<String> = inner
            .map
            .iter()
            .filter(|(_, entry)| entry.bytes > inner.max_entry_bytes)
            .map(|(id, _)| id.clone())
            .collect();
        for id in oversized {
            if let Some(entry) = inner.map.remove(&id) {
                inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
            }
            inner.order.retain(|item| item != &id);
        }
        while inner.order.len() > inner.max_entries || inner.total_bytes > inner.max_bytes {
            let Some(old) = inner.order.pop_front() else {
                break;
            };
            if let Some(entry) = inner.map.remove(&old) {
                inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<StoredTurn> {
        let mut inner = self.lock();
        let expired = inner
            .map
            .get(id)
            .map(|e| e.at.elapsed() > TTL)
            .unwrap_or(false);
        if expired {
            if let Some(entry) = inner.map.remove(id) {
                inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
            }
            inner.order.retain(|x| x != id);
            return None;
        }
        inner.map.get(id).map(|e| StoredTurn {
            items: e.items.clone(),
            meta: e.meta.clone(),
        })
    }

    /// Store a turn. Returns false when the turn is too large or is evicted by
    /// the byte budget, so callers can tell the client it will not be reusable.
    pub fn put(&self, id: String, items: Vec<Value>, meta: TurnMeta) -> bool {
        let bytes = serde_json::to_vec(&items)
            .map(|value| value.len())
            .unwrap_or(usize::MAX);
        let mut inner = self.lock();
        if bytes > inner.max_entry_bytes || bytes > inner.max_bytes {
            return false;
        }
        if let Some(old) = inner.map.remove(&id) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.bytes);
            inner.order.retain(|x| x != &id);
        }
        inner.map.insert(
            id.clone(),
            Entry {
                at: Instant::now(),
                items,
                bytes,
                meta,
            },
        );
        inner.order.push_back(id.clone());
        inner.total_bytes += bytes;

        while inner.order.len() > inner.max_entries || inner.total_bytes > inner.max_bytes {
            let Some(old) = inner.order.pop_front() else {
                break;
            };
            if let Some(entry) = inner.map.remove(&old) {
                inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
            }
        }
        inner.map.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn items(size: usize) -> Vec<Value> {
        vec![json!({"text": "x".repeat(size)})]
    }

    #[test]
    fn evicts_when_byte_budget_is_exceeded() {
        let store = ResponseStore::new(10, 900, 800);
        let _ = store.put("a".into(), items(300), TurnMeta::default());
        let _ = store.put("b".into(), items(300), TurnMeta::default());
        assert_eq!(store.len(), 2);
        let _ = store.put("c".into(), items(300), TurnMeta::default());
        assert_eq!(store.len(), 2);
        assert!(store.get("a").is_none());
        assert!(store.get("b").is_some());
        assert!(store.get("c").is_some());
    }

    #[test]
    fn keeps_turn_identity_and_applies_new_limits() {
        let store = ResponseStore::new(10, 4096, 4096);
        let meta = TurnMeta {
            native: true,
            route_id: "r".into(),
            model: "m".into(),
            provider: "p".into(),
            upstream_model: "u".into(),
            provider_base_url: "http://example".into(),
        };
        assert!(store.put("id".into(), items(50), meta));
        let stored = store.get("id").unwrap();
        assert!(stored.meta.native);
        assert_eq!(stored.meta.model, "m");
        assert_eq!(stored.meta.provider, "p");
        store.set_limits(10, 4096, 10);
        assert!(store.get("id").is_none());
    }

    #[test]
    fn refuses_oversized_turns() {
        let store = ResponseStore::new(10, 4096, 128);
        assert!(!store.put("big".into(), items(512), TurnMeta::default()));
        assert!(store.get("big").is_none());
        assert_eq!(store.len(), 0);
    }
}
