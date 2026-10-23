use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use wasmtime::component::Component;

use crate::cache::keys::ArtifactKey;

#[derive(Clone, Debug)]
pub struct MemoryCache {
    max_bytes: u64,
    lfu_protect_hits: u64,
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    entries: HashMap<ArtifactKey, CacheEntry>,
    lru: VecDeque<ArtifactKey>,
    total_bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

struct CacheEntry {
    component: Arc<Component>,
    bytes_estimate: u64,
    hit_count: u64,
    pinned: bool,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheEntry")
            .field("bytes_estimate", &self.bytes_estimate)
            .field("hit_count", &self.hit_count)
            .field("pinned", &self.pinned)
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: u64,
    pub total_bytes: u64,
}

impl MemoryCache {
    pub fn new(max_bytes: u64, lfu_protect_hits: u64) -> Self {
        Self {
            max_bytes,
            lfu_protect_hits,
            state: Arc::new(Mutex::new(MemoryState::default())),
        }
    }

    pub fn get(&self, key: &ArtifactKey) -> Option<Arc<Component>> {
        let mut state = self.state.lock().ok()?;
        if state.entries.contains_key(key) {
            state.hits = state.hits.saturating_add(1);
            let component = {
                let entry = state.entries.get_mut(key)?;
                entry.hit_count = entry.hit_count.saturating_add(1);
                Arc::clone(&entry.component)
            };
            touch_lru(&mut state.lru, key);
            return Some(component);
        }
        state.misses = state.misses.saturating_add(1);
        None
    }

    pub fn insert(
        &self,
        key: ArtifactKey,
        value: Arc<Component>,
        bytes_estimate: usize,
        pinned: bool,
    ) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let bytes_estimate = bytes_estimate as u64;
        if let Some(existing) = state.entries.remove(&key) {
            state.total_bytes = state.total_bytes.saturating_sub(existing.bytes_estimate);
            remove_lru(&mut state.lru, &key);
        }
        state.entries.insert(
            key.clone(),
            CacheEntry {
                component: value,
                bytes_estimate,
                hit_count: 0,
                pinned,
            },
        );
        state.total_bytes = state.total_bytes.saturating_add(bytes_estimate);
        state.lru.push_front(key.clone());
        self.evict_if_needed(&mut state);
    }

    pub fn stats(&self) -> MemoryStats {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return MemoryStats::default(),
        };
        MemoryStats {
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
            entries: state.entries.len() as u64,
            total_bytes: state.total_bytes,
        }
    }

    fn evict_if_needed(&self, state: &mut MemoryState) {
        if self.max_bytes == 0 {
            return;
        }
        let mut attempts = state.lru.len();
        while state.total_bytes > self.max_bytes && attempts > 0 {
            attempts -= 1;
            let Some(candidate) = state.lru.pop_back() else {
                break;
            };
            if should_skip_candidate(state, &candidate, true, self.lfu_protect_hits) {
                state.lru.push_front(candidate);
                continue;
            }
            if let Some(entry) = state.entries.remove(&candidate) {
                state.total_bytes = state.total_bytes.saturating_sub(entry.bytes_estimate);
                state.evictions = state.evictions.saturating_add(1);
            }
        }
        if state.total_bytes <= self.max_bytes {
            return;
        }
        let mut attempts = state.lru.len();
        while state.total_bytes > self.max_bytes && attempts > 0 {
            attempts -= 1;
            let Some(candidate) = state.lru.pop_back() else {
                break;
            };
            if should_skip_candidate(state, &candidate, false, self.lfu_protect_hits) {
                state.lru.push_front(candidate);
                continue;
            }
            if let Some(entry) = state.entries.remove(&candidate) {
                state.total_bytes = state.total_bytes.saturating_sub(entry.bytes_estimate);
                state.evictions = state.evictions.saturating_add(1);
            }
        }
    }
}

fn should_skip_candidate(
    state: &MemoryState,
    key: &ArtifactKey,
    protect_lfu: bool,
    lfu_threshold: u64,
) -> bool {
    let Some(entry) = state.entries.get(key) else {
        return false;
    };
    if entry.pinned {
        return true;
    }
    if protect_lfu && lfu_threshold > 0 && entry.hit_count >= lfu_threshold {
        return true;
    }
    false
}

fn touch_lru(lru: &mut VecDeque<ArtifactKey>, key: &ArtifactKey) {
    if let Some(pos) = lru.iter().position(|item| item == key) {
        lru.remove(pos);
        lru.push_front(key.clone());
    }
}

fn remove_lru(lru: &mut VecDeque<ArtifactKey>, key: &ArtifactKey) {
    if let Some(pos) = lru.iter().position(|item| item == key) {
        lru.remove(pos);
    }
}
