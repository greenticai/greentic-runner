use std::sync::Arc;

use crate::cache::engine_profile::{CpuPolicy, EngineProfile};
use crate::cache::keys::ArtifactKey;
use crate::cache::memory::MemoryCache;

fn build_component() -> Arc<wasmtime::component::Component> {
    let engine = wasmtime::Engine::default();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/packs/secrets_store_smoke/components/echo_secret.wasm");
    let bytes = std::fs::read(path).expect("fixture wasm");
    Arc::new(wasmtime::component::Component::from_binary(&engine, &bytes).expect("component"))
}

fn key(id: &str) -> ArtifactKey {
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    ArtifactKey::new(profile.id().to_string(), id.to_string())
}

#[test]
fn lru_eviction_order() {
    let cache = MemoryCache::new(10, 2);
    let component = build_component();
    cache.insert(key("sha256:one"), Arc::clone(&component), 6, false);
    cache.insert(key("sha256:two"), Arc::clone(&component), 6, false);
    assert!(cache.get(&key("sha256:one")).is_none());
    assert!(cache.get(&key("sha256:two")).is_some());
}

#[test]
fn lfu_protect_keeps_hot_entries() {
    let cache = MemoryCache::new(10, 2);
    let component = build_component();
    let hot = key("sha256:hot");
    let cold = key("sha256:cold");
    cache.insert(hot.clone(), Arc::clone(&component), 6, false);
    cache.get(&hot);
    cache.get(&hot);
    cache.insert(cold.clone(), Arc::clone(&component), 6, false);
    cache.insert(key("sha256:new"), Arc::clone(&component), 6, false);
    assert!(cache.get(&hot).is_some());
}

#[test]
fn pinned_entries_survive_eviction() {
    let cache = MemoryCache::new(10, 1);
    let component = build_component();
    let pinned = key("sha256:pinned");
    cache.insert(pinned.clone(), Arc::clone(&component), 6, true);
    cache.insert(key("sha256:other"), Arc::clone(&component), 6, false);
    assert!(cache.get(&pinned).is_some());
}

#[test]
fn stats_track_hits_and_bytes() {
    let cache = MemoryCache::new(20, 1);
    let component = build_component();
    let first = key("sha256:one");
    cache.insert(first.clone(), Arc::clone(&component), 8, false);
    cache.get(&first);
    cache.get(&first);
    cache.get(&key("sha256:missing"));
    let stats = cache.stats();
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 1);
    assert!(stats.total_bytes >= 8);
}

#[test]
fn second_pass_evicts_hot_entries_when_first_pass_evicts_nothing() {
    let cache = MemoryCache::new(10, 1);
    let component = build_component();
    let hot = key("sha256:hot");
    cache.insert(hot.clone(), Arc::clone(&component), 6, false);
    cache.get(&hot);
    // Pinned insert cannot be evicted and the hot entry is LFU-protected in the
    // first pass, so only the second pass can bring the cache under budget.
    cache.insert(key("sha256:pinned"), Arc::clone(&component), 6, true);
    assert!(cache.get(&hot).is_none());
    assert!(cache.stats().total_bytes <= 10);
}
