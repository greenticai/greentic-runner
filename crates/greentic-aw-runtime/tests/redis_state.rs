//! Integration tests against a real Redis instance.
//!
//! The whole module skips gracefully if `REDIS_URL` is not set.
//! CI sets `REDIS_URL=redis://localhost:6379/15` (DB 15 to avoid
//! stepping on shared dev state).

use greentic_aw_runtime::state::{AgentStateStore, ChatMessage, ConversationState};
use greentic_aw_runtime::state_redis::RedisAgentStateStore;
use greentic_aw_runtime::tenant::TenantContext;

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

async fn make_store() -> RedisAgentStateStore {
    let url = redis_url().expect("REDIS_URL must be set for integration tests");
    RedisAgentStateStore::connect(&url)
        .await
        .expect("redis connect")
}

#[tokio::test]
#[allow(clippy::panic)] // diagnostic else-branch in test — intentional
async fn save_then_load_roundtrips_state() {
    let Some(_) = redis_url() else {
        eprintln!("REDIS_URL unset; skipping");
        return;
    };
    let store = make_store().await;
    let tc = TenantContext::new("test-acme", "test-prod");
    let session = format!("sess-{}", uuid::Uuid::new_v4());

    let mut state = ConversationState::empty(&tc, &session);
    state.messages.push(ChatMessage::User {
        content: "hello".into(),
        attachments: Vec::new(),
    });
    store.save(&tc, &session, &state).await.unwrap();

    let loaded = store.load(&tc, &session).await.unwrap();
    assert_eq!(loaded.messages.len(), 1);
    if let ChatMessage::User { content, .. } = &loaded.messages[0] {
        assert_eq!(content, "hello");
    } else {
        panic!("expected User message");
    }
}

#[tokio::test]
#[allow(clippy::panic)] // diagnostic else-branch in test — intentional
async fn state_survives_store_drop_and_rebuild() {
    // Acceptance §8.5 #6: conversation state persists across a runtime
    // drop/rebuild (simulating a runner-host restart).
    let Some(url) = redis_url() else {
        eprintln!("REDIS_URL unset; skipping");
        return;
    };
    let tenant = TenantContext::new("acme", "prod");
    let session = format!("restart-{}", uuid::Uuid::new_v4());

    // First "runner instance": save a conversation, then drop the store.
    {
        let store = RedisAgentStateStore::connect(&url).await.unwrap();
        let mut state = ConversationState::empty(&tenant, &session);
        state.messages.push(ChatMessage::User {
            content: "remember me".into(),
            attachments: Vec::new(),
        });
        store.save(&tenant, &session, &state).await.unwrap();
    } // store dropped — simulates a runner restart

    // Second "runner instance": a fresh store loads the persisted state.
    let store2 = RedisAgentStateStore::connect(&url).await.unwrap();
    let loaded = store2.load(&tenant, &session).await.unwrap();
    let has_msg = loaded.messages.iter().any(|m| {
        matches!(
            m,
            ChatMessage::User { content, .. } if content == "remember me"
        )
    });
    assert!(
        has_msg,
        "conversation state must persist across store drop/rebuild"
    );
}

#[tokio::test]
async fn load_returns_empty_state_when_no_record_exists() {
    let Some(_) = redis_url() else {
        return;
    };
    let store = make_store().await;
    let tc = TenantContext::new("test-acme", "test-prod");
    let session = format!("never-existed-{}", uuid::Uuid::new_v4());

    let loaded = store.load(&tc, &session).await.unwrap();
    assert_eq!(loaded.schema_version, 1);
    assert_eq!(loaded.session_id, session);
    assert!(loaded.messages.is_empty());
}

#[tokio::test]
async fn load_returns_schema_incompatible_when_version_in_future() {
    let Some(url) = redis_url() else {
        return;
    };
    let store = RedisAgentStateStore::connect(&url).await.unwrap();
    let tc = TenantContext::new("test-acme", "test-prod");
    let session = format!("future-{}", uuid::Uuid::new_v4());

    // Direct Redis write of a state blob with schema_version=99.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    let key = format!("aw:test-acme:test-prod:{session}:state");
    let payload = serde_json::json!({
        "schema_version": 99,
        "session_id": session,
        "tenant_id":  "test-acme",
        "env_id":     "test-prod",
        "messages":   [],
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    });
    use redis::AsyncCommands;
    let _: () = conn.set_ex(&key, payload.to_string(), 60).await.unwrap();

    let err = store.load(&tc, &session).await.unwrap_err();
    match err {
        greentic_aw_runtime::error::StateError::SchemaIncompatible { found, supported } => {
            assert_eq!(found, 99);
            assert_eq!(supported, 1);
        }
        other => panic!("expected SchemaIncompatible, got {other:?}"),
    }
}

#[tokio::test]
async fn second_acquire_lock_blocks_until_first_releases() {
    let Some(_) = redis_url() else {
        return;
    };
    let store_a = make_store().await;
    let store_b = make_store().await;
    let tc = TenantContext::new("test-acme", "test-prod");
    let session = format!("lock-{}", uuid::Uuid::new_v4());

    let lock1 = store_a
        .acquire_lock(&tc, &session, std::time::Duration::from_millis(500))
        .await
        .unwrap();

    // Concurrent acquire from store_b should time out within ~500ms.
    let started = std::time::Instant::now();
    let result = store_b
        .acquire_lock(&tc, &session, std::time::Duration::from_millis(500))
        .await;
    assert!(started.elapsed() >= std::time::Duration::from_millis(400));
    assert!(matches!(
        result,
        Err(greentic_aw_runtime::error::StateError::LockTimeout(_))
    ));

    drop(lock1);
    // After drop, the lock releases (best-effort) within ~200ms.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let lock2 = store_b
        .acquire_lock(&tc, &session, std::time::Duration::from_millis(500))
        .await
        .unwrap();
    drop(lock2);
}

#[tokio::test]
async fn lock_refresh_extends_ttl() {
    let Some(url) = redis_url() else {
        return;
    };
    let store = make_store().await;
    let tc = TenantContext::new("test-acme", "test-prod");
    let session = format!("refresh-{}", uuid::Uuid::new_v4());

    let lock = store
        .acquire_lock(&tc, &session, std::time::Duration::from_millis(500))
        .await
        .unwrap();

    // Inspect the raw TTL.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    let key = format!("aw:test-acme:test-prod:{session}:lock");
    use redis::AsyncCommands;
    let ttl_before: i64 = conn.ttl(&key).await.unwrap();
    assert!(
        ttl_before > 80,
        "expected initial TTL ~90s, got {ttl_before}"
    );

    // Wait, then refresh.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    lock.refresh().await.unwrap();
    let ttl_after: i64 = conn.ttl(&key).await.unwrap();
    assert!(
        ttl_after > ttl_before - 1,
        "refresh should reset TTL to ~90, got {ttl_after}"
    );
    drop(lock);
}

#[tokio::test]
#[allow(clippy::panic)] // diagnostic else-branch in test — intentional
async fn two_tenants_share_redis_without_cross_talk() {
    let Some(_) = redis_url() else {
        return;
    };
    let store = make_store().await;
    let a = TenantContext::new("acme", "prod");
    let b = TenantContext::new("beta", "prod");
    let session = format!("shared-{}", uuid::Uuid::new_v4());

    let mut state_a = ConversationState::empty(&a, &session);
    state_a.messages.push(ChatMessage::User {
        content: "from-acme".into(),
        attachments: Vec::new(),
    });
    store.save(&a, &session, &state_a).await.unwrap();

    let mut state_b = ConversationState::empty(&b, &session);
    state_b.messages.push(ChatMessage::User {
        content: "from-beta".into(),
        attachments: Vec::new(),
    });
    store.save(&b, &session, &state_b).await.unwrap();

    let loaded_a = store.load(&a, &session).await.unwrap();
    let loaded_b = store.load(&b, &session).await.unwrap();
    if let (ChatMessage::User { content: ca, .. }, ChatMessage::User { content: cb, .. }) =
        (&loaded_a.messages[0], &loaded_b.messages[0])
    {
        assert_eq!(ca, "from-acme");
        assert_eq!(cb, "from-beta");
    } else {
        panic!("expected User messages on both tenants");
    }
}
