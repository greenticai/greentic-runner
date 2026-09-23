//! A deployed worker's conversation must survive the process that parked it.
//!
//! These tests need a real Redis. `ci.yml`'s `redis-storage` job runs them
//! against a `redis:7-alpine` service container; locally:
//!
//! ```sh
//! docker run -d --name runner-redis -p 127.0.0.1:6399:6379 redis:7-alpine
//! REDIS_URL=redis://127.0.0.1:6399 \
//!   cargo test -p greentic-runner-host --test redis_lb_safety
//! ```
//!
//! With `REDIS_URL` unset they skip. A skip that reads as a pass is how a
//! durability gate stops gating, so the CI job also sets
//! `GREENTIC_REDIS_TESTS_REQUIRED=1`, under which an absent `REDIS_URL` is a
//! failure instead.

#[cfg(feature = "session-redis")]
mod redis_lb {
    use std::env;

    use anyhow::Result;
    use greentic_runner_host::engine::host::SessionKey;
    use greentic_runner_host::engine::runtime::{FlowResumeStore, IngressEnvelope};
    use greentic_runner_host::runner::engine::{ExecutionState, FlowSnapshot, FlowWait};
    use greentic_runner_host::storage::{
        DynStateStore, SessionBackend, StateBackend, StorageConfig, session_store_from_config,
        state_host_from, state_store_from_config,
    };
    use greentic_types::{EnvId, ReplyScope, TenantCtx, TenantId};
    use serde_json::json;

    /// A namespace nothing else in this file or in production uses, so a run
    /// against a shared Redis cannot collide with another.
    fn namespace(suffix: &str) -> String {
        format!("greentic:test:{}:{suffix}", std::process::id())
    }

    fn envelope_for(tenant: &str, conversation: &str) -> IngressEnvelope {
        IngressEnvelope {
            tenant: tenant.into(),
            env: Some("local".into()),
            pack_id: Some("pack.redis".into()),
            flow_id: "flow.main".into(),
            flow_type: Some("messaging".into()),
            action: Some("messaging".into()),
            // Left unset so the hint derives from `tenant`, which is what makes
            // the two-tenant isolation case below meaningful rather than a
            // restatement of the literal passed in.
            session_hint: None,
            provider: Some("provider".into()),
            messaging_endpoint_id: None,
            channel: Some(conversation.into()),
            conversation: Some(conversation.into()),
            user: Some("user".into()),
            entry_node: None,
            activity_id: Some("activity-redis".into()),
            timestamp: None,
            payload: json!({ "text": "hi" }),
            metadata: None,
            reply_scope: Some(ReplyScope {
                conversation: conversation.into(),
                thread: None,
                reply_to: None,
                correlation: None,
            }),
        }
        .canonicalize()
    }

    fn envelope() -> IngressEnvelope {
        envelope_for("demo", "conv")
    }

    fn wait_snapshot(next_node: &str) -> FlowWait {
        let state: ExecutionState = serde_json::from_value(json!({
            "input": { "text": "hi" },
            "nodes": {},
            "egress": []
        }))
        .expect("state");
        FlowWait {
            reason: Some("await-user".into()),
            snapshot: FlowSnapshot {
                pack_id: "pack.redis".into(),
                flow_id: "flow.main".into(),
                next_flow: None,
                next_node: next_node.into(),
                awaiting_submit: false,
                state,
            },
        }
    }

    /// `Some(url)` when a Redis is available, `None` when these tests should
    /// skip — and a panic when CI said one would be there and it is not.
    fn redis_url() -> Option<String> {
        let required = env::var("GREENTIC_REDIS_TESTS_REQUIRED")
            .map(|value| value == "1")
            .unwrap_or(false);
        match env::var("REDIS_URL") {
            Ok(value) if !value.trim().is_empty() => Some(value),
            _ if required => panic!(
                "GREENTIC_REDIS_TESTS_REQUIRED=1 but REDIS_URL is unset: the durability gate \
                 would have skipped silently"
            ),
            _ => {
                eprintln!("REDIS_URL not set; skipping redis durability test");
                None
            }
        }
    }

    fn resume_store(url: &str, namespace: &str) -> Result<FlowResumeStore> {
        let backend = SessionBackend::redis(url, namespace)?;
        Ok(FlowResumeStore::new(session_store_from_config(&backend)?))
    }

    fn state_store(url: &str) -> Result<DynStateStore> {
        state_store_from_config(&StateBackend::redis(url)?)
    }

    fn state_key(tenant: &str) -> Result<SessionKey> {
        let ctx = TenantCtx::new(EnvId::new("local")?, TenantId::new(tenant)?);
        Ok(SessionKey::new(
            &ctx,
            "pack.redis",
            "flow.main",
            Some("session".into()),
        ))
    }

    /// The property the whole feature exists for: a parked flow saved by one
    /// store instance is readable by a second one, which is what a revision
    /// rollout and a cold start each look like from the store's side.
    #[tokio::test]
    async fn a_parked_flow_survives_being_read_through_a_second_store_instance() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let namespace = namespace("resume");
        let envelope = envelope();

        let store_a = resume_store(&url, &namespace)?;
        let _ = store_a.save(&envelope, &wait_snapshot("node-a")).await?;
        drop(store_a);

        let store_b = resume_store(&url, &namespace)?;
        let snapshot = store_b
            .fetch(&envelope)
            .await?
            .expect("the parked snapshot did not survive the store instance");
        assert_eq!(snapshot.next_node, "node-a");

        store_b.clear(&envelope).await?;
        assert!(store_b.fetch(&envelope).await?.is_none());
        Ok(())
    }

    /// Same for flow state, which is the other half of a resumable turn.
    #[tokio::test]
    async fn flow_state_survives_being_read_through_a_second_store_instance() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let key = state_key("redistest")?;

        let host_a = state_host_from(state_store(&url)?);
        host_a.set_json(&key, json!({ "value": 1 })).await?;
        drop(host_a);

        let host_b = state_host_from(state_store(&url)?);
        assert_eq!(host_b.get_json(&key).await?, Some(json!({ "value": 1 })));
        host_b.del(&key).await?;
        assert_eq!(host_b.get_json(&key).await?, None);
        Ok(())
    }

    /// Two tenants on one Redis, one namespace. Isolation here comes from the
    /// session hint, which carries the tenant.
    #[tokio::test]
    async fn two_tenants_do_not_read_each_others_parked_flows() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let namespace = namespace("tenants");
        let acme = envelope_for("acme", "conv");
        let umbrella = envelope_for("umbrella", "conv");

        let store = resume_store(&url, &namespace)?;
        let _ = store.save(&acme, &wait_snapshot("acme-node")).await?;

        assert!(
            store.fetch(&umbrella).await?.is_none(),
            "a second tenant read the first tenant's parked flow"
        );

        let _ = store
            .save(&umbrella, &wait_snapshot("umbrella-node"))
            .await?;
        assert_eq!(
            store.fetch(&acme).await?.map(|s| s.next_node),
            Some("acme-node".to_string()),
            "the second tenant's save overwrote the first tenant's"
        );

        store.clear(&acme).await?;
        store.clear(&umbrella).await?;
        Ok(())
    }

    /// Two deployment environments on one Redis. Isolation here comes from the
    /// NAMESPACE, and only from it: a session entry key carries no environment,
    /// which is why `SessionBackend::redis` refuses to default one.
    #[tokio::test]
    async fn two_namespaces_do_not_read_each_others_parked_flows() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let envelope = envelope();

        let prod = resume_store(&url, &namespace("ns-prod"))?;
        let staging = resume_store(&url, &namespace("ns-staging"))?;

        let _ = prod.save(&envelope, &wait_snapshot("prod-node")).await?;
        assert!(
            staging.fetch(&envelope).await?.is_none(),
            "a second environment read the first environment's parked flow"
        );

        let _ = staging
            .save(&envelope, &wait_snapshot("staging-node"))
            .await?;
        assert_eq!(
            prod.fetch(&envelope).await?.map(|s| s.next_node),
            Some("prod-node".to_string()),
            "the second environment evicted the first environment's parked flow"
        );

        prod.clear(&envelope).await?;
        staging.clear(&envelope).await?;
        Ok(())
    }

    /// The state store takes no namespace because `greentic_state::key::fqn`
    /// puts the environment and tenant in every key. This is the test that says
    /// so out loud, so a bump that changed the key layout would be caught here
    /// rather than by a customer reading another customer's state.
    #[tokio::test]
    async fn two_tenants_do_not_read_each_others_flow_state() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let acme = state_key("acmeredis")?;
        let umbrella = state_key("umbrellaredis")?;

        let host = state_host_from(state_store(&url)?);
        host.set_json(&acme, json!({ "owner": "acme" })).await?;

        assert_eq!(
            host.get_json(&umbrella).await?,
            None,
            "a second tenant read the first tenant's flow state"
        );

        host.set_json(&umbrella, json!({ "owner": "umbrella" }))
            .await?;
        assert_eq!(
            host.get_json(&acme).await?,
            Some(json!({ "owner": "acme" })),
            "the second tenant's write landed on the first tenant's key"
        );

        host.del(&acme).await?;
        host.del(&umbrella).await?;
        Ok(())
    }

    /// A reachable backend builds. Paired with the unreachable cases in
    /// `tests/storage_config.rs`, which need no Redis.
    #[tokio::test]
    async fn a_reachable_backend_builds_both_stores() -> Result<()> {
        let Some(url) = redis_url() else {
            return Ok(());
        };
        let config = StorageConfig::redis(url.as_str(), namespace("build"))?;
        assert!(config.is_durable());
        let _ = session_store_from_config(&config.session)?;
        let _ = state_store_from_config(&config.state)?;
        Ok(())
    }
}

#[cfg(not(feature = "session-redis"))]
#[test]
fn redis_durability_tests_need_the_session_redis_feature() {
    // Not silently skipped: the feature is ON by default, so reaching this is
    // itself the finding.
    eprintln!("session-redis feature disabled; skipping redis durability tests");
}
