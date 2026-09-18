//! `POST /agent/chat` — a loopback HTTP ingress that wraps `RunnerHost::handle_activity`
//! so an external caller (the designer's runner sidecar) can send a chat turn to a
//! loaded agentic-worker pack and receive the reply. Blocking JSON response (v1).

use serde::{Deserialize, Serialize};

use crate::activity::Activity;

/// One chat turn for a loaded worker pack.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentChatRequest {
    pub text: String,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub flow_id: Option<String>,
}

/// One outbound reply line.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyView {
    pub text: String,
}

/// The worker's reply turn.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentChatResponse {
    pub replies: Vec<ReplyView>,
}

/// Extract a human-readable reply line from an outbound activity.
///
/// Priority order:
/// 1. `payload["text"]` as a string
/// 2. `payload["messages"][0]["text"]` as a string
/// 3. Compact JSON rendering of the whole payload
fn reply_text(activity: &Activity) -> String {
    let payload = activity.payload();
    if let Some(t) = payload.get("text").and_then(|v| v.as_str()) {
        return t.to_string();
    }
    if let Some(t) = payload
        .get("messages")
        .and_then(|m| m.get(0))
        .and_then(|m0| m0.get("text"))
        .and_then(|v| v.as_str())
    {
        return t.to_string();
    }
    serde_json::to_string(payload).unwrap_or_default()
}

/// Map the runtime's outbound activities into the chat response, dropping empties.
pub fn replies_to_response(activities: Vec<Activity>) -> AgentChatResponse {
    let replies = activities
        .iter()
        .map(reply_text)
        .filter(|t| !t.trim().is_empty())
        .map(|text| ReplyView { text })
        .collect();
    AgentChatResponse { replies }
}

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, extract::State};

use crate::host::RunnerHost;
use crate::http::auth::AdminGuard;
use crate::runner::ServerState;

/// Default conversation/user identifiers so a caller that omits them still
/// threads a single in-memory conversation across turns.
const DEFAULT_CONVERSATION: &str = "test-chat";
const DEFAULT_USER: &str = "test-chat-user";

/// Extracted core logic — separated so tests can exercise tenant resolution
/// and error mapping without needing the full axum extractor stack.
async fn execute_chat(
    host: &RunnerHost,
    default_tenant: &str,
    req: AgentChatRequest,
) -> Result<AgentChatResponse, (StatusCode, serde_json::Value)> {
    let tenant = req
        .tenant
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| default_tenant.to_string());

    let mut activity = Activity::text(req.text)
        .in_conversation(
            req.conversation_id
                .unwrap_or_else(|| DEFAULT_CONVERSATION.to_string()),
        )
        .from_user(req.user_id.unwrap_or_else(|| DEFAULT_USER.to_string()));
    if let Some(flow) = req.flow_id {
        activity = activity.with_flow(flow);
    }

    match host.handle_activity(&tenant, activity).await {
        Ok(activities) => Ok(replies_to_response(activities)),
        Err(e) => {
            let msg = format!("{e:#}");
            // handle_activity returns "tenant <name> not loaded" when the tenant
            // isn't present in ActivePacks. Surface that as 404.
            let (code, error) = if msg.contains("not loaded") {
                (StatusCode::NOT_FOUND, "tenant_not_loaded")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "agent_chat_failed")
            };
            Err((code, serde_json::json!({ "error": error, "message": msg })))
        }
    }
}

/// `POST /agent/chat` — loopback-only (AdminGuard). Sends one chat turn to
/// the loaded worker pack and returns its reply.
pub async fn agent_chat(
    _guard: AdminGuard,
    State(state): State<ServerState>,
    Json(req): Json<AgentChatRequest>,
) -> impl IntoResponse {
    match execute_chat(&state.host, state.routing.default_tenant(), req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err((code, body)) => (code, Json(body)).into_response(),
    }
}

#[cfg(feature = "agentic-worker")]
use axum::response::sse::{Event, KeepAlive, Sse};
#[cfg(feature = "agentic-worker")]
use futures::{Stream, StreamExt};
#[cfg(feature = "agentic-worker")]
use std::convert::Infallible;
#[cfg(feature = "agentic-worker")]
use std::sync::Arc;

#[cfg(feature = "agentic-worker")]
use crate::http::agent_stream::{SseForwardObserver, StreamFrame, StreamObserverRegistry};

/// Extract the first non-empty reply line from a turn's outbound activities,
/// using the same mapping `replies_to_response` uses for the non-streaming
/// `/agent/chat` endpoint.
#[cfg(feature = "agentic-worker")]
fn first_nonempty_reply(activities: &[Activity]) -> Option<String> {
    activities
        .iter()
        .map(reply_text)
        .find(|t| !t.trim().is_empty())
}

/// RAII guard: removes this turn's entry from `stream_observers` on every
/// exit path (Ok, Err, or a panic unwinding through the task). The registry
/// is keyed only by `conversationId`, so a stale guard from an earlier turn
/// must not evict a newer turn's observer — `Drop` only removes the entry
/// when it is still THIS turn's observer (`Arc::ptr_eq`), so a guard whose
/// entry was already replaced by a newer overlapping turn is a no-op.
#[cfg(feature = "agentic-worker")]
struct Cleanup {
    registry: StreamObserverRegistry,
    conversation_id: String,
    observer: Arc<dyn greentic_aw_runtime::StepObserver>,
}

#[cfg(feature = "agentic-worker")]
impl Cleanup {
    /// The remove-if predicate, extracted so it can be exercised directly by
    /// tests without needing a full `tokio::spawn` + `Drop` round-trip.
    fn remove_stale(
        registry: &StreamObserverRegistry,
        conversation_id: &str,
        observer: &Arc<dyn greentic_aw_runtime::StepObserver>,
    ) {
        registry.remove_if(conversation_id, |_k, stored| Arc::ptr_eq(stored, observer));
    }
}

#[cfg(feature = "agentic-worker")]
impl Drop for Cleanup {
    fn drop(&mut self) {
        Self::remove_stale(&self.registry, &self.conversation_id, &self.observer);
    }
}

/// Build the ingress [`Activity`] for one `/agent/chat/stream` turn.
///
/// The session id is stamped with the `conversationId` on purpose. The SSE
/// handler registers this turn's stream observer keyed by the raw
/// `conversationId` (`stream_observers.insert(conversation_id, ..)`), while the
/// `dw.agent` node looks that observer up by the node's `session_id` — which is
/// the canonical ingress session hint. Leaving the session unset makes
/// `IngressEnvelope::canonicalize` derive a 5-part
/// `{tenant}:{provider}:{channel}:{conversation}:{user}` hint that never equals
/// the bare `conversationId`, so the lookup misses, the turn runs with no
/// observer, and it silently falls back to a single non-streamed reply (no
/// token / tool / llm-call frames — the "no trace log" bug). Stamping the
/// session keeps the hint bare so the registration and lookup keys agree.
#[cfg(feature = "agentic-worker")]
fn build_stream_turn_activity(
    text: String,
    conversation_id: String,
    user_id: Option<String>,
    flow_id: Option<String>,
) -> Activity {
    let mut activity = Activity::text(text)
        .with_session(conversation_id.clone())
        .in_conversation(conversation_id)
        .from_user(user_id.unwrap_or_else(|| DEFAULT_USER.to_string()));
    if let Some(flow) = flow_id {
        activity = activity.with_flow(flow);
    }
    activity
}

/// Extractor-free core: registers a streaming observer under the
/// conversation id, runs the turn on a background task, and returns a stream
/// of frames. Mirrors `execute_chat`'s extractor-free split so tests can
/// drive it without axum's extractor stack.
///
/// The `stream_observers` registry assumes at most one in-flight streaming
/// turn per `conversationId`; a second overlapping turn on the same id
/// replaces the first turn's registry entry (see `Cleanup` above for how the
/// first turn's eventual cleanup avoids evicting the replacement).
#[cfg(feature = "agentic-worker")]
pub fn agent_chat_stream_core(
    state: &ServerState,
    req: AgentChatRequest,
) -> impl Stream<Item = StreamFrame> + use<> {
    let (frame_tx, frame_rx) = tokio::sync::mpsc::unbounded_channel::<StreamFrame>();
    let conversation_id = req
        .conversation_id
        .clone()
        .unwrap_or_else(|| DEFAULT_CONVERSATION.to_string());

    let observer = Arc::new(SseForwardObserver::new(frame_tx.clone()));
    let observer_dyn: Arc<dyn greentic_aw_runtime::StepObserver> = observer.clone();
    state
        .stream_observers
        .insert(conversation_id.clone(), observer_dyn.clone());

    let host = state.host.clone();
    let default_tenant = state.routing.default_tenant().to_string();
    let registry: StreamObserverRegistry = state.stream_observers.clone();

    tokio::spawn(async move {
        // See `Cleanup`'s doc comment for why this guard is ptr_eq-protected.
        let _cleanup = Cleanup {
            registry,
            conversation_id: conversation_id.clone(),
            observer: observer_dyn,
        };

        let tenant = req.tenant.unwrap_or(default_tenant);
        let activity =
            build_stream_turn_activity(req.text, conversation_id, req.user_id, req.flow_id);

        match host.handle_activity(&tenant, activity).await {
            Ok(activities) => {
                // No-delta fallback: only synthesize a single `TextChunk`
                // from the assembled reply when the backend never streamed
                // token deltas via the observer — a streaming backend
                // already delivered its text incrementally, so emitting the
                // reply again here would double-print it.
                if !observer.streamed()
                    && let Some(reply) = first_nonempty_reply(&activities)
                {
                    let _ = frame_tx.send(StreamFrame::TextChunk { text: reply });
                }
                let _ = frame_tx.send(StreamFrame::Done);
            }
            Err(e) => {
                let _ = frame_tx.send(StreamFrame::Error {
                    message: format!("{e:#}"),
                });
            }
        }
    });

    futures::stream::unfold(frame_rx, |mut rx| async move {
        rx.recv().await.map(|frame| (frame, rx))
    })
}

/// `POST /agent/chat/stream` — loopback-only (AdminGuard). SSE variant of
/// `/agent/chat`: streams token/tool frames then a terminal `done`/`error`
/// frame. Headers are sent as soon as the stream opens, so a turn that fails
/// (e.g. unknown tenant) still returns 200 with an `error` frame rather than
/// a 4xx/5xx status.
#[cfg(feature = "agentic-worker")]
pub async fn agent_chat_stream(
    _guard: AdminGuard,
    State(state): State<ServerState>,
    Json(req): Json<AgentChatRequest>,
) -> impl IntoResponse {
    let stream = agent_chat_stream_core(&state, req).map(|frame| {
        let data = serde_json::to_string(&frame).unwrap_or_else(|_| "null".into());
        Ok::<Event, Infallible>(Event::default().event("frame").data(data))
    });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keepalive"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::Activity;
    use serde_json::json;

    fn reply_with(payload: serde_json::Value) -> Activity {
        // Build an outbound-style activity carrying `payload`. Use the same
        // constructor the runner uses for replies (Activity::from_output) —
        // read activity.rs and match it; here we assert on the mapping only.
        Activity::from_output(payload, "demo")
    }

    #[test]
    fn maps_text_payload_to_reply() {
        let out = replies_to_response(vec![reply_with(json!({"text": "hello there"}))]);
        assert_eq!(out.replies.len(), 1);
        assert_eq!(out.replies[0].text, "hello there");
    }

    #[test]
    fn maps_nested_messages_text() {
        let out = replies_to_response(vec![reply_with(
            json!({"messages": [{"text": "nested hi"}]}),
        )]);
        assert_eq!(out.replies[0].text, "nested hi");
    }

    #[test]
    fn skips_empty_and_keeps_order() {
        let out = replies_to_response(vec![
            reply_with(json!({"text": ""})),
            reply_with(json!({"text": "second"})),
        ]);
        assert_eq!(out.replies.len(), 1);
        assert_eq!(out.replies[0].text, "second");
    }

    #[test]
    fn request_deserializes_camel_case() {
        let r: AgentChatRequest = serde_json::from_value(json!({
            "text": "hi", "conversationId": "c1", "userId": "u1"
        }))
        .unwrap();
        assert_eq!(r.text, "hi");
        assert_eq!(r.conversation_id.as_deref(), Some("c1"));
        assert_eq!(r.user_id.as_deref(), Some("u1"));
    }

    /// Route wiring smoke-test: `execute_chat` against a host with no loaded
    /// packs returns 404 with `error = "tenant_not_loaded"`, proving that
    /// `handle_activity`'s "not loaded" error is correctly mapped by the handler
    /// core.
    #[tokio::test]
    async fn agent_chat_unknown_tenant_maps_to_not_found() {
        let host = crate::host::RunnerHost::for_test();
        let req = AgentChatRequest {
            text: "hello".into(),
            tenant: Some("nope".into()),
            conversation_id: None,
            user_id: None,
            flow_id: None,
        };
        let err = execute_chat(&host, "test", req)
            .await
            .expect_err("unknown tenant should fail");
        assert_eq!(err.0, StatusCode::NOT_FOUND);
        assert_eq!(err.1["error"], "tenant_not_loaded");
    }

    /// With no loaded pack, the turn errors; the SSE core must still deliver a
    /// single `error` frame (not panic / not silently swallow), and the
    /// conversation's registry entry must be cleaned up afterwards (RAII guard).
    #[cfg(feature = "agentic-worker")]
    #[tokio::test]
    async fn agent_chat_stream_unknown_tenant_emits_error_frame_not_500() {
        let state = crate::runner::ServerState::for_test();
        let req = AgentChatRequest {
            text: "hi".into(),
            tenant: Some("nope".into()),
            conversation_id: Some("c1".into()),
            user_id: None,
            flow_id: None,
        };
        let frames = drive_stream_to_vec(agent_chat_stream_core(&state, req)).await;
        assert!(
            matches!(frames.last(), Some(StreamFrame::Error { .. })),
            "got {frames:?}"
        );
        // registry cleaned up after the turn
        assert!(state.stream_observers.get("c1").is_none());
    }

    /// Regression for the silent "no trace" bug on `/agent/chat/stream`: the
    /// SSE handler registers the turn's stream observer keyed by the raw
    /// `conversationId` (`stream_observers.insert(conversation_id, ..)`), but the
    /// dw.agent node looks it up by the canonical ingress session hint. When the
    /// activity carries no explicit session, `canonicalize` derives a 5-part
    /// `{tenant}:{provider}:{channel}:{conversation}:{user}` hint that never
    /// equals the bare `conversationId`, the observer is never found, and the
    /// turn silently falls back to a single non-streamed reply (no tool / token
    /// frames). Stamping session = conversationId keeps the hint bare so the two
    /// keys match.
    #[cfg(feature = "agentic-worker")]
    #[test]
    fn stream_turn_activity_stamps_session_with_conversation_id() {
        let activity = build_stream_turn_activity("hi".into(), "conv-uuid-1".into(), None, None);
        assert_eq!(
            activity.session_id(),
            Some("conv-uuid-1"),
            "stream turn must stamp session = conversationId so the canonical \
             hint matches the observer registry key"
        );
        assert_eq!(activity.conversation(), Some("conv-uuid-1"));
    }

    #[cfg(feature = "agentic-worker")]
    async fn drive_stream_to_vec(
        stream: impl futures::Stream<Item = StreamFrame>,
    ) -> Vec<StreamFrame> {
        use futures::StreamExt;
        stream.collect().await
    }

    /// Two overlapping turns on the same `conversationId`: the second
    /// `insert` replaces the first turn's observer. The first turn's stale
    /// `Cleanup` must not evict the second turn's observer (ptr_eq guard);
    /// the second turn's own cleanup does remove it.
    #[cfg(feature = "agentic-worker")]
    #[test]
    fn cleanup_remove_stale_does_not_evict_a_replaced_observer() {
        let registry: StreamObserverRegistry = Arc::new(dashmap::DashMap::new());
        let (tx_a, _rx_a) = tokio::sync::mpsc::unbounded_channel();
        let (tx_b, _rx_b) = tokio::sync::mpsc::unbounded_channel();
        let observer_a: Arc<dyn greentic_aw_runtime::StepObserver> =
            Arc::new(SseForwardObserver::new(tx_a));
        let observer_b: Arc<dyn greentic_aw_runtime::StepObserver> =
            Arc::new(SseForwardObserver::new(tx_b));

        registry.insert("c1".to_string(), observer_a.clone());
        // A second overlapping turn on the same conversation id replaces A.
        registry.insert("c1".to_string(), observer_b.clone());

        // A's stale cleanup must not evict B's entry.
        Cleanup::remove_stale(&registry, "c1", &observer_a);
        assert!(
            registry
                .get("c1")
                .is_some_and(|e| Arc::ptr_eq(e.value(), &observer_b)),
            "stale cleanup for A must not evict B's observer"
        );

        // B's own cleanup removes its entry.
        Cleanup::remove_stale(&registry, "c1", &observer_b);
        assert!(registry.get("c1").is_none());
    }
}
