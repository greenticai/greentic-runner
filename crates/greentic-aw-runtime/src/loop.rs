//! Plan-Act-Observe agent loop. Spec §5.3.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::error::{AgentError, LlmError, TerminationReason};
use crate::llm::LlmRequest;
use crate::state::{ChatMessage, ConversationState};
use crate::telemetry::StepTelemetryCtx;
use crate::tenant::TenantContext;
use crate::tools::{dispatch_tool_call, is_tool_allowed, list_tools_for_llm};
use crate::{AgentInput, AgentOutput, AgentRuntime, AgentStep, StepObserver, StepUsage};

/// Agents already warned about unavailable tools, so the loud preflight warning
/// fires once per agent per process (the loop resolves tools every iteration —
/// we must not warn on each one).
static PREFLIGHT_WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Emit one loud, actionable warning when an agent declares tools that will not
/// reach the LLM. No-op when nothing is missing; deduplicated per agent.
///
/// Without this the runtime drops unresolved tools silently and the agent runs
/// with a reduced — or empty — tool set, then fabricates tool results.
fn preflight_warn_tools(agent_id: &str, missing: &[crate::tools::MissingTool], declared: usize) {
    if missing.is_empty() {
        return;
    }
    let mut seen = PREFLIGHT_WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !seen.insert(agent_id.to_string()) {
        return;
    }
    let details = missing
        .iter()
        .map(|m| format!("    - {}/{}: {}", m.extension_id, m.tool_name, m.reason))
        .collect::<Vec<_>>()
        .join("\n");
    let message = format!(
        "agentic worker tools unavailable — the agent will hallucinate tool results \
         until fixed:\n{details}\n  fix: install the extension as a signed pack (with \
         manifest.json) from the store, or run a dev-allow-unsigned runner with \
         GREENTIC_EXT_ALLOW_UNSIGNED=1"
    );
    warn!(
        agent = %agent_id,
        missing = missing.len(),
        declared,
        "{}",
        message
    );
}

pub async fn run_step(
    runtime: &AgentRuntime,
    tenant: TenantContext,
    session_id: &str,
    agent_id: &str,
    message: AgentInput,
    observer: Arc<dyn StepObserver>,
) -> Result<AgentOutput, AgentError> {
    let started = Instant::now();
    let config = runtime
        .config_provider
        .agent_config(&tenant, agent_id)
        .await?;

    // --- Cost budget gate (spec Decision 14) ---
    if let Some(cap) = config.limits.daily_token_cap_per_tenant {
        let used = runtime.token_meter.current(&tenant).await?;
        if used >= u64::from(cap) {
            return Err(AgentError::TokenBudgetExceeded);
        }
    }

    // --- Credit balance gate (Slice D2): fail-open on billing unavailability ---
    if runtime.billing_meter.over_budget(&tenant).await {
        return Err(AgentError::CreditBudgetExceeded);
    }

    // --- Acquire distributed lock (default wait 5s) ---
    let lock = runtime
        .state_store
        .acquire_lock(&tenant, session_id, Duration::from_secs(5))
        .await
        .map_err(|e| match e {
            crate::error::StateError::LockTimeout(_) => AgentError::LockTimeout,
            other => AgentError::StateLoad(other),
        })?;

    // --- Load state (best-effort; empty on failure) ---
    let mut state = match runtime.state_store.load(&tenant, session_id).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "state load failed; proceeding with empty state");
            ConversationState::empty(&tenant, session_id)
        }
    };
    // --- Opening-message short-circuit ---
    // On the FIRST turn with no user text (the flow entered the agent via a
    // button/card rather than a typed message), reply with the author-configured
    // opening message instead of spending an LLM call to fabricate a greeting.
    // Recorded in state so the conversation has context; `FinalReply` so a
    // conversational agent then parks, awaiting the user's real first message.
    if state.messages.is_empty()
        && message.text.trim().is_empty()
        && let Some(opening) = config
            .opening_message
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    {
        let opening = opening.to_string();
        state.messages.push(ChatMessage::Assistant {
            content: opening.clone(),
            tool_calls: vec![],
        });
        if let Err(e) = runtime.state_store.save(&tenant, session_id, &state).await {
            warn!(error = %e, "state save failed after opening message");
        }
        return Ok(AgentOutput {
            reply: opening,
            trail: Vec::new(),
            terminated_by: TerminationReason::FinalReply,
            usage: StepUsage::default(),
        });
    }

    // --- Assemble guardrail chain (once per step, before any message push) ---
    // Mandatory refs from the platform policy are resolved first; if any
    // mandatory guardrail cannot be resolved the agent is blocked (fail-closed).
    let guardrail_chain = {
        let registry = runtime.ext_runtime.capability_registry();
        let mandatory = match runtime.guardrail_policy.mandatory_guardrails(&tenant).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "mandatory guardrail policy unavailable; failing closed");
                return Err(AgentError::GuardrailDenied {
                    direction: crate::guardrail::GuardrailDirection::Inbound,
                    code: "internal".to_string(),
                    message: "A required guardrail is unavailable.".to_string(),
                    details: serde_json::to_string(
                        &serde_json::json!({ "policy_unavailable": true }),
                    )
                    .ok(),
                });
            }
        };
        match crate::guardrail::assemble_chain(&registry, &mandatory, &config.guardrails) {
            Ok(chain) => chain,
            Err(unresolved) => {
                return Err(AgentError::GuardrailDenied {
                    direction: crate::guardrail::GuardrailDirection::Inbound,
                    code: "internal".to_string(),
                    message: "A required guardrail is unavailable.".to_string(),
                    details: serde_json::to_string(
                        &serde_json::json!({ "unresolved_mandatory": unresolved }),
                    )
                    .ok(),
                });
            }
        }
    };
    let guardrail_ctx = crate::guardrail::GuardrailRunCtx {
        agent_id: agent_id.to_string(),
        session_id: session_id.to_string(),
        tenant_id: tenant.tenant_id.clone(),
        env_id: tenant.env_id.clone(),
    };

    // --- Inbound guardrail hook ---
    let user_text = match crate::guardrail::run_chain(
        &guardrail_chain,
        crate::guardrail::GuardrailDirection::Inbound,
        message.text,
        &guardrail_ctx,
        runtime.guardrail_evaluator.as_ref(),
    ) {
        crate::guardrail::ChainOutcome::Pass {
            content,
            observations,
        } => {
            for obs in &observations {
                observer.on_guardrail(obs);
            }
            content
        }
        crate::guardrail::ChainOutcome::Denied {
            info,
            direction,
            observation,
        } => {
            observer.on_guardrail(&observation);
            return Err(AgentError::GuardrailDenied {
                direction,
                code: info.code,
                message: info.message,
                details: info.details,
            });
        }
    };
    // Keep user_message for long-term memory recall query below.
    let user_message = user_text.clone();
    state
        .messages
        .push(ChatMessage::User { content: user_text });

    // Whether long-term memory is active for this turn (provider wired + the
    // agent's binding enabled). Drives recall-inject, the `recall_memory` tool,
    // and background ingest below.
    let lt_active = crate::long_term::long_term_active(runtime.long_term_memory.is_some(), &config);
    // Whether short-term ("working") memory is active for this turn (provider
    // wired + the agent's binding enabled). Drives `remember`/`recall` tools.
    let st_active =
        crate::short_term::short_term_active(runtime.short_term_memory.is_some(), &config);
    // Whether the `end_conversation` tool + system-prompt note are offered
    // this turn. Enabled when EITHER the agent's own config opts in OR this
    // invocation does (the flow node marked `conversational` — SP3). The node
    // drives the engine park-loop, so it must also let the agent end it.
    let conv_active =
        message.conversational || crate::end_conversation::conversational_active(&config);

    // --- Long-term recall: inject relevant facts into this turn's prompt ---
    let system_prompt = if lt_active {
        let facts = runtime
            .recall_long_term(
                &tenant,
                crate::long_term::RecallQuery {
                    query: user_message.clone(),
                    limit: Some(crate::long_term::AUTO_INJECT_K),
                },
            )
            .await
            .unwrap_or_default();
        crate::long_term::augment_system_prompt(&config.system_prompt, &facts)
    } else {
        config.system_prompt.clone()
    };

    // --- Knowledge retrieval: inject relevant corpus chunks (RAG-as-context,
    // D3). Distinct seam from long-term memory (`cap://dw.knowledge`); both may
    // augment the same turn, the knowledge block appended after the LT facts.
    // Retrieval failures degrade to no injection rather than failing the turn.
    let kn_active = crate::knowledge::knowledge_active(runtime.knowledge.is_some(), &config);
    let system_prompt = if kn_active {
        // The agent's own knowledge binding, threaded through so a backend that
        // delegates retrieval to a per-worker target can read its ids from the
        // binding's `params` (see `Knowledge::search_bound`).
        let binding = config.knowledge.as_ref().and_then(|k| k.knowledge.as_ref());
        let outcome = runtime
            .search_knowledge(
                &tenant,
                crate::knowledge::KnowledgeQuery {
                    query: user_message.clone(),
                    limit: Some(crate::knowledge::auto_top_k(&config)),
                },
                binding,
            )
            .await;
        // A retrieval failure still degrades to no injection — a dead backend
        // must not fail the turn — but it is no longer SILENT. This used to end
        // `.unwrap_or_default()`, which turned every `Err` into an empty vector
        // with no log, no trace, and nothing to tell "the corpus held nothing
        // relevant" apart from "the backend did not answer". The worker then
        // answered confidently from nothing, and with a third-party retrieval
        // service — which can be down, rate-limited, or holding a rotated
        // credential — that case stops being rare.
        let chunks = match outcome {
            Ok(chunks) => {
                // Surface the retrieval in the live trace (test-chat "Tools used")
                // so the operator sees which chunks were pulled in. Observer-only
                // — not the trail.
                crate::knowledge::emit_retrieval_trace(observer.as_ref(), &user_message, &chunks);
                chunks
            }
            // The normal state of every worker without a knowledge backend
            // wired, so it must not warn — `knowledge_active` can be true from
            // config alone while the host mounted nothing.
            Err(e @ crate::knowledge::KnowledgeError::NotConfigured) => {
                tracing::debug!(agent_id = %config.agent_id, error = %e, "knowledge retrieval skipped");
                Vec::new()
            }
            Err(e) => {
                tracing::warn!(
                    agent_id = %config.agent_id,
                    error = %e,
                    "knowledge retrieval failed; the turn proceeds with no retrieved context"
                );
                crate::knowledge::emit_retrieval_failure_trace(
                    observer.as_ref(),
                    &user_message,
                    &e,
                );
                Vec::new()
            }
        };
        crate::knowledge::augment_system_prompt(&system_prompt, &chunks)
    } else {
        system_prompt
    };

    // --- Conversational note: tell the model the `end_conversation` tool
    // exists and when to call it. Applied only for conversational agents. ---
    let system_prompt = if conv_active {
        crate::end_conversation::augment_system_prompt(&system_prompt)
    } else {
        system_prompt
    };

    // Resolve the per-tenant agentic-worker MCP catalog once per step. The
    // source is infallible (degrades to an empty catalog on any admin/server
    // failure) and TTL-cached, so a stable config does not re-hit the network
    // across iterations. `None` source → no MCP tools at all.
    let mcp_catalog = match runtime.mcp.as_ref() {
        Some(src) => Some(src.catalog(&tenant).await),
        None => None,
    };

    // Resolve the per-tenant component tool catalog once per step (mirrors the
    // MCP catalog above). Infallible + TTL-cached; `None` source → no
    // `component:` tools at all.
    let component_catalog = match runtime.components.as_ref() {
        Some(src) => Some(src.catalog(&tenant).await),
        None => None,
    };

    // Resolve the per-tenant flow tool catalog once per step (mirrors the
    // component catalog above). Infallible + TTL-cached; `None` source → no
    // `flow:` tools at all.
    let flow_catalog = match runtime.flows.as_ref() {
        Some(src) => Some(src.catalog(&tenant).await),
        None => None,
    };

    // Resolve the per-tenant SoRLa SoR tool catalog once per step (mirrors the
    // component/flow catalogs above). Infallible + TTL-cached; `None` source →
    // no `sorla:` tools at all.
    let sorla_catalog = match runtime.sorla.as_ref() {
        Some(src) => Some(src.catalog(&tenant).await),
        None => None,
    };

    // Preflight: surface declared tools that won't reach the LLM. Without this
    // the runtime drops unresolved tools silently (per-tool debug warns) and the
    // agent runs with a smaller — or empty — tool set, then hallucinates tool
    // results. Warn loudly, once per agent per process, with the reason + fix.
    preflight_warn_tools(
        &config.agent_id,
        &crate::tools::missing_tools(
            &runtime.ext_runtime,
            mcp_catalog.as_deref(),
            component_catalog.as_deref(),
            flow_catalog.as_deref(),
            sorla_catalog.as_deref(),
            &config.tools,
        ),
        config.tools.len(),
    );

    let mut total_tokens: u64 = 0;
    // Separate in/out accumulators surfaced on `AgentOutput.usage` (total is
    // still tracked for telemetry/metering below).
    let mut tokens_in_total: u64 = 0;
    let mut tokens_out_total: u64 = 0;
    let mut trail: Vec<AgentStep> = Vec::new();
    let mut terminated_by = TerminationReason::MaxIterations;
    let mut iterations: u32 = 0;
    let mut reply = String::new();
    // Turn-scoped: set once any tool the agent tried failed (dispatch error or
    // allow-list block). A tool failure often lands one iteration BEFORE the
    // model decides to give up and call `end_conversation`, so this flag must
    // survive across iterations of the Plan-Act-Observe loop. Consumed by the
    // `end_conversation` short-circuit below to keep a conversational agent
    // PARKED (surface the blocker) instead of silently ending the conversation.
    let mut turn_had_tool_error = false;

    for iter in 0..config.limits.max_iter {
        iterations = iter + 1;

        // Extend the lock TTL each iteration; losing the extension is
        // preferable to aborting a partially-complete turn.
        if let Err(e) = lock.refresh().await {
            warn!(error = %e, "lock refresh failed; continuing");
        }

        if started.elapsed() >= config.limits.timeout {
            terminated_by = TerminationReason::Timeout;
            break;
        }

        let mut tools_schema = list_tools_for_llm(
            &runtime.ext_runtime,
            mcp_catalog.as_deref(),
            component_catalog.as_deref(),
            flow_catalog.as_deref(),
            sorla_catalog.as_deref(),
            &config.tools,
        );
        if lt_active {
            tools_schema.push(crate::long_term::recall_memory_tool_schema());
        }
        if st_active {
            tools_schema.push(crate::short_term::remember_tool_schema());
            tools_schema.push(crate::short_term::recall_tool_schema());
        }
        if conv_active {
            tools_schema.push(crate::end_conversation::end_conversation_tool_schema());
        }
        let request = LlmRequest {
            system_prompt: system_prompt.clone(),
            history: state.messages.clone(),
            tools: tools_schema,
            provider: config.llm.clone(),
        };

        // Stream only when the observer actually consumes deltas. A
        // non-streaming caller (the default `step` path, `NoopStepObserver`)
        // stays on `complete`, preserving the exact request wire shape — no
        // `stream: true` — that every pre-streaming caller relied on.
        // On a mid-stream error the provider may have billed for partial
        // tokens, but usage only arrives in the stream's final chunk; those
        // partial tokens are NOT metered here. This matches the blocking
        // path, which likewise meters nothing when complete() errors.
        let llm_result = if observer.wants_streaming() {
            let obs = observer.clone();
            let on_delta: crate::llm::OnDelta =
                Box::new(move |chunk: &str| obs.on_token_delta(chunk));
            runtime.llm.complete_streaming(request, on_delta).await
        } else {
            runtime.llm.complete(request).await
        };
        let response = match llm_result {
            Ok(r) => r,
            Err(LlmError::ServiceUnavailable) => {
                let _ = runtime.state_store.save(&tenant, session_id, &state).await;
                return Err(AgentError::LlmProviderUnavailable);
            }
            Err(other) => {
                let _ = runtime.state_store.save(&tenant, session_id, &state).await;
                return Err(AgentError::Llm(other));
            }
        };

        let step_tokens = u64::from(response.tokens_in) + u64::from(response.tokens_out);
        total_tokens += step_tokens;
        tokens_in_total += u64::from(response.tokens_in);
        tokens_out_total += u64::from(response.tokens_out);
        // Per-call trace: record this LLM iteration's assistant content + token
        // cost (before its tool calls) so a caller can show a per-message
        // breakdown, not just the aggregate `AgentOutput.usage`.
        trail.push(AgentStep::LlmCall {
            content: response.content.clone().unwrap_or_default(),
            tokens_in: u64::from(response.tokens_in),
            tokens_out: u64::from(response.tokens_out),
        });
        // Stream the per-iteration token trace (a streaming consumer renders a
        // per-message LLM/token line even when the turn calls no tools).
        observer.on_llm_call(
            u64::from(response.tokens_in),
            u64::from(response.tokens_out),
        );
        if let Err(e) = runtime.token_meter.add(&tenant, step_tokens).await {
            warn!(error = %e, "token meter add failed; continuing");
        }
        // Fire-and-forget billing emit: never blocks the agent step.
        if let Err(e) = runtime
            .billing_meter
            .emit(
                &tenant,
                u64::from(response.tokens_in),
                u64::from(response.tokens_out),
                agent_id,
                &config.llm.model,
            )
            .await
        {
            warn!(error = %e, "billing meter emit failed; continuing");
        }

        // --- Mixed text + tool_calls: tool_calls win (spec Decision 12) ---
        if !response.tool_calls.is_empty() {
            // --- Host built-in: `end_conversation` (conversational agents) ---
            // Agent-driven exit signal (SP1). Short-circuit BEFORE recording the
            // multi-tool assistant message so saved history carries no dangling
            // tool_call. Any co-occurring tool calls are ignored — the agent
            // chose to end the conversation.
            if conv_active
                && let Some(end_call) = response
                    .tool_calls
                    .iter()
                    .find(|c| c.tool_name == crate::end_conversation::END_CONVERSATION_TOOL)
            {
                observer.on_tool_call(&end_call.tool_name, &end_call.call_id, &end_call.args);
                let closing = end_call
                    .args
                    .get("final_message")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .filter(|s| !s.is_empty())
                    .or_else(|| response.content.clone())
                    .unwrap_or_default();
                let ok = serde_json::json!({ "ok": true });
                observer.on_tool_result(&end_call.tool_name, &end_call.call_id, &ok);
                trail.push(AgentStep::ToolCall {
                    name: end_call.tool_name.clone(),
                    call_id: end_call.call_id.clone(),
                    args: end_call.args.clone(),
                    result: ok,
                    duration_ms: 0,
                });
                reply = closing;
                // Blocker guard: if a tool/backend failed earlier this turn, the
                // model often gives up and asks to end. Honouring that would
                // silently advance the flow past the failure with the user none
                // the wiser. Instead keep the conversation PARKED (FinalReply,
                // not ConversationEnded) so the closing message — which explains
                // the failure — is shown and the user can respond or retry.
                terminated_by = if turn_had_tool_error {
                    TerminationReason::FinalReply
                } else {
                    TerminationReason::ConversationEnded
                };
                break;
            }

            // Record the assistant's tool-call turn BEFORE the tool results.
            // OpenAI requires every `tool` message to follow an assistant
            // message carrying the matching `tool_calls`; without this the next
            // turn 400s ("messages with role 'tool' must be a response to a
            // preceeding message with 'tool_calls'").
            state.messages.push(ChatMessage::Assistant {
                content: response.content.clone().unwrap_or_default(),
                tool_calls: response.tool_calls.clone(),
            });
            for call in response.tool_calls {
                // --- Host built-in: `recall_memory` (long-term lookup) ---
                // Intercepted before the allow-list + WASM dispatch; routed to
                // the runtime's long-term backend instead of an extension.
                if lt_active && call.tool_name == crate::long_term::RECALL_MEMORY_TOOL {
                    observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                    let t0 = Instant::now();
                    let result = host_recall_memory(runtime, &tenant, &call).await;
                    let duration_ms = t0.elapsed().as_millis() as u64;
                    observer.on_tool_result(&call.tool_name, &call.call_id, &result);
                    state.messages.push(ChatMessage::Tool {
                        call_id: call.call_id.clone(),
                        content: result.clone(),
                    });
                    trail.push(AgentStep::ToolCall {
                        name: call.tool_name.clone(),
                        call_id: call.call_id,
                        args: call.args.clone(),
                        result,
                        duration_ms,
                    });
                    continue;
                }
                // --- Host built-in: short-term `remember` / `recall` ---
                // Intercepted before the allow-list + WASM dispatch; routed to
                // the runtime's short-term backend instead of an extension.
                if st_active && call.tool_name == crate::short_term::REMEMBER_TOOL {
                    observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                    let t0 = Instant::now();
                    let result = host_remember(runtime, &tenant, session_id, &call).await;
                    let duration_ms = t0.elapsed().as_millis() as u64;
                    observer.on_tool_result(&call.tool_name, &call.call_id, &result);
                    state.messages.push(ChatMessage::Tool {
                        call_id: call.call_id.clone(),
                        content: result.clone(),
                    });
                    trail.push(AgentStep::ToolCall {
                        name: call.tool_name.clone(),
                        call_id: call.call_id,
                        args: call.args.clone(),
                        result,
                        duration_ms,
                    });
                    continue;
                }
                if st_active && call.tool_name == crate::short_term::RECALL_TOOL {
                    observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                    let t0 = Instant::now();
                    let result = host_recall(runtime, &tenant, session_id, &call).await;
                    let duration_ms = t0.elapsed().as_millis() as u64;
                    observer.on_tool_result(&call.tool_name, &call.call_id, &result);
                    state.messages.push(ChatMessage::Tool {
                        call_id: call.call_id.clone(),
                        content: result.clone(),
                    });
                    trail.push(AgentStep::ToolCall {
                        name: call.tool_name.clone(),
                        call_id: call.call_id,
                        args: call.args.clone(),
                        result,
                        duration_ms,
                    });
                    continue;
                }
                if !is_tool_allowed(&call, &config.tools) {
                    let reason = "not in allow-list";
                    observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                    let err_obs = serde_json::json!({ "error": reason });
                    observer.on_tool_failed(&call.tool_name, &call.call_id, &err_obs);
                    state.messages.push(ChatMessage::Tool {
                        call_id: call.call_id.clone(),
                        content: serde_json::json!({ "error": "tool not allowed for this agent" }),
                    });
                    trail.push(AgentStep::ToolCallBlocked {
                        name: call.tool_name.clone(),
                        reason: reason.into(),
                    });
                    turn_had_tool_error = true;
                    continue;
                }

                // --- Idempotency: reuse a previously-recorded result ---
                match runtime.ledger.get(&tenant, session_id, &call.call_id).await {
                    Ok(Some(cached)) => {
                        observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                        observer.on_tool_result(&call.tool_name, &call.call_id, &cached);
                        state.messages.push(ChatMessage::Tool {
                            call_id: call.call_id.clone(),
                            content: cached,
                        });
                        trail.push(AgentStep::ToolCallReused {
                            name: call.tool_name.clone(),
                            call_id: call.call_id.clone(),
                        });
                        continue;
                    }
                    Ok(None) => {} // fall through to dispatch
                    Err(e) => {
                        warn!(error = %e, "ledger get failed; dispatching without idempotency");
                    }
                }

                // --- Dispatch (blocking WASM via spawn_blocking) ---
                // Tool dispatch errors are NOT termination (spec §6): surface
                // the error as a Tool observation so the LLM can react, then
                // continue. Failed calls are NOT recorded in the ledger
                // (they should remain retryable on the next turn).
                observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
                let t0 = Instant::now();
                let result = match dispatch_tool_call(
                    runtime.ext_runtime.clone(),
                    mcp_catalog.clone(),
                    component_catalog.clone(),
                    flow_catalog.clone(),
                    sorla_catalog.clone(),
                    call.clone(),
                    &tenant,
                )
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let duration_ms = t0.elapsed().as_millis() as u64;
                        warn!(
                            error = %e, tool = %call.tool_name,
                            "tool dispatch failed; recording as observation and continuing"
                        );
                        let err_obs = serde_json::json!({ "error": e.to_string() });
                        state.messages.push(ChatMessage::Tool {
                            call_id: call.call_id.clone(),
                            content: err_obs.clone(),
                        });
                        // Surface the failure so audit/stream observers see a matching
                        // outcome instead of a dangling call.
                        observer.on_tool_failed(&call.tool_name, &call.call_id, &err_obs);
                        trail.push(AgentStep::ToolCall {
                            name: call.tool_name.clone(),
                            call_id: call.call_id.clone(),
                            args: call.args.clone(),
                            result: err_obs,
                            duration_ms,
                        });
                        turn_had_tool_error = true;
                        continue;
                    }
                };
                let duration_ms = t0.elapsed().as_millis() as u64;

                observer.on_tool_result(&call.tool_name, &call.call_id, &result);

                // Record successful result in ledger (best-effort).
                if let Err(e) = runtime
                    .ledger
                    .record(&tenant, session_id, &call.call_id, result.clone())
                    .await
                {
                    warn!(error = %e, "ledger record failed; continuing");
                }

                state.messages.push(ChatMessage::Tool {
                    call_id: call.call_id.clone(),
                    content: result.clone(),
                });
                trail.push(AgentStep::ToolCall {
                    name: call.tool_name.clone(),
                    call_id: call.call_id,
                    args: call.args.clone(),
                    result,
                    duration_ms,
                });
            }
            continue; // next LLM turn with tool observations
        }

        // --- No tool calls: final reply ---
        reply = response.content.unwrap_or_default();
        // NOTE: the assistant ChatMessage push and trail push are deferred
        // until AFTER the outbound guardrail chain so history reflects the
        // guarded reply, not the raw LLM output.
        terminated_by = TerminationReason::FinalReply;
        break;
    }

    // --- Outbound guardrail hook (FinalReply only) ---
    // The outbound chain is intentionally skipped when the loop terminated via
    // Timeout or MaxIterations: in those cases `reply` is an empty string, and
    // running an outbound guardrail against an empty string could trigger a
    // policy deny that masks the true termination reason from the caller.
    // Only a real final reply (where the LLM produced content) is subject to
    // outbound policy; the assistant message and audit trail are written only
    // after this check passes, so saved state reflects the guarded reply.
    if matches!(
        terminated_by,
        TerminationReason::FinalReply | TerminationReason::ConversationEnded
    ) {
        let reply = match crate::guardrail::run_chain(
            &guardrail_chain,
            crate::guardrail::GuardrailDirection::Outbound,
            reply,
            &guardrail_ctx,
            runtime.guardrail_evaluator.as_ref(),
        ) {
            crate::guardrail::ChainOutcome::Pass {
                content,
                observations,
            } => {
                for obs in &observations {
                    observer.on_guardrail(obs);
                }
                content
            }
            crate::guardrail::ChainOutcome::Denied {
                info,
                direction,
                observation,
            } => {
                observer.on_guardrail(&observation);
                return Err(AgentError::GuardrailDenied {
                    direction,
                    code: info.code,
                    message: info.message,
                    details: info.details,
                });
            }
        };
        state.messages.push(ChatMessage::Assistant {
            content: reply.clone(),
            tool_calls: vec![],
        });
        trail.push(AgentStep::Reply {
            text: reply.clone(),
        });

        state.truncate_history(config.limits.max_history_turns);
        if let Err(e) = runtime.state_store.save(&tenant, session_id, &state).await {
            warn!(error = %e, "state save failed at end of step");
        }

        // --- Long-term ingest (fire-and-forget): persist this turn as an episode.
        // Runs on the FinalReply path — the only path with a non-empty reply. It
        // previously sat after this branch's early return and so never executed. ---
        if !reply.is_empty()
            && lt_active
            && let Some(memory) = runtime.long_term_memory.clone()
        {
            match crate::long_term::to_types_tenant(&tenant) {
                Ok(ctx) => {
                    let episode = crate::long_term::EpisodeIngest {
                        name: format!("{session_id}:turn"),
                        body: format!("{user_message}\n\n{reply}"),
                        source: crate::long_term::EpisodeSource::Message,
                        source_description: Some("agentic-worker turn".into()),
                        reference_time: chrono::Utc::now(),
                    };
                    tokio::spawn(async move {
                        if let Err(e) = memory.ingest_episode(&ctx, episode).await {
                            warn!(error = %e, "background long-term ingest failed");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "long-term ingest skipped: tenant conversion failed");
                }
            }
        }

        runtime.telemetry.record_step(&StepTelemetryCtx {
            tenant_id: tenant.tenant_id.clone(),
            env_id: tenant.env_id.clone(),
            session_id: session_id.to_string(),
            agent_id: agent_id.to_string(),
            terminated_by: terminated_by.clone(),
            iterations,
            total_tokens,
            duration: started.elapsed(),
        });

        return Ok(AgentOutput {
            reply,
            trail,
            terminated_by,
            usage: StepUsage {
                tokens_in: tokens_in_total,
                tokens_out: tokens_out_total,
                iterations,
            },
        });
    }

    state.truncate_history(config.limits.max_history_turns);
    if let Err(e) = runtime.state_store.save(&tenant, session_id, &state).await {
        warn!(error = %e, "state save failed at end of step");
    }

    runtime.telemetry.record_step(&StepTelemetryCtx {
        tenant_id: tenant.tenant_id.clone(),
        env_id: tenant.env_id.clone(),
        session_id: session_id.to_string(),
        agent_id: agent_id.to_string(),
        terminated_by: terminated_by.clone(),
        iterations,
        total_tokens,
        duration: started.elapsed(),
    });

    Ok(AgentOutput {
        reply,
        trail,
        terminated_by,
        usage: StepUsage {
            tokens_in: tokens_in_total,
            tokens_out: tokens_out_total,
            iterations,
        },
    })
}

/// Handle a host built-in `recall_memory` tool call: parse `query`/`limit` from
/// the call args, query long-term memory, and return the facts as JSON (or an
/// error object the LLM can observe and react to).
async fn host_recall_memory(
    runtime: &AgentRuntime,
    tenant: &TenantContext,
    call: &crate::state::ToolCallRecord,
) -> serde_json::Value {
    let query = call
        .args
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let limit = call
        .args
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(crate::long_term::TOOL_LIMIT);
    match runtime
        .recall_long_term(
            tenant,
            crate::long_term::RecallQuery {
                query,
                limit: Some(limit),
            },
        )
        .await
    {
        Ok(facts) => serde_json::json!({ "facts": facts }),
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    }
}

/// Handle a host built-in `remember` call: store `{key, value}` into short-term
/// memory for this `(tenant, session)`. Returns `{"ok": true}` or `{"error": ...}`.
async fn host_remember(
    runtime: &AgentRuntime,
    tenant: &TenantContext,
    session_id: &str,
    call: &crate::state::ToolCallRecord,
) -> serde_json::Value {
    let Some(provider) = runtime.short_term_memory.as_ref() else {
        return serde_json::json!({ "error": "short-term memory not configured" });
    };
    let key = call
        .args
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let value = call
        .args
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if key.is_empty() {
        return serde_json::json!({ "error": "missing 'key'" });
    }
    if value.is_empty() {
        return serde_json::json!({ "error": "missing 'value'" });
    }
    let record = crate::memory::MemoryRecord {
        key: key.to_string(),
        value: value.to_string(),
    };
    match provider.remember(tenant, session_id, record).await {
        Ok(()) => serde_json::json!({ "ok": true }),
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    }
}

/// Handle a host built-in `recall` call: read a value back by `key`. Returns
/// `{"value": <string|null>}` or `{"error": ...}`.
async fn host_recall(
    runtime: &AgentRuntime,
    tenant: &TenantContext,
    session_id: &str,
    call: &crate::state::ToolCallRecord,
) -> serde_json::Value {
    let Some(provider) = runtime.short_term_memory.as_ref() else {
        return serde_json::json!({ "error": "short-term memory not configured" });
    };
    let key = call
        .args
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if key.is_empty() {
        return serde_json::json!({ "error": "missing 'key'" });
    }
    let query = crate::memory::MemoryQuery {
        key: key.to_string(),
    };
    match provider.recall(tenant, session_id, &query).await {
        Ok(Some(record)) => serde_json::json!({ "value": record.value }),
        Ok(None) => serde_json::json!({ "value": serde_json::Value::Null }),
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    }
}

#[cfg(all(test, feature = "test-mock"))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use crate::config::{AgentConfig, AgentLimits, LlmProviderRef};
    use crate::error::{AgentError, LlmError, TerminationReason};
    use crate::llm::LlmResponse;
    use crate::mock::{MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry};
    use crate::tenant::TenantContext;
    use crate::{AgentInput, AgentRuntime};

    fn cfg() -> AgentConfig {
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![],
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: "openai".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        }
    }

    /// Happy-path loop test: one LLM call → reply, telemetry recorded.
    #[tokio::test]
    async fn happy_path_returns_llm_reply() {
        let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("hi from llm".into()),
            tool_calls: vec![],
            tokens_in: 10,
            tokens_out: 20,
        })]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        cp.insert(&tc, "a", cfg());
        let cp = Arc::new(cp);

        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(
            cp,
            store,
            ext,
            llm,
            telemetry.clone(),
            token_meter,
            ledger,
            None,
        );

        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hello".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "hi from llm");
        assert_eq!(telemetry.recorded.lock().unwrap().len(), 1);
        // Per-turn usage is surfaced on the output (one LLM call: 10 in / 20 out).
        assert_eq!(out.usage.tokens_in, 10);
        assert_eq!(out.usage.tokens_out, 20);
        assert_eq!(out.usage.iterations, 1);
        // The trail records the LLM call (per-message breakdown) before the reply.
        assert!(matches!(
            out.trail.first(),
            Some(crate::AgentStep::LlmCall {
                tokens_in: 10,
                tokens_out: 20,
                ..
            })
        ));
    }

    /// An empty wallet must stop the run BEFORE any LLM spend. The mock backend is
    /// primed with zero responses: if the gate were removed, the loop would reach
    /// the LLM and fail with an LLM error instead of `CreditBudgetExceeded`.
    #[tokio::test]
    async fn empty_wallet_refuses_the_run_before_any_llm_call() {
        let llm = Arc::new(MockLlmBackend::new(vec![]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        cp.insert(&tc, "a", cfg());
        let cp = Arc::new(cp);

        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(
            cp,
            store,
            ext,
            llm,
            telemetry.clone(),
            token_meter,
            ledger,
            None,
        )
        .with_billing_meter(Arc::new(crate::mock::MockBillingMeter::new(true)));

        let err = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hello".into(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("an empty wallet must refuse the run");

        assert!(
            matches!(err, AgentError::CreditBudgetExceeded),
            "expected CreditBudgetExceeded, got {err:?}"
        );
        assert!(
            telemetry.recorded.lock().unwrap().is_empty(),
            "nothing should have been executed"
        );
    }

    #[tokio::test]
    async fn opening_message_short_circuits_first_empty_turn() {
        // The mock LLM would reply "from llm" if called — but an opening message
        // on an empty FIRST turn must be returned verbatim WITHOUT an LLM call.
        let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("from llm".into()),
            tool_calls: vec![],
            tokens_in: 5,
            tokens_out: 5,
        })]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        let mut c = cfg();
        c.opening_message = Some("Welcome to support!".into());
        cp.insert(&tc, "a", c);
        let cp = Arc::new(cp);
        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None);
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                // Blank input (whitespace only) — e.g. a button click, no message.
                AgentInput {
                    text: "  ".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "Welcome to support!");
        // No LLM call → zero usage, empty trail.
        assert_eq!(out.usage.tokens_in, 0);
        assert_eq!(out.usage.tokens_out, 0);
        assert!(out.trail.is_empty());
    }

    #[tokio::test]
    async fn opening_message_ignored_when_user_typed() {
        // With actual user text, the opening message is NOT used — the LLM runs.
        let (runtime, tc) = {
            let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
                content: Some("real answer".into()),
                tool_calls: vec![],
                tokens_in: 3,
                tokens_out: 4,
            })]));
            let store = Arc::new(MockAgentStateStore::new());
            let telemetry = Arc::new(MockTelemetry::new());
            let cp = MockConfigProvider::new();
            let tc = TenantContext::new("acme", "prod");
            let mut c = cfg();
            c.opening_message = Some("Welcome!".into());
            cp.insert(&tc, "a", c);
            let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
            let runtime = AgentRuntime::new(
                Arc::new(cp),
                store,
                ext,
                llm,
                telemetry,
                Arc::new(crate::cost::MockTokenMeter::new(0)),
                Arc::new(crate::mock::NoopToolLedger),
                None,
            );
            (runtime, tc)
        };
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "track my order".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "real answer");
    }

    /// Build a runtime whose mock LLM replays `responses` in order, for a
    /// conversational agent with the given allow-listed `tools`.
    fn conversational_runtime(
        responses: Vec<Result<LlmResponse, LlmError>>,
        tools: Vec<crate::config::ToolRef>,
    ) -> (AgentRuntime, TenantContext) {
        runtime_with_config_conversational(responses, tools, true)
    }

    /// Like [`conversational_runtime`] but the agent CONFIG's `conversational`
    /// is caller-controlled, so tests can exercise the node/invocation flag
    /// (`AgentInput.conversational`) independently of the config default.
    fn runtime_with_config_conversational(
        responses: Vec<Result<LlmResponse, LlmError>>,
        tools: Vec<crate::config::ToolRef>,
        config_conversational: bool,
    ) -> (AgentRuntime, TenantContext) {
        let llm = Arc::new(MockLlmBackend::new(responses));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        let mut c = cfg();
        c.conversational = config_conversational;
        c.tools = tools;
        cp.insert(&tc, "a", c);
        let cp = Arc::new(cp);
        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None);
        (runtime, tc)
    }

    fn end_conversation_call(final_message: &str) -> crate::state::ToolCallRecord {
        crate::state::ToolCallRecord {
            call_id: "end-call".into(),
            extension_id: "host".into(),
            tool_name: crate::end_conversation::END_CONVERSATION_TOOL.into(),
            args: serde_json::json!({ "final_message": final_message }),
        }
    }

    /// Blocker guard: when a tool fails during the turn, the model's subsequent
    /// `end_conversation` request must NOT end the conversation — it parks
    /// (`FinalReply`) so the closing message (which explains the failure) is
    /// shown and the flow does not silently advance past the blocker.
    #[tokio::test]
    async fn conversational_tool_error_then_end_conversation_stays_parked() {
        // Iteration 1: the model calls a tool that is not in the allow-list →
        // recorded as a tool failure. Iteration 2: it gives up and asks to end.
        let responses = vec![
            Ok(LlmResponse {
                content: Some("let me look that up".into()),
                tool_calls: vec![crate::state::ToolCallRecord {
                    call_id: "c1".into(),
                    extension_id: "greentic.test".into(),
                    tool_name: "lookup".into(),
                    args: serde_json::json!({}),
                }],
                tokens_in: 1,
                tokens_out: 1,
            }),
            Ok(LlmResponse {
                content: Some("reasoning".into()),
                tool_calls: vec![end_conversation_call("Sorry, my systems are unavailable.")],
                tokens_in: 1,
                tokens_out: 1,
            }),
        ];
        // Empty allow-list → the `lookup` call is blocked (a tool failure).
        let (runtime, tc) = conversational_runtime(responses, vec![]);
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hi".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.terminated_by, TerminationReason::FinalReply);
        assert_eq!(out.reply, "Sorry, my systems are unavailable.");
    }

    /// Control: with no tool failure in the turn, `end_conversation` ends the
    /// conversation normally (`ConversationEnded`) so the flow routes onward.
    #[tokio::test]
    async fn conversational_clean_end_conversation_ends() {
        let responses = vec![Ok(LlmResponse {
            content: Some("reasoning".into()),
            tool_calls: vec![end_conversation_call("All set — goodbye!")],
            tokens_in: 1,
            tokens_out: 1,
        })];
        let (runtime, tc) = conversational_runtime(responses, vec![]);
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hi".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
        assert_eq!(out.reply, "All set — goodbye!");
    }

    /// SP3: the flow node's `conversational` flag (carried on `AgentInput`) must
    /// enable the host `end_conversation` tool EVEN WHEN the agent's own config
    /// default is non-conversational — so a node-marked-conversational segment
    /// can be ended by the agent (and the engine advances past the park-loop).
    #[tokio::test]
    async fn node_conversational_input_enables_end_conversation_over_config() {
        let responses = vec![Ok(LlmResponse {
            content: Some("bye".into()),
            tool_calls: vec![end_conversation_call("Take care!")],
            tokens_in: 1,
            tokens_out: 1,
        })];
        // Agent CONFIG is NON-conversational; only the invocation opts in.
        let (runtime, tc) = runtime_with_config_conversational(responses, vec![], false);
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hi".into(),
                    conversational: true,
                },
            )
            .await
            .unwrap();
        // end_conversation was honoured because the invocation flag turned
        // conv_active on despite the config default.
        assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
        assert_eq!(out.reply, "Take care!");
    }

    /// Control: a non-conversational config AND a non-conversational invocation
    /// means `end_conversation` is NOT offered, so the model's call is treated as
    /// an ordinary (disallowed) tool and the turn ends as a normal FinalReply.
    #[tokio::test]
    async fn non_conversational_ignores_end_conversation() {
        let responses = vec![
            Ok(LlmResponse {
                content: Some("trying to end".into()),
                tool_calls: vec![end_conversation_call("bye")],
                tokens_in: 1,
                tokens_out: 1,
            }),
            Ok(LlmResponse {
                content: Some("final answer".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            }),
        ];
        let (runtime, tc) = runtime_with_config_conversational(responses, vec![], false);
        let out = runtime
            .step(
                tc.clone(),
                "sess-1",
                "a",
                AgentInput {
                    text: "hi".into(),
                    conversational: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    }

    /// 4b: when a knowledge backend is wired and the agent's binding is enabled,
    /// retrieved chunks are injected into the system prompt the LLM sees.
    #[tokio::test]
    async fn knowledge_chunks_inject_into_system_prompt() {
        let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("ok".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        })]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");

        // Config with an enabled knowledge binding (top_k = 3).
        let mut c = cfg();
        c.knowledge = Some(crate::config::KnowledgeSettings {
            knowledge: Some(crate::config::MemoryProviderRef {
                provider: "provider.knowledge.chronicle".into(),
                capability: "cap://dw.knowledge".into(),
                params: serde_json::Map::new(),
                credential_ref: None,
            }),
            embedding: None,
            top_k: 3,
        });
        cp.insert(&tc, "a", c);
        let cp = Arc::new(cp);

        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let kb = Arc::new(crate::mock::MockKnowledge::new(vec![
            crate::knowledge::RetrievedChunk {
                text: "Refunds are processed within 5 business days.".into(),
                score: 0.9,
                doc_id: None,
                chunk_index: None,
                metadata: serde_json::Map::new(),
            },
        ]));
        let runtime = AgentRuntime::new(
            cp,
            store,
            ext,
            llm.clone(),
            telemetry,
            token_meter,
            ledger,
            None,
        )
        .with_knowledge(kb);

        runtime
            .step(
                tc,
                "sess-k",
                "a",
                AgentInput {
                    text: "do I get refunds?".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let prompts = llm.seen_system_prompts.lock().unwrap();
        assert_eq!(prompts.len(), 1);
        assert!(
            prompts[0].contains("<knowledge>"),
            "knowledge block missing from system prompt: {}",
            prompts[0]
        );
        assert!(
            prompts[0].contains("Refunds are processed within 5 business days."),
            "retrieved chunk missing from system prompt: {}",
            prompts[0]
        );
    }

    #[derive(Default)]
    struct Collecting {
        deltas: std::sync::Mutex<Vec<String>>,
        tool_calls: std::sync::Mutex<Vec<String>>,
    }
    impl crate::StepObserver for Collecting {
        fn wants_streaming(&self) -> bool {
            true
        }
        fn on_token_delta(&self, chunk: &str) {
            self.deltas.lock().expect("lock").push(chunk.to_string());
        }
        fn on_tool_call(&self, name: &str, _call_id: &str, _args: &serde_json::Value) {
            self.tool_calls.lock().expect("lock").push(name.to_string());
        }
    }

    /// `step_with_observer` mirrors `happy_path_returns_llm_reply` but
    /// drives a collecting observer; the default-impl single-delta path
    /// (MockLlmBackend uses the trait default) must forward the full reply
    /// to `on_token_delta`.
    #[tokio::test]
    async fn step_with_observer_streams_reply_deltas() {
        let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("hi from llm".into()),
            tool_calls: vec![],
            tokens_in: 10,
            tokens_out: 20,
        })]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        cp.insert(&tc, "a", cfg());
        let cp = Arc::new(cp);

        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(
            cp,
            store,
            ext,
            llm,
            telemetry.clone(),
            token_meter,
            ledger,
            None,
        );

        let obs = Arc::new(Collecting::default());
        let out = runtime
            .step_with_observer(
                tc.clone(),
                "sess-2",
                "a",
                AgentInput {
                    text: "hello".into(),
                    ..Default::default()
                },
                obs.clone(),
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "hi from llm");
        assert_eq!(*obs.deltas.lock().unwrap(), vec!["hi from llm".to_string()]);
    }

    /// A non-streaming observer (default `wants_streaming() == false`) must
    /// keep `step` on the `complete` path: no token deltas are produced, and
    /// the reply is still returned. This is the regression guard for callers
    /// (and mocks) that speak the non-streaming OpenAI wire shape — turning
    /// streaming on unconditionally would send `stream: true` and break them.
    #[tokio::test]
    async fn non_streaming_observer_emits_no_deltas() {
        #[derive(Default)]
        struct CountOnly {
            deltas: std::sync::Mutex<u32>,
        }
        impl crate::StepObserver for CountOnly {
            fn on_token_delta(&self, _chunk: &str) {
                *self.deltas.lock().expect("lock") += 1;
            }
        }

        let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("hi from llm".into()),
            tool_calls: vec![],
            tokens_in: 10,
            tokens_out: 20,
        })]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        cp.insert(&tc, "a", cfg());
        let cp = Arc::new(cp);
        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None);

        let obs = Arc::new(CountOnly::default());
        let out = runtime
            .step_with_observer(
                tc.clone(),
                "sess-3",
                "a",
                AgentInput {
                    text: "hello".into(),
                    ..Default::default()
                },
                obs.clone(),
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "hi from llm");
        assert_eq!(
            *obs.deltas.lock().unwrap(),
            0,
            "no deltas on the non-streaming path"
        );
    }

    /// A tool call for a tool NOT in the agent's allow-list must still fire
    /// `on_tool_call` (so a chip renders with real args) followed by
    /// `on_tool_failed` (so it renders as failed/blocked) — NOT
    /// `on_tool_result`, which would make a blocked tool look like it
    /// succeeded.
    #[tokio::test]
    async fn blocked_tool_fires_on_tool_call_then_on_tool_failed() {
        use crate::state::ToolCallRecord;

        #[derive(Default)]
        struct ToolEvents {
            calls: std::sync::Mutex<Vec<(String, String, serde_json::Value)>>,
            results: std::sync::Mutex<Vec<(String, String, serde_json::Value)>>,
            failures: std::sync::Mutex<Vec<(String, String, serde_json::Value)>>,
        }
        impl crate::StepObserver for ToolEvents {
            fn on_tool_call(&self, name: &str, call_id: &str, args: &serde_json::Value) {
                self.calls.lock().unwrap().push((
                    name.to_string(),
                    call_id.to_string(),
                    args.clone(),
                ));
            }
            fn on_tool_result(&self, name: &str, call_id: &str, result: &serde_json::Value) {
                self.results.lock().unwrap().push((
                    name.to_string(),
                    call_id.to_string(),
                    result.clone(),
                ));
            }
            fn on_tool_failed(&self, name: &str, call_id: &str, error: &serde_json::Value) {
                self.failures.lock().unwrap().push((
                    name.to_string(),
                    call_id.to_string(),
                    error.clone(),
                ));
            }
        }

        let llm = Arc::new(MockLlmBackend::new(vec![
            Ok(LlmResponse {
                content: None,
                tool_calls: vec![ToolCallRecord {
                    call_id: "call_1".into(),
                    extension_id: "http".into(),
                    tool_name: "fetch".into(),
                    args: serde_json::json!({"url": "https://example.com"}),
                }],
                tokens_in: 10,
                tokens_out: 5,
            }),
            Ok(LlmResponse {
                content: Some("done".into()),
                tool_calls: vec![],
                tokens_in: 3,
                tokens_out: 2,
            }),
        ]));
        let store = Arc::new(MockAgentStateStore::new());
        let telemetry = Arc::new(MockTelemetry::new());
        let cp = MockConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        // `cfg()` has an empty tools allow-list, so the tool call is blocked.
        cp.insert(&tc, "a", cfg());
        let cp = Arc::new(cp);
        let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
        let token_meter = Arc::new(crate::cost::MockTokenMeter::new(0));
        let ledger = Arc::new(crate::mock::NoopToolLedger);
        let runtime = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None);

        let obs = Arc::new(ToolEvents::default());
        let out = runtime
            .step_with_observer(
                tc.clone(),
                "sess-4",
                "a",
                AgentInput {
                    text: "please fetch".into(),
                    ..Default::default()
                },
                obs.clone(),
            )
            .await
            .unwrap();
        assert_eq!(out.reply, "done");

        let calls = obs.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "fetch");
        assert_eq!(calls[0].1, "call_1");
        assert_eq!(
            calls[0].2,
            serde_json::json!({"url": "https://example.com"})
        );
        drop(calls);

        assert!(
            obs.results.lock().unwrap().is_empty(),
            "a blocked tool must not fire on_tool_result"
        );
        let failures = obs.failures.lock().unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, "fetch");
        assert_eq!(failures[0].1, "call_1");
        assert_eq!(
            failures[0].2,
            serde_json::json!({"error": "not in allow-list"})
        );
    }

    // --- StepObserver::on_guardrail wiring ------------------------------
    //
    // A genuine `run_step` integration test would need a `ResolvedGuardrail`
    // chain entry whose capability actually resolves, which in turn needs a
    // populated `CapabilityRegistry` on the `ExtensionRuntime`. There is no
    // public seam to inject offerings directly: `ExtensionRuntime::for_test()`
    // builds an empty registry, and the only way to populate one is
    // `register_loaded_from_dir`, which requires a real, signed WASM
    // extension on disk (see `tests/guardrail_e2e.rs`, which does exactly
    // that for the full e2e suite — real component-guardrail-pii binary,
    // ephemeral Ed25519 signing key, tempdir). That is too heavy for a
    // focused unit test of the observer seam, so per the plan's authorised
    // fallback these tests exercise `run_chain` directly and apply the exact
    // notify pattern the two `run_step` call sites use (mirrored above),
    // without building a full `AgentRuntime`. This still proves the
    // regression this task guards against: a BLOCKED denial (Enforce) must
    // reach the observer, not just a Monitored one.
    #[derive(Default)]
    struct RecordingObserver {
        guardrails: std::sync::Mutex<Vec<crate::guardrail::GuardrailObservation>>,
    }
    impl crate::StepObserver for RecordingObserver {
        fn on_guardrail(&self, obs: &crate::guardrail::GuardrailObservation) {
            self.guardrails.lock().unwrap().push(obs.clone());
        }
    }

    struct DenyingEvaluator {
        code: String,
        message: String,
    }
    impl crate::guardrail::GuardrailEvaluator for DenyingEvaluator {
        fn evaluate(
            &self,
            _extension_id: &str,
            _input: &crate::guardrail::GuardrailInput,
        ) -> Result<crate::guardrail::GuardrailVerdict, crate::guardrail::GuardrailInvokeError>
        {
            Ok(crate::guardrail::GuardrailVerdict::Deny(
                crate::guardrail::GuardrailDenyInfo {
                    code: self.code.clone(),
                    message: self.message.clone(),
                    details: None,
                },
            ))
        }
    }

    fn guardrail_run_ctx() -> crate::guardrail::GuardrailRunCtx {
        crate::guardrail::GuardrailRunCtx {
            agent_id: "a".into(),
            session_id: "s1".into(),
            tenant_id: "acme".into(),
            env_id: "prod".into(),
        }
    }

    /// Mirrors the notify logic at the loop.rs guardrail call sites: forward
    /// every `Pass` observation, or the `Denied` observation, to the observer.
    fn run_chain_and_notify(
        chain: &[crate::guardrail::ResolvedGuardrail],
        direction: crate::guardrail::GuardrailDirection,
        content: String,
        observer: &dyn crate::StepObserver,
        evaluator: &dyn crate::guardrail::GuardrailEvaluator,
    ) -> Result<String, crate::error::AgentError> {
        match crate::guardrail::run_chain(
            chain,
            direction,
            content,
            &guardrail_run_ctx(),
            evaluator,
        ) {
            crate::guardrail::ChainOutcome::Pass {
                content,
                observations,
            } => {
                for obs in &observations {
                    observer.on_guardrail(obs);
                }
                Ok(content)
            }
            crate::guardrail::ChainOutcome::Denied {
                info,
                direction,
                observation,
            } => {
                observer.on_guardrail(&observation);
                Err(AgentError::GuardrailDenied {
                    direction,
                    code: info.code,
                    message: info.message,
                    details: info.details,
                })
            }
        }
    }

    // NOTE: these two tests pin the notify *pattern* mirrored above (call
    // `on_guardrail` for every `Pass` observation, or for a `Denied`
    // observation) — they do NOT exercise `run_step`'s real wiring of that
    // pattern (see the module doc above for why a genuine `run_step`
    // integration test isn't feasible as a focused unit test here). The real
    // `run_step` → `observer.on_guardrail` wiring is covered by
    // `crates/greentic-aw-runtime/tests/guardrail_e2e.rs`, and the
    // `CompositeObserver` fan-out regression (dropping `on_guardrail` when
    // fanning out to multiple observers) is covered by
    // `crates/greentic-runner-host/src/http/agent_stream.rs`'s
    // `composite_fans_out_to_all_members_and_ors_streaming` test.
    #[test]
    fn guardrail_notify_pattern_reports_a_monitored_denial() {
        // A recording observer proves the observation escapes run_chain and
        // reaches the seam the host emits from.
        let observer = RecordingObserver::default();
        let chain = vec![crate::guardrail::ResolvedGuardrail {
            extension_id: "ext-pii".into(),
            cap_id: "greentic:guardrail/pii".into(),
            mandatory: false,
            mode: crate::config::GuardrailMode::Monitor,
            config: serde_json::Value::Null,
        }];
        let evaluator = DenyingEvaluator {
            code: "pii".into(),
            message: "blocked pii".into(),
        };
        let out = run_chain_and_notify(
            &chain,
            crate::guardrail::GuardrailDirection::Inbound,
            "hi".into(),
            &observer,
            &evaluator,
        );
        assert!(out.is_ok(), "monitor mode must not fail the turn");
        let seen = observer.guardrails.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].action, crate::guardrail::GuardrailAction::Monitored);
    }

    #[test]
    fn guardrail_notify_pattern_reports_a_blocked_denial() {
        let observer = RecordingObserver::default();
        let chain = vec![crate::guardrail::ResolvedGuardrail {
            extension_id: "ext-pii".into(),
            cap_id: "greentic:guardrail/pii".into(),
            mandatory: false,
            mode: crate::config::GuardrailMode::Enforce,
            config: serde_json::Value::Null,
        }];
        let evaluator = DenyingEvaluator {
            code: "pii".into(),
            message: "blocked pii".into(),
        };
        let out = run_chain_and_notify(
            &chain,
            crate::guardrail::GuardrailDirection::Inbound,
            "hi".into(),
            &observer,
            &evaluator,
        );
        assert!(matches!(out, Err(AgentError::GuardrailDenied { .. })));
        let seen = observer.guardrails.lock().unwrap();
        assert_eq!(seen.len(), 1, "a blocked denial must still be observed");
        assert_eq!(seen[0].action, crate::guardrail::GuardrailAction::Blocked);
    }
}
