//! OpenAI Chat Completions API client (function-calling mode).
//! Single backend MVP; multi-provider routing deferred (spec §10).

use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::LlmError;
use crate::llm::{LlmBackend, LlmRequest, LlmResponse, LlmToolSchema, OnDelta};
use crate::state::{ChatMessage, ToolCallRecord};
use crate::tool_wire_name::{ToolNameCodec, wire_tool_name};

/// Maximum time to wait between consecutive bytes from the streaming
/// response before treating the connection as dead. reqwest's client
/// `.timeout()` bounds the *whole* request, not the gap between chunks,
/// so a server that opens the stream and then stalls would otherwise hang
/// indefinitely. Overridable only in tests for fast, deterministic checks.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Endpoint used when [`ENV_BASE_URL`] names none.
const DEFAULT_BASE_URL: &str = "https://api.openai.com";

/// Names an OpenAI-compatible endpoint (Ollama, vLLM, LiteLLM, a gateway, …).
///
/// This is the ORIGIN, not the API root: the request paths below append
/// `/v1/chat/completions` themselves, so the value is `http://127.0.0.1:11434`
/// and not `http://127.0.0.1:11434/v1`.
///
/// Already read by the `greentic-llm-backend` path in greentic-runner-host;
/// honouring it here too is what lets a STOCK default build reach a non-OpenAI
/// endpoint with no feature flag.
const ENV_BASE_URL: &str = "GREENTIC_LLM_BASE_URL";

pub struct OpenAiLlmBackend {
    api_key: String,
    base_url: String,
    client: Client,
}

impl OpenAiLlmBackend {
    /// Build a backend against the endpoint named by [`ENV_BASE_URL`], falling
    /// back to OpenAI's own host when that variable is unset or blank.
    ///
    /// Reading the env HERE rather than only at the call sites is deliberate.
    /// This type is what `in_process_llm_backend_with_key` constructs on the
    /// default build, where the `greentic-llm-backend` block that reads the same
    /// variable is not compiled in — so until this change an Ollama or other
    /// OpenAI-compatible endpoint was unreachable from a stock binary no matter
    /// how it was configured, and every dw.agent call went to api.openai.com.
    /// The two paths now agree on one variable.
    ///
    /// [`with_base_url`](Self::with_base_url) still takes an explicit endpoint
    /// and ignores the env, for callers that must pin one (tests included).
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, base_url_from_env())
    }

    /// Build a backend against an explicitly supplied endpoint, ignoring
    /// [`ENV_BASE_URL`].
    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: Client::builder()
                .timeout(Duration::from_secs(45))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }
}

/// Resolve the endpoint from [`ENV_BASE_URL`], falling back to [`DEFAULT_BASE_URL`].
fn base_url_from_env() -> String {
    normalize_base_url(std::env::var(ENV_BASE_URL).ok())
}

/// The pure half of [`base_url_from_env`], split out so it is testable without
/// mutating process-global env (which races every other test in the binary).
///
/// An unset, blank or whitespace-only value means "the operator configured
/// nothing", not "use the empty string" — both fall back to OpenAI's host. A
/// trailing slash is trimmed because the request paths append `/v1/...`, and
/// `https://host//v1/chat/completions` is a 404 on several gateways.
fn normalize_base_url(configured: Option<String>) -> String {
    configured
        .map(|url| url.trim().trim_end_matches('/').to_string())
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string())
}

#[derive(Serialize)]
struct OaRequest<'a> {
    model: &'a str,
    messages: Vec<OaMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OaTool<'a>>>,
    tool_choice: &'static str,
    /// `Some(true)` on the streaming path; `None` (omitted) on the
    /// blocking path so the request body is byte-identical to before.
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
    /// `Some({"include_usage": true})` on the streaming path so the final
    /// chunk carries token usage; `None` (omitted) on the blocking path.
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<serde_json::Value>,
}

#[derive(Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
enum OaMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        content: Option<String>,
        // OpenAI rejects `tool_calls: []` (400, code `empty_array`) on a
        // multi-turn request — omit the field entirely for a text-only
        // assistant turn (it's only valid with >=1 call).
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<OaToolCallEmit>,
    },
    Tool {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Serialize)]
struct OaToolCallEmit {
    id: String,
    #[serde(rename = "type")]
    typ: &'static str,
    function: OaToolFn,
}

#[derive(Serialize)]
struct OaToolFn {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct OaTool<'a> {
    #[serde(rename = "type")]
    typ: &'static str,
    function: OaToolDef<'a>,
}

#[derive(Serialize)]
struct OaToolDef<'a> {
    name: String,
    description: &'a str,
    parameters: &'a serde_json::Value,
}

#[derive(Deserialize)]
struct OaResponse {
    choices: Vec<OaChoice>,
    usage: OaUsage,
}

#[derive(Deserialize)]
struct OaChoice {
    message: OaMessageIn,
}

#[derive(Deserialize)]
struct OaMessageIn {
    content: Option<String>,
    tool_calls: Option<Vec<OaToolCallIn>>,
}

#[derive(Deserialize)]
struct OaToolCallIn {
    id: String,
    function: OaToolFnIn,
}

#[derive(Deserialize)]
struct OaToolFnIn {
    name: String,
    arguments: String, // JSON-encoded string per OpenAI spec
}

#[derive(Deserialize)]
struct OaUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
}

impl LlmBackend for OpenAiLlmBackend {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let messages = build_messages(&req);
            // Built from the list the model is about to be shown: a sanitised
            // wire name cannot be split back apart by string surgery.
            let codec = ToolNameCodec::for_tools(&req.tools);
            let tools: Option<Vec<OaTool<'_>>> = if req.tools.is_empty() {
                None
            } else {
                Some(req.tools.iter().map(build_tool).collect())
            };
            let body = OaRequest {
                model: &req.provider.model,
                messages,
                tools,
                tool_choice: "auto",
                stream: None,
                stream_options: None,
            };
            let url = format!("{}/v1/chat/completions", self.base_url);
            let resp = self
                .client
                .post(&url)
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| LlmError::Transport(e.to_string()))?;
            let status = resp.status();
            if status.is_server_error() {
                return Err(LlmError::ServiceUnavailable);
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(LlmError::BadRequest(format!("{status}: {text}")));
            }
            let oa: OaResponse = resp
                .json()
                .await
                .map_err(|e| LlmError::Decode(e.to_string()))?;
            let choice = oa
                .choices
                .into_iter()
                .next()
                .ok_or_else(|| LlmError::Decode("no choices".into()))?;
            let tool_calls = choice
                .message
                .tool_calls
                .unwrap_or_default()
                .into_iter()
                .map(|c| {
                    build_tool_call_record(&codec, c.id, &c.function.name, &c.function.arguments)
                })
                .collect();
            Ok(LlmResponse {
                content: choice.message.content,
                tool_calls,
                tokens_in: oa.usage.prompt_tokens,
                tokens_out: oa.usage.completion_tokens,
            })
        })
    }

    /// Streaming completion over the OpenAI SSE protocol.
    ///
    /// The response body is split into lines at the byte level (so a
    /// multi-byte UTF-8 scalar straddling a network-chunk boundary is never
    /// corrupted to `U+FFFD`) and each complete line is fed to
    /// [`StreamAccumulator::push_line`]. Each `stream.next()` await is bounded
    /// by a 60s idle timeout ([`STREAM_IDLE_TIMEOUT`]): reqwest's client
    /// `.timeout()` bounds the whole request, not the gap between chunks, so a
    /// stalled-but-open stream would otherwise hang. On idle elapse this
    /// returns [`LlmError::Transport`] `"stream idle timeout"`.
    fn complete_streaming<'a>(
        &'a self,
        req: LlmRequest,
        on_delta: OnDelta,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let messages = build_messages(&req);
            // Built from the list the model is about to be shown: a sanitised
            // wire name cannot be split back apart by string surgery.
            let codec = ToolNameCodec::for_tools(&req.tools);
            let tools: Option<Vec<OaTool<'_>>> = if req.tools.is_empty() {
                None
            } else {
                Some(req.tools.iter().map(build_tool).collect())
            };
            let body = OaRequest {
                model: &req.provider.model,
                messages,
                tools,
                tool_choice: "auto",
                stream: Some(true),
                stream_options: Some(serde_json::json!({ "include_usage": true })),
            };
            let url = format!("{}/v1/chat/completions", self.base_url);
            let resp = self
                .client
                .post(&url)
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| LlmError::Transport(e.to_string()))?;
            let status = resp.status();
            // A non-2xx response is an error body, not an SSE stream — read
            // it whole and map exactly like the blocking path.
            if status.is_server_error() {
                return Err(LlmError::ServiceUnavailable);
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(LlmError::BadRequest(format!("{status}: {text}")));
            }

            let mut acc = StreamAccumulator::new();
            let mut line_buf = SseLineBuffer::new();
            let mut stream = resp.bytes_stream();
            // Bound the gap between chunks: reqwest's request timeout does not
            // cover an open-but-stalled stream.
            while let Some(chunk) = tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.next())
                .await
                .map_err(|_| LlmError::Transport("stream idle timeout".into()))?
            {
                let bytes = chunk.map_err(|e| LlmError::Transport(e.to_string()))?;
                for line in line_buf.push_bytes(&bytes) {
                    if let Some(text) = acc.push_line(&line)? {
                        on_delta(&text);
                    }
                }
            }
            // Flush any trailing line without a terminating newline so a
            // missing final newline does not drop the last event.
            if let Some(line) = line_buf.take_remainder()
                && let Some(text) = acc.push_line(&line)?
            {
                on_delta(&text);
            }
            Ok(acc.finish(&codec))
        })
    }
}

/// Byte-level line splitter for an SSE response body.
///
/// SSE chunks arrive as arbitrary byte slices: a single multi-byte UTF-8
/// scalar (e.g. `é`, an emoji) can be split across two network chunks.
/// Decoding each raw chunk independently with `from_utf8_lossy` would turn
/// the split bytes into `U+FFFD`, corrupting the emitted delta. This buffer
/// holds raw bytes and only decodes **complete** lines (terminated by `\n`),
/// so a scalar straddling a chunk boundary is reassembled before decoding.
struct SseLineBuffer {
    buf: Vec<u8>,
}

impl SseLineBuffer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append `bytes` and return every newly-completed line, with trailing
    /// `\r`/`\n` stripped. Bytes after the last `\n` are retained for the
    /// next call. Decoding happens only on whole lines, so a multi-byte
    /// scalar split across calls is never turned into `U+FFFD`.
    fn push_bytes(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(bytes);
        let mut lines = Vec::new();
        while let Some(idx) = self.buf.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = self.buf.drain(..=idx).collect();
            let line = String::from_utf8_lossy(&line_bytes);
            lines.push(line.trim_end_matches(['\r', '\n']).to_string());
        }
        lines
    }

    /// Decode any remaining bytes that were never newline-terminated as a
    /// final line. Returns `None` when the buffer is empty. Call once after
    /// the stream ends so a missing trailing newline does not drop the last
    /// event.
    fn take_remainder(&mut self) -> Option<String> {
        if self.buf.is_empty() {
            return None;
        }
        let line = String::from_utf8_lossy(&self.buf);
        let line = line.trim_end_matches(['\r', '\n']).to_string();
        self.buf.clear();
        Some(line)
    }
}

/// Pure accumulator for OpenAI streaming (SSE) chat-completion chunks.
///
/// Fed one `data: ...` line at a time via [`StreamAccumulator::push_line`],
/// it concatenates content deltas, reassembles indexed tool-call fragments
/// (`id`/`name` arrive once; `arguments` concatenate across chunks), and
/// captures token usage from the final chunk. [`StreamAccumulator::finish`]
/// projects the accumulated state into an [`LlmResponse`], reusing the same
/// `split_tool_name` + `unwrap_or(json!({}))` argument parsing as the
/// blocking path.
struct StreamAccumulator {
    content: String,
    /// Tool-call fragments keyed by their stream `index` (stable ordering).
    tool_calls: BTreeMap<u32, ToolCallFrag>,
    tokens_in: u32,
    tokens_out: u32,
}

#[derive(Default)]
struct ToolCallFrag {
    id: String,
    name: String,
    arguments: String,
}

impl StreamAccumulator {
    fn new() -> Self {
        Self {
            content: String::new(),
            tool_calls: BTreeMap::new(),
            tokens_in: 0,
            tokens_out: 0,
        }
    }

    /// Parse one streaming line. Returns `Ok(Some(text))` when the line
    /// carried a content delta to emit, `Ok(None)` for non-content lines
    /// (tool-call fragments, usage, keep-alives, `[DONE]`, blank lines).
    fn push_line(&mut self, line: &str) -> Result<Option<String>, LlmError> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }
        let payload = match line.strip_prefix("data:") {
            Some(rest) => rest.trim(),
            None => return Ok(None), // comments / keep-alives
        };
        if payload == "[DONE]" {
            return Ok(None);
        }
        let chunk: OaStreamChunk =
            serde_json::from_str(payload).map_err(|e| LlmError::Decode(e.to_string()))?;
        if let Some(usage) = chunk.usage {
            self.tokens_in = usage.prompt_tokens;
            self.tokens_out = usage.completion_tokens;
        }
        let mut emit: Option<String> = None;
        for choice in chunk.choices {
            let delta = choice.delta;
            if let Some(text) = delta.content
                && !text.is_empty()
            {
                self.content.push_str(&text);
                emit = Some(text);
            }
            for tc in delta.tool_calls.unwrap_or_default() {
                let frag = self.tool_calls.entry(tc.index).or_default();
                if let Some(id) = tc.id {
                    frag.id = id;
                }
                if let Some(func) = tc.function {
                    if let Some(name) = func.name {
                        frag.name = name;
                    }
                    if let Some(args) = func.arguments {
                        frag.arguments.push_str(&args);
                    }
                }
            }
        }
        Ok(emit)
    }

    fn finish(self, codec: &ToolNameCodec) -> LlmResponse {
        let tool_calls = self
            .tool_calls
            .into_values()
            .map(|frag| build_tool_call_record(codec, frag.id, &frag.name, &frag.arguments))
            .collect();
        let content = if self.content.is_empty() {
            None
        } else {
            Some(self.content)
        };
        LlmResponse {
            content,
            tool_calls,
            tokens_in: self.tokens_in,
            tokens_out: self.tokens_out,
        }
    }
}

#[derive(Deserialize)]
struct OaStreamChunk {
    #[serde(default)]
    choices: Vec<OaStreamChoice>,
    #[serde(default)]
    usage: Option<OaUsage>,
}

#[derive(Deserialize)]
struct OaStreamChoice {
    delta: OaStreamDelta,
}

#[derive(Deserialize, Default)]
struct OaStreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OaStreamToolCall>>,
}

#[derive(Deserialize)]
struct OaStreamToolCall {
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<OaStreamToolFn>,
}

#[derive(Deserialize)]
struct OaStreamToolFn {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

/// Build a [`ToolCallRecord`] from the raw pieces an OpenAI tool call
/// carries: the call id, the dotted `extension.tool` name, and the
/// JSON-encoded `arguments` string. Shared by the blocking ([`complete`])
/// and streaming ([`StreamAccumulator::finish`]) paths so both decode
/// arguments and split the name identically. Malformed argument JSON
/// degrades to an empty object rather than failing the whole turn.
///
/// [`complete`]: OpenAiLlmBackend::complete
fn build_tool_call_record(
    codec: &ToolNameCodec,
    call_id: String,
    name: &str,
    raw_args: &str,
) -> ToolCallRecord {
    let args: serde_json::Value = serde_json::from_str(raw_args).unwrap_or(serde_json::json!({}));
    let (extension_id, tool_name) = codec.decode(name);
    ToolCallRecord {
        call_id,
        extension_id,
        tool_name,
        args,
    }
}

/// Encode an `(extension_id, tool_name)` pair into a single OpenAI function
/// name. OpenAI requires `^[a-zA-Z0-9_-]+$` (no dots), but `extension_id`s are
/// dotted (e.g. `greentic.telco-x-tools`) and the historical `{ext}.{tool}`
/// join inserted yet another dot — so every extension tool was rejected with a
/// 400 (`tools[..].function.name` invalid). We escape dots to `_DOT_` and join
/// with `_FN_`, both of which round-trip exactly via [`split_tool_name`]. An
/// empty extension id encodes to the bare tool name (legacy "no dot" case).
pub fn encode_tool_name(extension_id: &str, tool_name: &str) -> String {
    if extension_id.is_empty() {
        return tool_name.to_string();
    }
    format!("{}_FN_{}", extension_id.replace('.', "_DOT_"), tool_name)
}

/// Inverse of [`encode_tool_name`]: split an LLM-emitted function name back into
/// `(extension_id, tool_name)`, restoring escaped dots. No separator → the whole
/// string is the tool name with an empty extension id.
pub fn split_tool_name(name: &str) -> (String, String) {
    match name.split_once("_FN_") {
        Some((ext, tool)) => (ext.replace("_DOT_", "."), tool.to_string()),
        None => (String::new(), name.to_string()),
    }
}

fn build_messages(req: &LlmRequest) -> Vec<OaMessage> {
    let mut out: Vec<OaMessage> = Vec::with_capacity(req.history.len() + 1);
    out.push(OaMessage::System {
        content: req.system_prompt.clone(),
    });
    for m in &req.history {
        match m {
            ChatMessage::System { content } => out.push(OaMessage::System {
                content: content.clone(),
            }),
            ChatMessage::User { content } => out.push(OaMessage::User {
                content: content.clone(),
            }),
            ChatMessage::Assistant {
                content,
                tool_calls,
            } => {
                let calls = tool_calls
                    .iter()
                    .map(|tc| OaToolCallEmit {
                        id: tc.call_id.clone(),
                        typ: "function",
                        function: OaToolFn {
                            name: wire_tool_name(&tc.extension_id, &tc.tool_name),
                            arguments: tc.args.to_string(),
                        },
                    })
                    .collect();
                out.push(OaMessage::Assistant {
                    content: Some(content.clone()),
                    tool_calls: calls,
                });
            }
            ChatMessage::Tool { call_id, content } => {
                out.push(OaMessage::Tool {
                    tool_call_id: call_id.clone(),
                    content: content.to_string(),
                });
            }
        }
    }
    out
}

fn build_tool(t: &LlmToolSchema) -> OaTool<'_> {
    OaTool {
        typ: "function",
        function: OaToolDef {
            name: wire_tool_name(&t.extension_id, &t.tool_name),
            description: &t.description,
            parameters: &t.parameters,
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn base_url_falls_back_to_openai_when_unconfigured() {
        // Unset, empty and whitespace-only all mean "operator configured
        // nothing" — none of them may produce an empty base URL, which would
        // build the request path `/v1/chat/completions` against no host.
        assert_eq!(normalize_base_url(None), "https://api.openai.com");
        assert_eq!(
            normalize_base_url(Some(String::new())),
            "https://api.openai.com"
        );
        assert_eq!(
            normalize_base_url(Some("   ".to_string())),
            "https://api.openai.com"
        );
    }

    #[test]
    fn base_url_honours_a_configured_endpoint_and_trims_it() {
        assert_eq!(
            normalize_base_url(Some("http://127.0.0.1:11434".to_string())),
            "http://127.0.0.1:11434"
        );
        // Surrounding whitespace and a trailing slash are operator typos, not
        // part of the host: `https://host//v1/chat/completions` 404s on several
        // OpenAI-compatible gateways.
        assert_eq!(
            normalize_base_url(Some("  http://127.0.0.1:11434/  ".to_string())),
            "http://127.0.0.1:11434"
        );
    }

    #[test]
    fn tool_name_round_trips_and_is_openai_safe() {
        // Dotted extension ids are the common case. OpenAI forbids dots in
        // function names (^[a-zA-Z0-9_-]+$), so encode escapes them; split must
        // restore the exact id (it's used for dispatch lookup).
        for (ext, tool) in [
            ("greentic.telco-x-tools", "tx_resolve_prefix"),
            ("greentic.tavily", "tavily_search"),
            ("http", "fetch"),
        ] {
            let encoded = encode_tool_name(ext, tool);
            assert!(
                !encoded.contains('.'),
                "encoded name must be OpenAI-safe (no dots): {encoded}"
            );
            assert_eq!(split_tool_name(&encoded), (ext.into(), tool.into()));
        }
        // No extension id → bare tool name, both directions.
        assert_eq!(encode_tool_name("", "toolname-no-ext"), "toolname-no-ext");
        assert_eq!(
            split_tool_name("toolname-no-ext"),
            (String::new(), "toolname-no-ext".into())
        );
    }

    #[test]
    fn line_buffer_reassembles_multibyte_char_split_across_chunks() {
        // The two bytes of `é` (0xC3 0xA9) are split across two chunks.
        // Decoding each chunk independently would yield U+FFFD; the buffer
        // must reassemble them so the emitted line contains no replacement
        // characters.
        let full = "data: {\"choices\":[{\"delta\":{\"content\":\"héllo\"}}]}\n";
        let bytes = full.as_bytes();
        // Find a split point inside the `é` (between its two UTF-8 bytes).
        let e_pos = full.find('é').expect("contains é");
        let split = e_pos + 1; // mid-scalar boundary
        let (a, b) = bytes.split_at(split);

        let mut lb = SseLineBuffer::new();
        let mut lines = lb.push_bytes(a);
        lines.extend(lb.push_bytes(b));
        assert_eq!(lines.len(), 1, "one complete line expected");
        assert!(
            !lines[0].contains('\u{FFFD}'),
            "line must not contain U+FFFD: {:?}",
            lines[0]
        );

        // Feed the reassembled line through the accumulator and verify the
        // delta is the intact string.
        let mut acc = StreamAccumulator::new();
        let delta = acc.push_line(&lines[0]).expect("parse");
        assert_eq!(delta.as_deref(), Some("héllo"));
    }

    #[test]
    fn line_buffer_reassembles_emoji_split_across_chunks() {
        // A 4-byte emoji split across two chunks must not corrupt.
        let full = "data: {\"choices\":[{\"delta\":{\"content\":\"hi 😀\"}}]}\n";
        let bytes = full.as_bytes();
        let emoji_pos = full.find('😀').expect("contains emoji");
        let (a, b) = bytes.split_at(emoji_pos + 2); // mid-scalar
        let mut lb = SseLineBuffer::new();
        let mut lines = lb.push_bytes(a);
        lines.extend(lb.push_bytes(b));
        assert_eq!(lines.len(), 1);
        let mut acc = StreamAccumulator::new();
        let delta = acc.push_line(&lines[0]).expect("parse");
        assert_eq!(delta.as_deref(), Some("hi 😀"));
    }

    #[test]
    fn line_buffer_yields_remainder_without_trailing_newline() {
        // A final event with no terminating newline must still be emitted
        // via take_remainder.
        let mut lb = SseLineBuffer::new();
        let lines = lb.push_bytes(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}");
        assert!(lines.is_empty(), "no newline yet: no complete line");
        let rem = lb.take_remainder().expect("remainder present");
        let mut acc = StreamAccumulator::new();
        assert_eq!(acc.push_line(&rem).expect("parse").as_deref(), Some("x"));
        assert!(lb.take_remainder().is_none(), "remainder consumed");
    }

    #[test]
    fn line_buffer_splits_two_events_in_one_chunk() {
        // Two `data:` events arriving in one byte chunk are both returned.
        let mut lb = SseLineBuffer::new();
        let chunk = "data: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\
                     data: {\"choices\":[{\"delta\":{\"content\":\"B\"}}]}\n";
        let lines = lb.push_bytes(chunk.as_bytes());
        assert_eq!(lines.len(), 2);
        let mut acc = StreamAccumulator::new();
        let d0 = acc.push_line(&lines[0]).expect("parse");
        let d1 = acc.push_line(&lines[1]).expect("parse");
        assert_eq!(d0.as_deref(), Some("A"));
        assert_eq!(d1.as_deref(), Some("B"));
    }

    #[test]
    fn push_line_handles_crlf_line_ending() {
        // A line carrying a trailing "\r" (CRLF wire format) parses cleanly.
        let mut acc = StreamAccumulator::new();
        // Simulate what SseLineBuffer produces after trimming: but feed a raw
        // CRLF-tailed payload to confirm push_line's own trim() handles it.
        let line = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\r";
        let delta = acc.push_line(line).expect("parse");
        assert_eq!(delta.as_deref(), Some("hi"));
    }

    #[test]
    fn push_line_ignores_sse_comment_keepalive() {
        // An SSE comment / keep-alive line (starts with ':') must yield no
        // delta and no error.
        let mut acc = StreamAccumulator::new();
        assert_eq!(acc.push_line(": keep-alive").expect("no error"), None);
        assert_eq!(acc.push_line(":").expect("no error"), None);
        // A blank line is also a no-op.
        assert_eq!(acc.push_line("").expect("no error"), None);
    }

    #[test]
    fn accumulates_stream_lines_into_response() {
        let lines = [
            r#"data: {"choices":[{"delta":{"content":"Hel"}}]}"#,
            r#"data: {"choices":[{"delta":{"content":"lo"}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"kb_FN_lookup","arguments":"{\"q\":"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}"#,
            r#"data: {"usage":{"prompt_tokens":7,"completion_tokens":9},"choices":[]}"#,
            "data: [DONE]",
        ];
        let mut acc = StreamAccumulator::new();
        let mut deltas = Vec::new();
        for l in lines {
            if let Some(text) = acc.push_line(l).expect("parse") {
                deltas.push(text);
            }
        }
        let resp = acc.finish(&ToolNameCodec::default());
        assert_eq!(deltas, vec!["Hel".to_string(), "lo".to_string()]);
        assert_eq!(resp.content.as_deref(), Some("Hello"));
        assert_eq!(resp.tool_calls.len(), 1);
        assert_eq!(resp.tool_calls[0].tool_name, "lookup");
        assert_eq!(resp.tool_calls[0].args, serde_json::json!({"q":"x"}));
        assert_eq!(resp.tokens_in, 7);
        assert_eq!(resp.tokens_out, 9);
    }

    #[test]
    fn assistant_with_empty_tool_calls_omits_field() {
        // Regression: a text-only assistant turn must NOT serialise
        // `tool_calls: []` (OpenAI 400, code `empty_array`).
        use crate::config::LlmProviderRef;
        use crate::state::ChatMessage;
        let req = LlmRequest {
            system_prompt: "sys".into(),
            history: vec![ChatMessage::Assistant {
                content: "hi".into(),
                tool_calls: vec![],
            }],
            tools: vec![],
            provider: LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o".into(),
                credential_ref: None,
            },
        };
        let value = serde_json::to_value(build_messages(&req)).unwrap();
        // [0] = system prompt, [1] = the assistant turn.
        assert_eq!(value[1]["role"], "assistant");
        assert!(
            value[1].get("tool_calls").is_none(),
            "empty tool_calls must be omitted, got: {}",
            value[1]
        );
    }
}
