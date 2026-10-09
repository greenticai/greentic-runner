//! The whole external-knowledge chain, run for real: adapter → real WASM
//! design extension → real HTTP retrieval service.
//!
//! Every layer of this feature is separately covered by unit tests that stop at
//! its own edge. `knowledge_ext`'s own tests parse hand-written JSON; the
//! extension's tests exercise the guest in isolation; the contract checker
//! exercises a service without either. Nothing until now has driven one
//! retrieval through all three, which is exactly where a contract mismatch
//! between them would live.
//!
//! ## Why these tests are `#[ignore]`d
//!
//! They need an installed, matching build of the `greentic.rag-http` extension
//! on the developer's machine (`~/.greentic/extensions/design/…`), they compile
//! a real WASM component through Cranelift, and they bind loopback sockets.
//! None of that belongs in an unattended CI lane; all of it is the point when
//! run by hand.
//!
//! ## Why the retrieval service is served from the test
//!
//! The conformance stub that fixed this contract is
//! `scripts/rag_contract_check.py --serve-stub` in **greentic-designer**, a
//! different repository. A committed test here cannot depend on a path inside
//! another repository's worktree, so the stub is reproduced in-process instead
//! — same two passages, same `top_k` slicing, same refusal of a request that
//! carries no auth header.
//!
//! Serving it here also buys something the Python stub cannot give: the test
//! records the request the WASM guest actually sent, so the auth-header and
//! `top_k` assertions test the hop rather than the answer.
//!
//! ## Running them
//!
//! The installed extension is unsigned, so the runtime's signature gate has to
//! be opened explicitly — which is a real property of this chain, not a test
//! detail:
//!
//! ```sh
//! GREENTIC_EXT_ALLOW_UNSIGNED=1 cargo test -p greentic-runner-host \
//!     --features dev-allow-unsigned --test knowledge_ext_e2e -- --ignored --nocapture
//! ```
#![cfg(feature = "agentic-worker")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use greentic_aw_runtime::config::MemoryProviderRef;
use greentic_aw_runtime::knowledge::{Knowledge, KnowledgeError, KnowledgeQuery};
use greentic_ext_runtime::host_ports::InMemorySecrets;
use greentic_ext_runtime::{DiscoveryPaths, ExtensionRuntime, HostOverrides, RuntimeConfig};
use greentic_runner_host::runner::knowledge_ext::{
    EXTENSION_ID_PARAM, EXTENSION_PROVIDER_ID, ExtensionKnowledge, TOOL_NAME_PARAM,
};
use greentic_types::{EnvId, TenantCtx, TenantId};

/// The extension id the binding names, and the tool inside it that retrieves.
const EXT_ID: &str = "greentic.rag-http";
const TOOL_NAME: &str = "search_knowledge";
/// The installed directory name. Pinned rather than globbed: a test that
/// silently picks up whichever version happens to be on the box proves nothing
/// about the one the chain was built against.
const EXT_DIR: &str = "greentic.rag-http-0.1.0";

/// The token the stub demands. Never asserted against a chunk — it must not
/// come back out of the extension.
const TOKEN: &str = "e2e-secret-token";

// ---------------------------------------------------------------------------
// The retrieval service
// ---------------------------------------------------------------------------

/// One request the guest actually sent, as the service saw it.
#[derive(Clone, Debug)]
struct SeenRequest {
    auth_header: Option<String>,
    body: serde_json::Value,
}

/// A loopback HTTP retrieval service speaking the §6 contract.
struct StubService {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    stop: Arc<AtomicBool>,
}

impl StubService {
    /// Bind, serve on a background thread, and return once the port is live.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
        let addr = listener.local_addr().expect("read the bound address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let thread_seen = Arc::clone(&seen);
        let thread_stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if thread_stop.load(Ordering::SeqCst) {
                    return;
                }
                match stream {
                    Ok(stream) => serve_one(stream, &thread_seen),
                    Err(_) => return,
                }
            }
        });

        Self { addr, seen, stop }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/v1/search", self.addr)
    }

    fn requests(&self) -> Vec<SeenRequest> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Drop for StubService {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Unblock the accept loop so the thread observes `stop` and exits.
        let _ = TcpStream::connect(self.addr);
    }
}

/// Handle one request exactly as the conformance stub does.
fn serve_one(mut stream: TcpStream, seen: &Arc<Mutex<Vec<SeenRequest>>>) {
    let mut reader = BufReader::new(match stream.try_clone() {
        Ok(clone) => clone,
        Err(_) => return,
    });

    let mut auth_header = None;
    let mut content_length = 0usize;
    let mut line = String::new();
    // Request line, then headers until the blank line.
    if reader.read_line(&mut line).is_err() {
        return;
    }
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => return,
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if name == "authorization" {
                auth_header = Some(value);
            } else if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
        }
    }

    let mut body_bytes = vec![0u8; content_length];
    if reader.read_exact(&mut body_bytes).is_err() {
        return;
    }
    let body: serde_json::Value =
        serde_json::from_slice(&body_bytes).unwrap_or(serde_json::Value::Null);

    seen.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(SeenRequest {
            auth_header: auth_header.clone(),
            body: body.clone(),
        });

    // No auth header ⇒ refuse. The contract requires this and the extension
    // must therefore be sending one.
    if auth_header.is_none() {
        write_response(&mut stream, 401, r#"{"error":"unauthorized"}"#);
        return;
    }

    let top_k = body
        .get("top_k")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(3) as usize;

    let chunks = serde_json::json!([
        { "text": "first chunk", "score": 0.95 },
        { "text": "second chunk", "score": 0.85 },
    ]);
    let chunks: Vec<serde_json::Value> = chunks
        .as_array()
        .expect("a literal array")
        .iter()
        .take(top_k)
        .cloned()
        .collect();
    let returned = chunks.len();
    let payload = serde_json::json!({
        "chunks": chunks,
        "returned": returned,
        "truncated": returned < 2,
    })
    .to_string();
    write_response(&mut stream, 200, &payload);
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = if status == 200 { "OK" } else { "Unauthorized" };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

/// A loopback port with nothing listening on it: bound, then released, so the
/// OS has just confirmed it free. The closest reproducible stand-in for "the
/// customer's service is down".
fn a_closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
    let port = listener.local_addr().expect("read the port").port();
    drop(listener);
    port
}

// ---------------------------------------------------------------------------
// The extension runtime
// ---------------------------------------------------------------------------

/// The install root the runner reads, honouring the same override the
/// production resolver does.
fn extensions_root() -> PathBuf {
    if let Ok(dir) = std::env::var("GREENTIC_EXTENSIONS_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME").expect("HOME must be set to locate the install root");
    PathBuf::from(home).join(".greentic").join("extensions")
}

/// A process-shared blocking client, built off the async runtime.
///
/// Mirrors `agent_node::shared_blocking_http_client`, and for its reason:
/// `reqwest::blocking::Client` owns a tokio runtime, and dropping one inside an
/// async context panics. Held in a `OnceLock` so the underlying runtime is
/// released at process exit rather than at the end of a `#[tokio::test]`.
fn shared_blocking_http_client() -> Option<greentic_ext_runtime::reqwest::blocking::Client> {
    static CLIENT: OnceLock<Option<greentic_ext_runtime::reqwest::blocking::Client>> =
        OnceLock::new();
    CLIENT
        .get_or_init(|| {
            std::thread::spawn(|| {
                greentic_ext_runtime::reqwest::blocking::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .ok()
            })
            .join()
            .ok()
            .flatten()
        })
        .clone()
}

/// Build a real `ExtensionRuntime` over the installed extension, with the three
/// `secret://client-rag/…` URIs it declares resolvable.
///
/// Mirrors `agent_node::build_ext_runtime`'s construction — same
/// `DiscoveryPaths`, same `HostOverrides` fields, same
/// `register_loaded_from_dir` — differing only in supplying test secrets and in
/// registering the one extension under test rather than scanning the whole
/// design directory.
fn build_runtime(endpoint: &str) -> Arc<ExtensionRuntime> {
    let root = extensions_root();
    let ext_dir = root.join("design").join(EXT_DIR);
    assert!(
        ext_dir.join("describe.json").is_file(),
        "the `{EXT_ID}` extension must be installed at {} — install it before running this test",
        ext_dir.display()
    );

    let secrets = InMemorySecrets::new();
    secrets.insert("secret://client-rag/endpoint", endpoint);
    // The conformance stub keys on the literal `Authorization` header, so the
    // header name is part of the contract being exercised, not an arbitrary
    // choice.
    secrets.insert("secret://client-rag/auth_header", "Authorization");
    secrets.insert("secret://client-rag/token", TOKEN);

    let overrides = HostOverrides {
        secrets_backend: Arc::new(secrets),
        http_client: shared_blocking_http_client(),
        llm_port: None,
        ..HostOverrides::default()
    };
    let config =
        RuntimeConfig::from_paths(DiscoveryPaths::new(root)).with_host_overrides(overrides);
    let mut runtime = ExtensionRuntime::new(config).expect("extension runtime construction");
    runtime
        .register_loaded_from_dir(&ext_dir)
        .unwrap_or_else(|e| panic!("loading {} failed: {e}", ext_dir.display()));
    Arc::new(runtime)
}

fn tenant() -> TenantCtx {
    TenantCtx::new(
        EnvId::new("prod").expect("a valid env id"),
        TenantId::new("acme").expect("a valid tenant id"),
    )
}

/// A binding naming this provider, with whatever delegation target is given.
fn binding(extension_id: Option<&str>, tool_name: Option<&str>) -> MemoryProviderRef {
    let mut params = serde_json::Map::new();
    if let Some(id) = extension_id {
        params.insert(EXTENSION_ID_PARAM.to_string(), id.into());
    }
    if let Some(name) = tool_name {
        params.insert(TOOL_NAME_PARAM.to_string(), name.into());
    }
    MemoryProviderRef {
        provider: EXTENSION_PROVIDER_ID.to_string(),
        capability: "cap://dw.knowledge".to_string(),
        params,
        credential_ref: None,
    }
}

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

/// Adapter → WASM extension → HTTP service, end to end.
///
/// Hops exercised, in order: the binding's `params` are read by the adapter;
/// `invoke_tool_ctx` instantiates the real component; the guest resolves its
/// three secrets through the host port; the guest's HTTP call passes the
/// runtime's URL allow-list and reaches the service; the service's body comes
/// back through `parse_chunks` as ranked chunks.
#[tokio::test]
#[ignore = "needs the greentic.rag-http extension installed; compiles real WASM"]
async fn a_bound_extension_retrieves_real_chunks_over_http() {
    let service = StubService::start();
    let knowledge = ExtensionKnowledge::new(build_runtime(&service.endpoint()), None);

    let chunks = knowledge
        .search_bound(
            &tenant(),
            KnowledgeQuery {
                query: "what is the refund window".to_string(),
                limit: Some(5),
            },
            Some(&binding(Some(EXT_ID), Some(TOOL_NAME))),
        )
        .await
        .expect("the whole chain must complete");

    assert_eq!(
        chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
        ["first chunk", "second chunk"],
        "the service's own order is the ranking and must survive both hops"
    );
    assert_eq!(chunks[0].score, 0.95);
    assert_eq!(chunks[1].score, 0.85);

    // The HTTP hop happened, and it happened authenticated. Asserting on the
    // request the service SAW is what separates "the extension called out" from
    // "the extension answered from somewhere else".
    let seen = service.requests();
    assert_eq!(seen.len(), 1, "exactly one retrieval request: {seen:?}");
    assert_eq!(
        seen[0].auth_header.as_deref(),
        Some(TOKEN),
        "the token must reach the service in the declared header"
    );
    assert_eq!(seen[0].body["query"], "what is the refund window");

    // No chunk may carry the credential back out.
    for chunk in &chunks {
        assert!(
            !chunk.text.contains(TOKEN),
            "the credential must never travel back in a chunk"
        );
    }
}

/// `top_k` is not decoration: the adapter's `limit` has to reach the service.
#[tokio::test]
#[ignore = "needs the greentic.rag-http extension installed; compiles real WASM"]
async fn the_querys_limit_reaches_the_service_as_top_k() {
    let service = StubService::start();
    let knowledge = ExtensionKnowledge::new(build_runtime(&service.endpoint()), None);

    let chunks = knowledge
        .search_bound(
            &tenant(),
            KnowledgeQuery {
                query: "escalation contacts".to_string(),
                limit: Some(1),
            },
            Some(&binding(Some(EXT_ID), Some(TOOL_NAME))),
        )
        .await
        .expect("the whole chain must complete");

    let seen = service.requests();
    assert_eq!(seen.len(), 1, "exactly one retrieval request: {seen:?}");
    assert_eq!(
        seen[0].body["top_k"], 1,
        "the query's limit must travel to the service, not just bound the parse"
    );
    assert_eq!(
        chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
        ["first chunk"],
        "the service honoured top_k and only its first passage came back"
    );
}

/// The service is down. Half the point of this adapter is that this is a
/// reported failure and not a confident answer from nothing.
#[tokio::test]
#[ignore = "needs the greentic.rag-http extension installed; compiles real WASM"]
async fn a_stopped_service_is_a_backend_error_never_an_empty_result() {
    let endpoint = format!("http://127.0.0.1:{}/v1/search", a_closed_port());
    let knowledge = ExtensionKnowledge::new(build_runtime(&endpoint), None);

    let outcome = knowledge
        .search_bound(
            &tenant(),
            KnowledgeQuery {
                query: "anything".to_string(),
                limit: Some(3),
            },
            Some(&binding(Some(EXT_ID), Some(TOOL_NAME))),
        )
        .await;

    match outcome {
        Err(KnowledgeError::Backend(message)) => {
            assert!(
                !message.contains(TOKEN),
                "an error message must not disclose the credential: {message}"
            );
        }
        Err(other) => panic!("expected a Backend error, got {other:?}"),
        Ok(chunks) => panic!(
            "an unreachable service answered with {} chunk(s); an empty result here is the \
             silent degrade this adapter exists to remove",
            chunks.len()
        ),
    }
}

/// A binding that names this provider and gives it nothing to invoke. It must
/// name the key, because that key is the whole fix an operator has to make.
#[tokio::test]
#[ignore = "needs the greentic.rag-http extension installed; compiles real WASM"]
async fn a_blank_tool_name_is_a_backend_error_naming_the_key() {
    let service = StubService::start();
    let knowledge = ExtensionKnowledge::new(build_runtime(&service.endpoint()), None);

    let mut malformed = binding(Some(EXT_ID), None);
    malformed
        .params
        .insert(TOOL_NAME_PARAM.to_string(), "   ".into());

    let outcome = knowledge
        .search_bound(
            &tenant(),
            KnowledgeQuery {
                query: "anything".to_string(),
                limit: Some(3),
            },
            Some(&malformed),
        )
        .await;

    match outcome {
        Err(KnowledgeError::Backend(message)) => assert!(
            message.contains(TOOL_NAME_PARAM),
            "the error must name the param an operator has to fix: {message}"
        ),
        Err(other) => panic!("expected a Backend error, got {other:?}"),
        Ok(chunks) => panic!(
            "a binding with no target retrieved {} chunk(s)",
            chunks.len()
        ),
    }

    assert!(
        service.requests().is_empty(),
        "a malformed binding must be refused before any service is called"
    );
}
