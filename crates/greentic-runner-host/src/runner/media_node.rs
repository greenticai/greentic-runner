//! Flow-execution media node (LOCKED ENCODING v1: `component == "media.create"`).
//!
//! A plain flow can create a file (`txt`, `md`, `csv`, `pdf`) and store it
//! through the same artifact `put` port an extension's `host.artifact.put`
//! uses, so the file reaches the user as the same `artifact://` id a worker's
//! file does.
//!
//! ```text
//! nodes:
//!   make_pdf:
//!     media.create:
//!       format: pdf              # txt | md | csv | pdf (required)
//!       name: "Claim Summary"    # file stem, sanitised by the renderer
//!       text: "{{vars.summary}}" # txt / md / pdf; blank is an error value
//!       rows: [[...], ...]       # csv only: array of arrays
//!       output: file             # optional state key
//!     routing: end
//! ```
//!
//! The result is the C5 value (`docs/media-contract-v1.md`):
//! `{ok:true, artifact:{id,mime_type,name}, artifacts:[..], warnings:[..]}` or
//! `{ok:false, error:{code,message}}`. A failure is a VALUE; the engine marks
//! the node failed only when it has an `on_error` edge (see
//! `engine::media_node_output`). Without an `output:` key the result IS the
//! node output, which is what greentic-start's outbound collector reads (a
//! top-level `artifact`).
//!
//! Rendering is ported from `component-media-ext`, see [`super::media_render`].

use std::sync::Arc;

use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};
use serde_json::Value;

use super::media_render::{
    Artifact, Failure, MAX_ARTIFACT_BYTES, codes, failure, parse_create_document, render, success,
};

/// Extension id reported to the port (host log only).
const SOURCE_ID: &str = "greentic.media";

/// Map a port refusal onto a C5 failure. The port's `Unavailable` text is a
/// fixed, credential-free reason; a door status is recognised in it.
fn put_failure(error: &ArtifactPortError) -> Failure {
    match error {
        ArtifactPortError::Unsupported => Failure::new(
            codes::ARTIFACT_UNAVAILABLE,
            "this runtime has no artifact storage",
        ),
        ArtifactPortError::InvalidMediaType => Failure::new(
            codes::ARTIFACT_REJECTED,
            "artifact storage refused the file type",
        ),
        ArtifactPortError::QuotaExceeded => Failure::new(
            codes::ARTIFACT_REJECTED,
            "artifact storage refused the file: the storage quota is exhausted",
        ),
        ArtifactPortError::Unavailable(reason) => match door_status(reason) {
            Some(401 | 403) => Failure::new(
                codes::UNAUTHORIZED,
                "artifact storage did not accept this worker's credential",
            ),
            Some(413) => Failure::new(
                codes::TOO_LARGE,
                "artifact storage refused the file as too large",
            ),
            Some(_) => Failure::new(
                codes::ARTIFACT_REJECTED,
                "artifact storage refused the file",
            ),
            None => Failure::new(
                codes::ARTIFACT_UNAVAILABLE,
                "artifact storage is unavailable",
            ),
        },
        _ => Failure::new(
            codes::ARTIFACT_UNAVAILABLE,
            "artifact storage is unavailable",
        ),
    }
}

/// The status in the port's `door answered <n>` reason, if that is what it is.
fn door_status(reason: &str) -> Option<u16> {
    reason.strip_prefix("door answered ")?.trim().parse().ok()
}

/// Create the file described by the rendered node `payload` and store it for
/// `tenant`. Always returns a C5 value; never panics.
pub async fn create(payload: &Value, tenant: &str, port: Option<&Arc<dyn ArtifactPort>>) -> Value {
    let request = match parse_create_document(payload) {
        Ok(request) => request,
        Err(f) => return failure(&f),
    };
    let rendered = match render(&request) {
        Ok(rendered) => rendered,
        Err(f) => return failure(&f),
    };
    if rendered.bytes.len() > MAX_ARTIFACT_BYTES {
        return failure(&Failure::new(
            codes::TOO_LARGE,
            "the document is larger than the 10 MB limit",
        ));
    }
    let Some(port) = port else {
        return failure(&Failure::new(
            codes::ARTIFACT_UNAVAILABLE,
            "this runtime has no artifact storage",
        ));
    };
    let name = format!("{}.{}", request.stem, rendered.ext);
    let put = ArtifactPutRequest {
        bytes: rendered.bytes,
        mime_type: rendered.mime.to_string(),
        name: name.clone(),
    };
    let ctx = HostCallContext {
        tenant: Some(tenant.to_string()).filter(|t| !t.trim().is_empty()),
        user_email: None,
    };
    let port = Arc::clone(port);
    // `ArtifactPort::put` is synchronous and blocks on the door; run it on the
    // blocking pool so it never parks an async worker.
    let stored = tokio::task::spawn_blocking(move || port.put(SOURCE_ID, &ctx, put)).await;
    match stored {
        Ok(Ok(id)) => success(
            &[Artifact {
                id,
                mime_type: rendered.mime.to_string(),
                name,
            }],
            &rendered.warnings,
        ),
        Ok(Err(error)) => {
            tracing::warn!(code = "media_create_put_failed", %error, "media.create put failed");
            failure(&put_failure(&error))
        }
        Err(_) => failure(&Failure::new(
            codes::ARTIFACT_UNAVAILABLE,
            "artifact storage is unavailable",
        )),
    }
}

/// Reserved key of a turn's final reply payload that lists the files the
/// RUNNER created for the end user during that turn (`media.create`), as
/// `[{id, mime_type, name}]`.
///
/// This is a runner-to-host contract: greentic-start delivers every listed
/// file whose id this deployment's artifact port stored, in addition to a
/// top-level `artifact`. Only [`attach_created`] writes it, and it removes any
/// copy a flow's own data put there, so a node (or an HTTP echo) cannot
/// forge the list. A failure result never lists a file.
pub const CREATED_ARTIFACTS_KEY: &str = "created_artifacts";

/// More files than this in one turn are not tracked (the host delivers at
/// most 5 per reply anyway).
pub const MAX_CREATED_PER_TURN: usize = 20;

/// The files a successful `media.create` result created; empty for a failure.
pub fn created_files(result: &Value) -> Vec<Value> {
    if is_failure(result) || result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Vec::new();
    }
    let listed = result
        .get("artifacts")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .cloned()
        .or_else(|| result.get("artifact").map(|one| vec![one.clone()]))
        .unwrap_or_default();
    listed
        .into_iter()
        .filter(|file| file.get("id").and_then(Value::as_str).is_some())
        .collect()
}

fn strip_reserved(value: &mut Value) {
    if let Value::Object(map) = value {
        map.remove(CREATED_ARTIFACTS_KEY);
    }
}

/// Fold the turn's created files into its final reply `reply`.
///
/// Always removes a pre-existing [`CREATED_ARTIFACTS_KEY`] first (a flow must
/// not be able to name files). With files: an object gets the key; an array
/// (several emitted payloads) gets it on its LAST element, so the link goes
/// with the final reply; a plain string becomes `{text, key}`; `null` becomes
/// `{key}` (a file-only turn).
pub fn attach_created(mut reply: Value, created: Vec<Value>) -> Value {
    match &mut reply {
        Value::Array(items) => items.iter_mut().for_each(strip_reserved),
        other => strip_reserved(other),
    }
    if created.is_empty() {
        return reply;
    }
    let list = Value::Array(created);
    let key = CREATED_ARTIFACTS_KEY.to_string();
    match reply {
        Value::Object(mut map) => {
            map.insert(key, list);
            Value::Object(map)
        }
        Value::Null => serde_json::json!({ key: list }),
        Value::String(text) => serde_json::json!({ "text": text, key: list }),
        Value::Array(mut items) => {
            match items.last_mut() {
                Some(Value::Object(map)) => {
                    map.insert(key, list);
                }
                Some(Value::String(text)) => {
                    let text = std::mem::take(text);
                    if let Some(last) = items.last_mut() {
                        *last = serde_json::json!({ "text": text, key: list });
                    }
                }
                _ => items.push(serde_json::json!({ key: list })),
            }
            Value::Array(items)
        }
        other => Value::Array(vec![other, serde_json::json!({ key: list })]),
    }
}

/// Whether a node result is a C5 failure value.
pub fn is_failure(result: &Value) -> bool {
    result.get("ok").and_then(Value::as_bool) == Some(false)
}

/// The failure's `error.message`, for the node's `errors[0]`.
pub fn failure_message(result: &Value) -> String {
    result
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("media.create failed")
        .to_string()
}

/// The failure's `error.code`.
pub fn failure_code(result: &Value) -> String {
    result
        .pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or("media_create_failed")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    type Seen = (String, String, usize, Option<String>);

    #[derive(Default)]
    struct Stub {
        seen: Mutex<Vec<Seen>>,
        answer: Mutex<Option<ArtifactPortError>>,
    }

    impl ArtifactPort for Stub {
        fn put(
            &self,
            _extension_id: &str,
            ctx: &HostCallContext,
            request: ArtifactPutRequest,
        ) -> Result<String, ArtifactPortError> {
            if let Ok(mut seen) = self.seen.lock() {
                seen.push((
                    request.name.clone(),
                    request.mime_type.clone(),
                    request.bytes.len(),
                    ctx.tenant.clone(),
                ));
            }
            match self.answer.lock().ok().and_then(|mut a| a.take()) {
                Some(error) => Err(error),
                None => Ok(format!("artifact://{}", "a".repeat(64))),
            }
        }
    }

    fn port(stub: &Arc<Stub>) -> Arc<dyn ArtifactPort> {
        Arc::clone(stub) as Arc<dyn ArtifactPort>
    }

    #[tokio::test]
    async fn each_format_is_stored_with_its_mime_and_name() {
        let cases = [
            (
                json!({"format":"txt","name":"n","text":"hi"}),
                "n.txt",
                "text/plain",
            ),
            (
                json!({"format":"md","name":"n","text":"# hi"}),
                "n.md",
                "text/markdown",
            ),
            (
                json!({"format":"csv","name":"n","rows":[["a","b"]]}),
                "n.csv",
                "text/csv",
            ),
            (
                json!({"format":"pdf","name":"n","text":"hi"}),
                "n.pdf",
                "application/pdf",
            ),
        ];
        for (payload, name, mime) in cases {
            let stub = Arc::new(Stub::default());
            let value = create(&payload, "acme", Some(&port(&stub))).await;
            assert_eq!(value["ok"], true, "{value}");
            assert_eq!(value["artifact"]["name"], name);
            assert_eq!(value["artifact"]["mime_type"], mime);
            assert_eq!(value["artifacts"].as_array().map(Vec::len), Some(1));
            let seen = stub.seen.lock().map(|s| s.clone()).unwrap_or_default();
            assert_eq!(seen[0].3.as_deref(), Some("acme"));
        }
    }

    #[tokio::test]
    async fn bad_input_never_reaches_the_port() {
        let stub = Arc::new(Stub::default());
        let blank = create(
            &json!({"format":"pdf","text":"  "}),
            "acme",
            Some(&port(&stub)),
        )
        .await;
        assert_eq!(blank["error"]["code"], "invalid_input");
        let fmt = create(
            &json!({"format":"docx","text":"x"}),
            "acme",
            Some(&port(&stub)),
        )
        .await;
        assert_eq!(fmt["error"]["code"], "unsupported_format");
        assert!(stub.seen.lock().map(|s| s.is_empty()).unwrap_or(false));
    }

    #[tokio::test]
    async fn no_port_is_an_unavailable_value() {
        let value = create(&json!({"format":"txt","text":"x"}), "acme", None).await;
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "artifact_unavailable");
    }

    #[tokio::test]
    async fn port_refusals_map_onto_codes() {
        let cases = [
            (ArtifactPortError::Unsupported, "artifact_unavailable"),
            (ArtifactPortError::InvalidMediaType, "artifact_rejected"),
            (ArtifactPortError::QuotaExceeded, "artifact_rejected"),
            (
                ArtifactPortError::Unavailable("door answered 401".into()),
                "unauthorized",
            ),
            (
                ArtifactPortError::Unavailable("door answered 403".into()),
                "unauthorized",
            ),
            (
                ArtifactPortError::Unavailable("door answered 413".into()),
                "too_large",
            ),
            (
                ArtifactPortError::Unavailable("door answered 500".into()),
                "artifact_rejected",
            ),
            (
                ArtifactPortError::Unavailable("artifact door unreachable".into()),
                "artifact_unavailable",
            ),
        ];
        for (error, code) in cases {
            let stub = Arc::new(Stub::default());
            if let Ok(mut answer) = stub.answer.lock() {
                *answer = Some(error);
            }
            let value = create(
                &json!({"format":"txt","text":"x"}),
                "acme",
                Some(&port(&stub)),
            )
            .await;
            assert_eq!(value["error"]["code"], code, "{value}");
        }
    }

    #[tokio::test]
    async fn a_pdf_with_unencodable_text_warns() {
        let stub = Arc::new(Stub::default());
        let value = create(
            &json!({"format":"pdf","text":"日本"}),
            "acme",
            Some(&port(&stub)),
        )
        .await;
        assert_eq!(value["warnings"][0]["code"], "characters_replaced");
    }
}

#[cfg(test)]
mod created_tests {
    use super::*;
    use serde_json::json;

    fn file(n: u8) -> Value {
        json!({"id": format!("artifact://{}", n.to_string().repeat(64)), "name": "n.txt"})
    }

    #[test]
    fn only_a_success_lists_files() {
        let ok = success(
            &[Artifact {
                id: "artifact://x".into(),
                mime_type: "text/plain".into(),
                name: "n.txt".into(),
            }],
            &[],
        );
        assert_eq!(created_files(&ok).len(), 1);
        let bad = failure(&Failure::new(codes::INVALID_INPUT, "no"));
        assert!(created_files(&bad).is_empty());
        assert!(created_files(&json!({"artifact": {"id": "a"}})).is_empty());
    }

    #[test]
    fn an_object_reply_gets_the_key_and_a_forged_one_is_replaced() {
        let reply = json!({"text": "hi", "created_artifacts": [{"id": "forged"}]});
        let out = attach_created(reply, vec![file(1)]);
        assert_eq!(out["created_artifacts"], json!([file(1)]));
        let none = attach_created(json!({"created_artifacts": [1]}), vec![]);
        assert!(none.get("created_artifacts").is_none());
    }

    #[test]
    fn shapes_other_than_an_object_are_wrapped_or_extended() {
        let s = attach_created(json!("hello"), vec![file(1)]);
        assert_eq!(s["text"], "hello");
        let n = attach_created(Value::Null, vec![file(1)]);
        assert_eq!(n["created_artifacts"][0], file(1));
        let a = attach_created(json!([{"text": "a"}, "b"]), vec![file(1)]);
        assert!(a[0].get("created_artifacts").is_none());
        assert_eq!(a[1]["text"], "b");
        assert_eq!(a[1]["created_artifacts"][0], file(1));
        let e = attach_created(json!([]), vec![file(1)]);
        assert_eq!(e[0]["created_artifacts"][0], file(1));
        let unchanged = attach_created(json!("x"), vec![]);
        assert_eq!(unchanged, json!("x"));
    }
}
