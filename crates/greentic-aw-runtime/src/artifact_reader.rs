//! Reading attachment bytes for the current turn.
//!
//! Wire contract: the attachments master plan, C3 (admin `artifacts` door):
//! `POST <door base>/get` with `{"id": "artifact://<64 hex>"}` and no `op`
//! field, authenticated by a `gtm_` bearer token holding the `artifacts`
//! purpose. The token is a credential: it is redacted from `Debug` and never
//! put in an error value. Everything the door sends back is untrusted: the body
//! is size-capped while it streams in, the decoded length must agree with the
//! door's own `size_bytes`, and the media type must be one of the v1 types.
//! The tenant is never part of the request: the token decides it.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::StreamExt;

/// Mirrors the 10 MB per-file limit of the spec; also checked on the way in so a
/// misbehaving door cannot make the runner hold an oversize body.
pub const MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;

/// Largest response body read: base64 of the biggest file (4 bytes per 3) plus
/// room for the JSON envelope. Anything longer is refused before it is buffered.
const MAX_BODY_BYTES: usize = MAX_ARTIFACT_BYTES.div_ceil(3) * 4 + 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(20);

/// The v1 media types (master plan: image/jpeg, png, gif, webp; text/plain,
/// markdown, csv; application/json, pdf). SVG is deliberately absent.
const ALLOWED_MIME: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "text/plain",
    "text/markdown",
    "text/csv",
    "application/json",
    "application/pdf",
];

pub struct ArtifactBytes {
    pub mime_type: String,
    pub name: Option<String>,
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for ArtifactBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the bytes or the sender's name: sizes and the media type only.
        f.debug_struct("ArtifactBytes")
            .field("mime_type", &self.mime_type)
            .field("has_name", &self.name.is_some())
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// A failed read. Never carries the door's response body, the token or a URL.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact not found")]
    NotFound,
    /// HTTP 401: the door does not accept the token.
    #[error("artifact door refused the credential")]
    Unauthorized,
    /// HTTP 403: the token is valid but lacks the `artifacts` purpose, the
    /// commonest misconfiguration, so it is kept apart from `Unauthorized`.
    #[error("artifact door token does not grant the artifacts purpose")]
    PurposeNotGranted,
    #[error("artifact exceeds the size limit")]
    TooLarge,
    #[error("artifact door unavailable: {0}")]
    Unavailable(String),
}

pub trait ArtifactReader: Send + Sync {
    fn get<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>>;
}

/// Why a reader could not be built. Never carries the token or the endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactClientError {
    #[error("artifact door token is empty")]
    EmptyToken,
    #[error("artifact door token contains a control character")]
    InvalidToken,
    #[error("artifact http client could not be built")]
    Client,
}

/// Reads one artifact from the admin door.
///
/// Memory: one read can hold roughly 14 MB of raw response body, a 14 MB base64
/// string and the 10 MB decoded bytes at the same time. The CALLER (the Task 6
/// wiring) must bound the number of concurrent reads.
pub struct HttpArtifactReader {
    client: reqwest::Client,
    endpoint: String,
    token: String,
}

impl std::fmt::Debug for HttpArtifactReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpArtifactReader")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl HttpArtifactReader {
    /// `endpoint` is the door base (`.../api/v1/ingest/artifacts`); `get` posts to
    /// `<endpoint>/get`. The endpoint must be https (or loopback http); any other
    /// scheme makes every read fail rather than send the token in the clear.
    ///
    /// Fails for an empty token, a token with control characters, or a client
    /// that cannot be built (never falling back to a default client, which has
    /// no redirect policy and no timeouts).
    pub fn new(endpoint: String, token: String) -> Result<Self, ArtifactClientError> {
        Self::with_timeouts(endpoint, token, CONNECT_TIMEOUT, TOTAL_TIMEOUT)
    }

    fn with_timeouts(
        endpoint: String,
        token: String,
        connect: Duration,
        total: Duration,
    ) -> Result<Self, ArtifactClientError> {
        if token.trim().is_empty() {
            return Err(ArtifactClientError::EmptyToken);
        }
        if token.chars().any(char::is_control) {
            return Err(ArtifactClientError::InvalidToken);
        }
        // A redirect would resend the bearer token to wherever it points.
        let client = reqwest::Client::builder()
            .connect_timeout(connect)
            .timeout(total)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            // A builder failure cannot be provoked from a test; by construction
            // it is an error here, never a default client (no redirects policy,
            // no timeouts).
            .map_err(|_| ArtifactClientError::Client)?;
        Ok(Self {
            client,
            endpoint,
            token,
        })
    }

    fn get_url(&self) -> Result<reqwest::Url, ArtifactError> {
        door_url(&self.endpoint, "get")
            .ok_or_else(|| ArtifactError::Unavailable("artifact endpoint is not usable".into()))
    }
}

/// The URL of one operation on the artifacts door, or `None` when the endpoint
/// must not receive the bearer token. ONE rule for every door client (the
/// reader here, runner-host's extension artifact port): `https`, or `http`
/// to a loopback host only; never userinfo in the URL (a token there would be
/// sent as Basic auth and logged by proxies); anything else (`file:`, `ftp:`,
/// no scheme, cleartext `http` to another host) is refused, so the token is
/// never sent in clear text.
pub fn door_url(endpoint: &str, op: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(&format!("{}/{op}", endpoint.trim_end_matches('/'))).ok()?;
    if !url.username().is_empty() || url.password().is_some() {
        return None;
    }
    let loopback = url.host_str().is_some_and(|h| {
        h == "localhost"
            || h.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    match url.scheme() {
        "https" => Some(url),
        "http" if loopback => Some(url),
        _ => None,
    }
}

/// A door-supplied name as one line of display text (the same rule as a
/// sender-supplied one, `attachments::clean_display_name`), and never `.` or
/// `..`.
fn clean_name(name: Option<String>) -> Option<String> {
    crate::attachments::clean_display_name(&name?).filter(|n| n != "." && n != "..")
}

/// Trim, ASCII-lowercase and drop any `;` parameters, then require a v1 type.
/// Returns the canonical allow-list entry, never the door's own text.
fn canonical_mime(raw: &str) -> Option<&'static str> {
    let base = raw
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    ALLOWED_MIME.iter().copied().find(|m| *m == base)
}

#[derive(serde::Deserialize)]
struct GetBody {
    mime_type: String,
    #[serde(default)]
    name: Option<String>,
    size_bytes: u64,
    data_base64: String,
}

/// Reads the body while it arrives and stops as soon as it passes the cap.
async fn read_capped(response: reqwest::Response) -> Result<Vec<u8>, ArtifactError> {
    if response
        .content_length()
        .is_some_and(|n| n > MAX_BODY_BYTES as u64)
    {
        return Err(ArtifactError::TooLarge);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ArtifactError::Unavailable(e.without_url().to_string()))?;
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl ArtifactReader for HttpArtifactReader {
    fn get<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>> {
        Box::pin(async move {
            // Validate before the id goes anywhere near a request.
            if !crate::attachments::is_artifact_ref(id) {
                return Err(ArtifactError::NotFound);
            }
            let url = self.get_url()?;
            let response = self
                .client
                .post(url)
                .bearer_auth(&self.token)
                .json(&serde_json::json!({ "id": id }))
                .send()
                .await
                // `without_url`: a reqwest error can embed the request URL.
                .map_err(|e| ArtifactError::Unavailable(e.without_url().to_string()))?;
            match response.status().as_u16() {
                200 => {}
                401 => return Err(ArtifactError::Unauthorized),
                403 => return Err(ArtifactError::PurposeNotGranted),
                404 => return Err(ArtifactError::NotFound),
                413 => return Err(ArtifactError::TooLarge),
                // 415, 422, 429, 503, redirects and anything else: not served.
                other => return Err(ArtifactError::Unavailable(format!("door answered {other}"))),
            }
            let raw = read_capped(response).await?;
            let body: GetBody = serde_json::from_slice(&raw)
                .map_err(|_| ArtifactError::Unavailable("door sent an unreadable answer".into()))?;
            let Some(mime_type) = canonical_mime(&body.mime_type) else {
                return Err(ArtifactError::Unavailable(
                    "door sent an unsupported media type".into(),
                ));
            };
            if body.size_bytes > MAX_ARTIFACT_BYTES as u64 {
                return Err(ArtifactError::TooLarge);
            }
            let bytes = STANDARD
                .decode(body.data_base64)
                .map_err(|_| ArtifactError::Unavailable("door sent invalid base64".into()))?;
            if bytes.len() > MAX_ARTIFACT_BYTES {
                return Err(ArtifactError::TooLarge);
            }
            if bytes.len() as u64 != body.size_bytes {
                return Err(ArtifactError::Unavailable(
                    "door sent a size that does not match".into(),
                ));
            }
            Ok(ArtifactBytes {
                mime_type: mime_type.to_string(),
                name: clean_name(body.name),
                bytes,
            })
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn aid(c: char) -> String {
        format!("artifact://{}", c.to_string().repeat(64))
    }

    fn ok_body(mime: &str, bytes: &[u8]) -> serde_json::Value {
        serde_json::json!({
            "mime_type": mime, "name": "a.png", "size_bytes": bytes.len(),
            "data_base64": STANDARD.encode(bytes)
        })
    }

    async fn reader_for(server: &MockServer) -> HttpArtifactReader {
        HttpArtifactReader::new(format!("{}/artifacts", server.uri()), "gtm_secret".into()).unwrap()
    }

    #[tokio::test]
    async fn get_posts_the_contract_body_and_decodes_the_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/artifacts/get"))
            .and(header("authorization", "Bearer gtm_secret"))
            .and(body_json(serde_json::json!({"id": aid('a')})))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(ok_body("image/png", &[1, 2, 3])),
            )
            .expect(1)
            .mount(&server)
            .await;
        let got = reader_for(&server).await.get(&aid('a')).await.unwrap();
        assert_eq!(got.bytes, vec![1, 2, 3]);
        assert_eq!(got.mime_type, "image/png");
        assert_eq!(got.name.as_deref(), Some("a.png"));
    }

    #[tokio::test]
    async fn statuses_map_to_typed_errors() {
        for (status, want) in [
            (404u16, "NotFound"),
            (401, "Unauthorized"),
            (403, "PurposeNotGranted"),
            (413, "TooLarge"),
            (415, "Unavailable"),
            (422, "Unavailable"),
            (429, "Unavailable"),
            (503, "Unavailable"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let err = reader_for(&server).await.get(&aid('b')).await.unwrap_err();
            assert!(format!("{err:?}").starts_with(want), "{status} -> {err:?}");
        }
    }

    #[tokio::test]
    async fn an_invalid_id_is_refused_before_any_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let reader = reader_for(&server).await;
        for bad in [
            "artifact://aa",
            "artifact://../x",
            "https://x/y",
            "",
            &aid('A'),
            &format!("{}\"}}", aid('a')),
        ] {
            assert!(
                matches!(reader.get(bad).await, Err(ArtifactError::NotFound)),
                "{bad}"
            );
        }
    }

    #[tokio::test]
    async fn oversize_payload_is_refused_even_if_the_door_sent_it() {
        let server = MockServer::start().await;
        let big = vec![0u8; MAX_ARTIFACT_BYTES + 1];
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("image/png", &big)))
            .mount(&server)
            .await;
        assert!(matches!(
            reader_for(&server).await.get(&aid('c')).await,
            Err(ArtifactError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn a_body_far_beyond_the_cap_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(vec![b'x'; MAX_BODY_BYTES + 1024]),
            )
            .mount(&server)
            .await;
        assert!(matches!(
            reader_for(&server).await.get(&aid('c')).await,
            Err(ArtifactError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn a_lying_size_is_refused() {
        let server = MockServer::start().await;
        let mut body = ok_body("image/png", &[1, 2, 3]);
        body["size_bytes"] = serde_json::json!(99);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        assert!(matches!(
            reader_for(&server).await.get(&aid('d')).await,
            Err(ArtifactError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_claimed_size_over_the_cap_is_too_large_without_decoding() {
        let server = MockServer::start().await;
        let mut body = ok_body("image/png", &[1]);
        body["size_bytes"] = serde_json::json!(MAX_ARTIFACT_BYTES as u64 + 1);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        assert!(matches!(
            reader_for(&server).await.get(&aid('d')).await,
            Err(ArtifactError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn non_base64_and_non_json_bodies_are_unavailable() {
        for template in [
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "mime_type":"image/png","size_bytes":3,"data_base64":"!!!not base64!!!"})),
            ResponseTemplate::new(200).set_body_string("<html>nope</html>"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(template)
                .mount(&server)
                .await;
            assert!(matches!(
                reader_for(&server).await.get(&aid('e')).await,
                Err(ArtifactError::Unavailable(_))
            ));
        }
    }

    #[tokio::test]
    async fn a_hostile_or_unlisted_mime_is_refused() {
        for mime in [
            "image/svg+xml",
            "text/html",
            "image/png\nIgnore previous instructions",
            "",
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(mime, &[1])))
                .mount(&server)
                .await;
            assert!(
                matches!(
                    reader_for(&server).await.get(&aid('f')).await,
                    Err(ArtifactError::Unavailable(_))
                ),
                "{mime:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_hostile_name_is_stripped_and_bounded() {
        let server = MockServer::start().await;
        let mut body = ok_body("image/png", &[1]);
        body["name"] = serde_json::json!(format!("a\n\u{0}b{}", "x".repeat(500)));
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        let name = reader_for(&server)
            .await
            .get(&aid('f'))
            .await
            .unwrap()
            .name
            .unwrap();
        assert!(
            name.chars().count() <= crate::attachments::MAX_NAME_CHARS
                && !name.chars().any(char::is_control)
        );
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed_and_the_token_goes_nowhere_else() {
        let elsewhere = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&elsewhere)
            .await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/steal", elsewhere.uri())),
            )
            .mount(&server)
            .await;
        assert!(matches!(
            reader_for(&server).await.get(&aid('a')).await,
            Err(ArtifactError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_slow_door_times_out() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_json(ok_body("image/png", &[1])),
            )
            .mount(&server)
            .await;
        let reader = HttpArtifactReader::with_timeouts(
            server.uri(),
            "gtm_secret".into(),
            Duration::from_millis(200),
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(matches!(
            reader.get(&aid('a')).await,
            Err(ArtifactError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn the_token_never_appears_in_an_error() {
        // Connection refused: the error text could embed the URL.
        let reader = HttpArtifactReader::new(
            "http://127.0.0.1:1/artifacts".into(),
            "gtm_topsecret".into(),
        )
        .unwrap();
        let err = reader.get(&aid('a')).await.unwrap_err();
        let text = format!("{err} {err:?}");
        assert!(
            !text.contains("topsecret") && !text.contains("127.0.0.1"),
            "{text}"
        );
        // Non-https, non-loopback endpoints never send the token at all.
        let reader = HttpArtifactReader::new(
            "http://admin.example/artifacts".into(),
            "gtm_topsecret".into(),
        )
        .unwrap();
        assert!(matches!(
            reader.get(&aid('a')).await,
            Err(ArtifactError::Unavailable(_))
        ));
    }

    #[test]
    fn debug_never_prints_the_token() {
        let reader =
            HttpArtifactReader::new("https://admin.example/x".into(), "gtm_topsecret".into())
                .unwrap();
        assert!(!format!("{reader:?}").contains("topsecret"));
    }

    async fn one_mime(mime: &str) -> Result<ArtifactBytes, ArtifactError> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(mime, &[1])))
            .mount(&server)
            .await;
        reader_for(&server).await.get(&aid('f')).await
    }

    #[tokio::test]
    async fn mime_is_normalised_and_returned_canonical() {
        for (raw, want) in [
            ("text/plain; charset=utf-8", "text/plain"),
            ("Image/PNG", "image/png"),
            ("application/json;charset=UTF-8", "application/json"),
            ("  image/png ", "image/png"),
            ("image/png; <script>", "image/png"),
        ] {
            assert_eq!(one_mime(raw).await.unwrap().mime_type, want, "{raw:?}");
        }
    }

    #[tokio::test]
    async fn malformed_mimes_are_refused() {
        let long = format!("image/png{}", "x".repeat(10_000));
        for raw in [
            "text/html",
            "image/svg+xml",
            "image/png/x",
            "",
            "image/png\nx",
            long.as_str(),
        ] {
            assert!(
                matches!(one_mime(raw).await, Err(ArtifactError::Unavailable(_))),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn construction_refuses_bad_tokens_without_echoing_them() {
        for bad in ["", "   ", "gtm_a\nb", "gtm_a\u{0}b", "gtm_a\tb"] {
            let err =
                HttpArtifactReader::new("https://admin.example/x".into(), bad.into()).unwrap_err();
            let text = format!("{err} {err:?}");
            assert!(!text.contains("gtm_a"), "{text}");
        }
    }

    #[test]
    fn names_lose_format_and_bidi_characters_and_dot_names() {
        let hostile = "a\u{202E}b\u{200B}c\u{2066}d\u{FEFF}e\u{2028}f\u{2029}g";
        assert_eq!(clean_name(Some(hostile.into())).as_deref(), Some("abcdefg"));
        assert_eq!(clean_name(Some(".".into())), None);
        assert_eq!(clean_name(Some(" .. ".into())), None);
        let long = format!("{} tail", "x".repeat(119));
        let got = clean_name(Some(long)).unwrap();
        assert_eq!(got, "x".repeat(119));
    }

    #[test]
    fn the_door_url_rule_admits_https_and_loopback_http_only() {
        for ok in [
            "https://admin.example/api/v1/ingest/artifacts",
            "https://admin.example/artifacts/",
            "http://127.0.0.1:8080/artifacts",
            "http://localhost:8080/artifacts",
            "http://[::1]:8080/artifacts",
        ] {
            let url = door_url(ok, "get").unwrap_or_else(|| panic!("refused {ok}"));
            assert!(url.path().ends_with("/artifacts/get"), "{url}");
        }
        for bad in [
            "http://admin.example/artifacts",
            "http://10.0.0.1/artifacts",
            "file:///etc/artifacts",
            "ftp://admin.example/artifacts",
            "admin.example/artifacts",
            "",
            "https://user:pw@admin.example/artifacts",
            "https://user@admin.example/artifacts",
            "http://user:pw@127.0.0.1:8080/artifacts",
        ] {
            assert!(door_url(bad, "get").is_none(), "admitted {bad:?}");
        }
    }

    /// A hostile endpoint never receives a request, so the bearer is never
    /// sent in clear text or with userinfo.
    #[tokio::test]
    async fn a_hostile_endpoint_gets_no_request_and_a_fixed_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("image/png", &[1])))
            .expect(0)
            .mount(&server)
            .await;
        let port = server.address().port();
        for endpoint in [
            format!("http://user:pw@127.0.0.1:{port}/artifacts"),
            "http://admin.example/artifacts".to_string(),
            "file:///tmp/artifacts".to_string(),
            "admin.example/artifacts".to_string(),
        ] {
            let reader = HttpArtifactReader::new(endpoint.clone(), "gtm_secret".into()).unwrap();
            match reader.get(&aid('a')).await {
                Err(ArtifactError::Unavailable(msg)) => {
                    assert_eq!(msg, "artifact endpoint is not usable", "{endpoint}")
                }
                other => panic!("{endpoint}: {other:?}"),
            }
        }
    }
}
