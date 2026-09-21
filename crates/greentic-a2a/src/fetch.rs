//! Retrieving an agent card from its well-known location, with a TTL cache.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::card::AgentCard;

/// Where an agent publishes its card. `/.well-known/agent.json` is the 0.x
/// path and is superseded; this one has an IANA registration template.
pub const WELL_KNOWN_PATH: &str = "/.well-known/agent-card.json";

#[derive(Debug, thiserror::Error)]
pub enum A2aError {
    #[error("agent base URL must be https, got {base}")]
    InsecureBase { base: String },
    #[error("agent base URL is not a URL: {base}")]
    BadBase { base: String },
    #[error("fetching the agent card failed: {0}")]
    Transport(String),
    #[error("the agent card at {url} did not parse: {source}")]
    Malformed {
        url: String,
        source: serde_json::Error,
    },
}

/// Build the card URL for an agent base, refusing plaintext.
///
/// Refused rather than warned about: the card names the address we will later
/// send a credential to, so trusting one fetched over plaintext would hand an
/// on-path attacker the agent's endpoint.
pub fn card_url(base: &str) -> Result<String, A2aError> {
    let parsed = url::Url::parse(base).map_err(|_| A2aError::BadBase {
        base: base.to_string(),
    })?;
    if parsed.scheme() != "https" {
        return Err(A2aError::InsecureBase {
            base: base.to_string(),
        });
    }
    // `join` re-roots at the origin rather than concatenating the base's raw
    // text, so a base carrying a path, query or fragment (`https://a.com/x?y=1`)
    // still resolves to the well-known location instead of appending onto it.
    let card = parsed
        .join(WELL_KNOWN_PATH)
        .map_err(|_| A2aError::BadBase {
            base: base.to_string(),
        })?;
    Ok(card.to_string())
}

struct Entry {
    card: Arc<AgentCard>,
    fetched_at: Instant,
}

/// A TTL cache over agent cards.
///
/// The spec asks servers to send `Cache-Control` and `ETag` (§8.6); honouring
/// those is a later refinement. A flat TTL is enough for C2, whose cost we are
/// avoiding is an HTTP round trip inside the agent loop.
pub struct CardCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, Entry>>,
    client: reqwest::Client,
}

impl CardCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
            // Redirects are refused outright rather than merely limited: a
            // plaintext downgrade (`https://` -> `http://`) arrives as a
            // redirect, and no legitimate well-known card location needs one.
            // A timeout is set for the same reason `card_url` refuses
            // plaintext — this fetch can run inside an agent loop, and a hung
            // agent must not block its caller forever.
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("a client with no redirect policy and a timeout is always buildable"),
        }
    }

    /// Fetch the card for an agent base, or serve it from cache.
    ///
    /// This is the entry point, and it is what enforces the https rule: it
    /// builds the URL through [`card_url`] before ever handing it to the
    /// internal fetch below.
    pub async fn get(&self, base: &str) -> Result<Arc<AgentCard>, A2aError> {
        self.get_from_url(&card_url(base)?).await
    }

    /// The internal fetch, by an already-built URL.
    ///
    /// This is the one place the https rule can be bypassed — pass it a
    /// `http://` URL directly and it will fetch over plaintext — which is
    /// exactly why it stays private. `mod tests` below is a child of this
    /// module, so it can still reach this function to exercise the cache
    /// against a loopback HTTP server without [`card_url`]'s refusal ever
    /// being weakened for an external caller.
    async fn get_from_url(&self, url: &str) -> Result<Arc<AgentCard>, A2aError> {
        if let Some(hit) = self.cached(url) {
            return Ok(hit);
        }
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|err| A2aError::Transport(err.to_string()))?;
        if !response.status().is_success() {
            return Err(A2aError::Transport(format!(
                "unexpected status {}",
                response.status()
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|err| A2aError::Transport(err.to_string()))?;
        let card: AgentCard =
            serde_json::from_str(&body).map_err(|source| A2aError::Malformed {
                url: url.to_string(),
                source,
            })?;
        let card = Arc::new(card);
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                url.to_string(),
                Entry {
                    card: Arc::clone(&card),
                    fetched_at: Instant::now(),
                },
            );
        }
        Ok(card)
    }

    fn cached(&self, url: &str) -> Option<Arc<AgentCard>> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(url)?;
        (entry.fetched_at.elapsed() < self.ttl).then(|| Arc::clone(&entry.card))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn the_well_known_path_is_the_v1_one_not_the_superseded_agent_json() {
        // `/.well-known/agent.json` is the 0.x path and is superseded.
        assert_eq!(WELL_KNOWN_PATH, "/.well-known/agent-card.json");
    }

    #[test]
    fn a_card_url_is_built_from_a_base_without_doubling_the_slash() {
        assert_eq!(
            card_url("https://api.example.com").unwrap(),
            "https://api.example.com/.well-known/agent-card.json"
        );
        assert_eq!(
            card_url("https://api.example.com/").unwrap(),
            "https://api.example.com/.well-known/agent-card.json"
        );
    }

    #[test]
    fn a_non_https_base_is_refused() {
        // The card carries the address we will send a credential to, so a
        // plaintext base is refused rather than warned about.
        assert!(matches!(
            card_url("http://api.example.com"),
            Err(A2aError::InsecureBase { .. })
        ));
    }

    #[test]
    fn a_base_with_a_path_still_resolves_to_the_well_known_location() {
        // String concatenation would have produced
        // ".../foo/bar/.well-known/agent-card.json"; the well-known location
        // is origin-rooted, so a base's own path must be discarded.
        assert_eq!(
            card_url("https://api.example.com/foo/bar").unwrap(),
            "https://api.example.com/.well-known/agent-card.json"
        );
    }

    #[test]
    fn a_base_with_a_query_still_resolves_to_the_well_known_location() {
        // String concatenation would have produced
        // ".../?x=1/.well-known/agent-card.json"; the query must not survive.
        assert_eq!(
            card_url("https://api.example.com/?x=1").unwrap(),
            "https://api.example.com/.well-known/agent-card.json"
        );
    }

    #[tokio::test]
    async fn a_served_card_is_fetched_once_and_then_cached() {
        let hits = Arc::new(AtomicUsize::new(0));
        let base = spawn_card_server(Arc::clone(&hits));
        let url = format!("{base}{WELL_KNOWN_PATH}");

        let cache = CardCache::new(Duration::from_secs(300));
        let first = cache.get_from_url(&url).await.expect("first fetch");
        let second = cache.get_from_url(&url).await.expect("cached");

        assert_eq!(first.name, "Recipe Agent");
        assert_eq!(second.name, "Recipe Agent");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the second get must not reach the network"
        );
    }

    // --- test server -------------------------------------------------------

    const CARD_JSON: &str = r#"{
      "name": "Recipe Agent",
      "description": "Helps with recipes and cooking.",
      "version": "1.0.0",
      "supportedInterfaces": [
        { "url": "https://api.example.com/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
      ],
      "capabilities": { "streaming": false, "pushNotifications": false },
      "defaultInputModes": ["text/plain"],
      "defaultOutputModes": ["text/plain"],
      "skills": [
        { "id": "suggest", "name": "Suggest a recipe", "description": "Suggests a dish.", "tags": ["cooking"] }
      ]
    }"#;

    /// Serve `CARD_JSON` on an ephemeral loopback port, counting requests, and
    /// return the base URL. The thread ends with the test process.
    fn spawn_card_server(hits: Arc<AtomicUsize>) -> String {
        let server = tiny_http::Server::http("127.0.0.1:0").expect("bind loopback");
        let port = server.server_addr().to_ip().expect("an ip addr").port();
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                hits.fetch_add(1, Ordering::SeqCst);
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .expect("a valid header");
                let response = tiny_http::Response::from_string(CARD_JSON).with_header(header);
                let _ = request.respond(response);
            }
        });
        format!("http://127.0.0.1:{port}")
    }
}
