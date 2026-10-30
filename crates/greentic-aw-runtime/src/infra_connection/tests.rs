#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const DOC: &str = r#"{"host":"q.example.com","port":6333,"scheme":"https","credential":"S3CRET","extras":{"collection_prefix":"acme__"}}"#;
const KIND: &str = "vector-store/qdrant";

struct MapSecrets {
    entries: HashMap<String, Vec<u8>>,
    seen: Mutex<Vec<String>>,
}

impl MapSecrets {
    fn with(pairs: &[(&str, &str)]) -> Arc<Self> {
        Arc::new(Self {
            entries: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                .collect(),
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl SecretsManager for MapSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.seen.lock().unwrap().push(path.to_string());
        self.entries
            .get(path)
            .cloned()
            .ok_or_else(|| SecretError::NotFound(path.to_string()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

fn doc_with(host: &str) -> String {
    format!(r#"{{"host":"{host}","port":6333,"credential":"c"}}"#)
}

fn field(doc: &str) -> Option<&'static str> {
    match InfraConnection::parse(KIND, doc.as_bytes()) {
        Err(InfraConnectionError::Invalid { field, .. }) => Some(field),
        _ => None,
    }
}

#[test]
fn canonical_name_matches_the_designer_for_both_first_kinds() {
    assert_eq!(canonical_name("vector-store/qdrant"), "vector_store_qdrant");
    assert_eq!(canonical_name("cache/redis"), "cache_redis");
    assert_eq!(canonical_name("vector_store_qdrant"), "vector_store_qdrant");
}

#[test]
fn uri_candidates_pin_env_default_team_underscore_and_try_the_served_tenant_first() {
    assert_eq!(
        uri_candidates("acme", KIND).unwrap(),
        vec![
            "secrets://default/acme/_/infra/vector_store_qdrant".to_string(),
            "secrets://default/default/_/infra/vector_store_qdrant".to_string(),
        ]
    );
    assert_eq!(uri_candidates("default", KIND).unwrap().len(), 1);
}

#[test]
fn uri_candidates_canonicalise_a_non_canonical_kind() {
    assert_eq!(
        uri_candidates("acme", "Vector-Store.Qdrant").unwrap(),
        uri_candidates("acme", KIND).unwrap()
    );
}

#[test]
fn kinds_that_canonicalise_to_the_placeholder_are_refused() {
    for bad in ["", "***", "..", "  "] {
        assert!(
            matches!(
                uri_candidates("acme", bad),
                Err(InfraConnectionError::BadKind(_))
            ),
            "{bad:?}"
        );
    }
    // A kind that really is "secret" stays addressable.
    assert!(uri_candidates("acme", "secret").is_ok());
}

#[test]
fn is_infra_uri_matches_the_first_segment_case_insensitively() {
    for yes in [
        "secret://infra",
        "secret://infra/",
        "secret://Infra/x",
        "secret://INFRA/a/b",
        "secret://infra/x",
    ] {
        assert!(is_infra_uri(yes), "{yes}");
    }
    for no in [
        "secret://infrastructure/x",
        "secret://tavily/k",
        "secrets://default/x/_/infra/y",
        "infra/x",
    ] {
        assert!(!is_infra_uri(no), "{no}");
    }
}

#[test]
fn version_zero_is_refused() {
    assert_eq!(
        field(r#"{"v":0,"host":"h","port":1,"credential":"c"}"#),
        Some("v")
    );
}

#[test]
fn base_url_lowercases_the_scheme() {
    let c = InfraConnection::parse(
        KIND,
        br#"{"host":"h","port":1,"scheme":"HTTPS","credential":"c"}"#,
    )
    .unwrap();
    assert_eq!(c.base_url(), "https://h:1");
}

#[test]
fn bad_kinds_are_refused() {
    assert!(matches!(
        uri_candidates("", "").map(|_| ()),
        Err(InfraConnectionError::BadKind(_))
    ));
    assert!(matches!(
        uri_candidates("acme", "  "),
        Err(InfraConnectionError::BadKind(_))
    ));
}

#[test]
fn parses_the_exact_document_the_designer_stages() {
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    assert_eq!(c.host(), "q.example.com");
    assert_eq!(c.port(), 6333);
    assert_eq!(c.scheme(), "https");
    assert_eq!(c.base_url(), "https://q.example.com:6333");
    assert_eq!(c.extra("collection_prefix"), Some("acme__"));
    assert_eq!(c.credential().expose_secret(), "S3CRET");
}

#[test]
fn an_ipv6_host_is_bracketed_only_in_base_url() {
    let c = InfraConnection::parse(KIND, doc_with("2001:db8::1").as_bytes()).unwrap();
    assert_eq!(c.host(), "2001:db8::1");
    assert_eq!(c.base_url(), "https://[2001:db8::1]:6333");
}

#[test]
fn a_version_above_one_is_refused_by_name() {
    let v2 = r#"{"v":2,"host":"h","port":1,"credential":"c"}"#;
    assert_eq!(
        InfraConnection::parse(KIND, v2.as_bytes()).unwrap_err(),
        InfraConnectionError::UnsupportedVersion {
            kind: KIND.into(),
            found: 2,
            supported: 1
        }
    );
    let v1 = r#"{"v":1,"host":"h","port":1,"credential":"c"}"#;
    assert!(InfraConnection::parse(KIND, v1.as_bytes()).is_ok());
}

#[test]
fn unknown_top_level_fields_are_tolerated_and_not_echoed() {
    let doc = r#"{"host":"h","port":1,"credential":"c","future":"X"}"#;
    let c = InfraConnection::parse(KIND, doc.as_bytes()).unwrap();
    let out = c.to_guest_document(KIND);
    assert!(!out.expose_secret().contains("future"));
}

#[test]
fn host_rules_mirror_the_admin() {
    for bad in [
        "",
        &"a".repeat(254),
        "a..b",
        "-a",
        "a-",
        "h:80",
        "https://h",
        "u@h",
        "a b",
    ] {
        assert_eq!(field(&doc_with(bad)), Some("host"), "host {bad:?}");
    }
    for good in ["1.2.3.4", "2001:db8::1", "q.example.com"] {
        assert_eq!(field(&doc_with(good)), None, "host {good:?}");
    }
}

#[test]
fn port_zero_and_non_numbers_are_refused() {
    assert_eq!(
        field(r#"{"host":"h","port":0,"credential":"c"}"#),
        Some("port")
    );
    assert_eq!(
        field(r#"{"host":"h","port":"6333","credential":"c"}"#),
        Some("port")
    );
    assert_eq!(
        field(r#"{"host":"h","port":65536,"credential":"c"}"#),
        Some("port")
    );
}

#[test]
fn scheme_must_be_alphanumeric_and_short() {
    let with = |s: &str| format!(r#"{{"host":"h","port":1,"scheme":"{s}","credential":"c"}}"#);
    assert_eq!(field(&with("ht tp")), Some("scheme"));
    assert_eq!(field(&with(&"a".repeat(17))), Some("scheme"));
    let c = InfraConnection::parse(KIND, br#"{"host":"h","port":1,"credential":"c"}"#).unwrap();
    assert_eq!(c.scheme(), "https");
}

#[test]
fn credential_bounds() {
    let with = |c: &str| format!(r#"{{"host":"h","port":1,"credential":"{c}"}}"#);
    assert_eq!(field(&with("")), Some("credential"));
    assert_eq!(field(&with(&"x".repeat(4097))), Some("credential"));
    assert_eq!(field(&with(&"x".repeat(4096))), None);
}

#[test]
fn extras_bounds() {
    let many: Vec<String> = (0..17).map(|i| format!(r#""k{i}":"v""#)).collect();
    let d = format!(
        r#"{{"host":"h","port":1,"credential":"c","extras":{{{}}}}}"#,
        many.join(",")
    );
    assert_eq!(field(&d), Some("extras"));
    let long = format!(
        r#"{{"host":"h","port":1,"credential":"c","extras":{{"k":"{}"}}}}"#,
        "v".repeat(257)
    );
    assert_eq!(field(&long), Some("extras"));
    assert_eq!(
        field(r#"{"host":"h","port":1,"credential":"c","extras":{"k":5}}"#),
        Some("extras")
    );
}

#[test]
fn debug_never_shows_the_credential() {
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    let s = format!("{c:?}");
    assert!(!s.contains("S3CRET"));
    assert!(s.contains("<redacted>"));
}

#[test]
fn errors_never_contain_a_value() {
    // This case exercises the typed-field path (port has the wrong JSON type);
    // the `garbage` case below exercises the serde_json parse-error path, whose
    // message would otherwise echo input.
    let doc = r#"{"host":"h","port":"S3CRET-IN-A-WRONG-TYPE","credential":"S3CRET"}"#;
    let err = InfraConnection::parse(KIND, doc.as_bytes()).unwrap_err();
    for s in [err.to_string(), format!("{err:?}")] {
        assert!(!s.contains("S3CRET"), "leaked: {s}");
    }
    let garbage = InfraConnection::parse(KIND, b"{\"credential\":\"S3CRET\" nope").unwrap_err();
    for s in [garbage.to_string(), format!("{garbage:?}")] {
        assert!(!s.contains("S3CRET"), "leaked: {s}");
    }
}

#[tokio::test]
async fn missing_entry_names_the_kind_and_both_uris_and_no_value() {
    let secrets = MapSecrets::with(&[]);
    let err = read(secrets.as_ref(), "acme", KIND).await.unwrap_err();
    assert!(matches!(err, InfraConnectionError::Missing { .. }));
    let msg = err.to_string();
    assert!(msg.contains(KIND), "{msg}");
    assert!(msg.contains("secrets://default/acme/_/infra/vector_store_qdrant"));
    assert!(msg.contains("secrets://default/default/_/infra/vector_store_qdrant"));
    assert!(msg.contains("deploy again"));
    // Matched verbatim by the Qdrant extension's `classify_host_error`
    // (component-rag-qdrant-ext-infra). Changing it breaks that consumer.
    assert!(msg.contains("is not connected to this deployment"), "{msg}");
}

struct BrokenSecrets;

#[async_trait]
impl SecretsManager for BrokenSecrets {
    async fn read(&self, _: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        Err(SecretError::Backend("S3CRET store down".into()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_store_error_is_unreadable_not_missing_and_leaks_nothing() {
    let err = read(&BrokenSecrets, "acme", KIND).await.unwrap_err();
    assert!(
        matches!(err, InfraConnectionError::Unreadable { .. }),
        "{err:?}"
    );
    assert!(!err.to_string().contains("S3CRET"));
}

#[test]
fn malformed_uri_message_is_bounded_and_quoted() {
    let long = format!("secret://infra/a/{}", "x".repeat(5000));
    let msg = malformed_uri_message(&long);
    assert!(msg.len() < 600, "{}", msg.len());
    assert!(msg.contains("\"secret://infra/a/"), "{msg}");
}

#[test]
fn the_guest_receives_the_canonical_kind() {
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    let v: Value =
        serde_json::from_str(c.to_guest_document("Vector-Store.Qdrant").expose_secret()).unwrap();
    assert_eq!(v["kind"], "vector_store_qdrant");
}

#[tokio::test]
async fn read_prefers_the_served_tenant_then_default() {
    let default_only =
        MapSecrets::with(&[("secrets://default/default/_/infra/vector_store_qdrant", DOC)]);
    let c = read(default_only.as_ref(), "acme", KIND).await.unwrap();
    assert_eq!(c.host(), "q.example.com");
    assert_eq!(
        *default_only.seen.lock().unwrap(),
        vec![
            "secrets://default/acme/_/infra/vector_store_qdrant".to_string(),
            "secrets://default/default/_/infra/vector_store_qdrant".to_string()
        ]
    );
    let acme_doc = r#"{"host":"acme.example.com","port":1,"credential":"c"}"#;
    let both = MapSecrets::with(&[
        (
            "secrets://default/acme/_/infra/vector_store_qdrant",
            acme_doc,
        ),
        ("secrets://default/default/_/infra/vector_store_qdrant", DOC),
    ]);
    assert_eq!(
        read(both.as_ref(), "acme", KIND).await.unwrap().host(),
        "acme.example.com"
    );
}

#[test]
fn require_extra_fails_closed() {
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    assert_eq!(c.require_extra("collection_prefix").unwrap(), "acme__");
    assert_eq!(
        c.require_extra("key_prefix").unwrap_err(),
        InfraConnectionError::MissingExtra {
            kind: KIND.into(),
            key: "key_prefix".into()
        }
    );
    let blank = r#"{"host":"h","port":1,"credential":"c","extras":{"key_prefix":"  "}}"#;
    let c = InfraConnection::parse(KIND, blank.as_bytes()).unwrap();
    assert!(c.require_extra("key_prefix").is_err());
}

#[test]
fn guest_document_matches_the_golden_fixture() {
    // A guest only ever receives the canonical name as `kind`.
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    let got: Value =
        serde_json::from_str(c.to_guest_document(&canonical_name(KIND)).expose_secret()).unwrap();
    let want: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/infra_connection_v1.json"
    ))
    .unwrap();
    assert_eq!(got, want);
}

#[test]
fn guest_document_round_trips_through_parse() {
    let c = InfraConnection::parse(KIND, DOC.as_bytes()).unwrap();
    let doc = c.to_guest_document(KIND);
    let again = InfraConnection::parse(KIND, doc.expose_secret().as_bytes()).unwrap();
    assert_eq!(again.base_url(), c.base_url());
    assert_eq!(again.extra("collection_prefix"), Some("acme__"));
}
