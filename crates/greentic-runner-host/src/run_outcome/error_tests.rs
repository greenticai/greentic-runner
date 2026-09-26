#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn secret_uris_are_removed_whole() {
    let text = "secret missing at secrets://default/acme/_/mcp/crm (and secret://x/y)";
    let out = redact(text);
    assert!(!out.contains("secrets://"), "{out}");
    assert!(!out.contains("secret://"), "{out}");
    assert!(!out.contains("acme/_/mcp"), "{out}");
    assert!(out.contains("[redacted]"), "{out}");
}

#[test]
fn bearer_tokens_and_authorization_values_are_removed() {
    let out = redact("upstream refused Bearer abc.def-123 for the call");
    assert!(!out.contains("abc.def-123"), "{out}");
    assert!(out.contains("Bearer [redacted]"), "{out}");

    let out = redact("request headers: Authorization: Bearer abc\nnext line kept");
    assert!(!out.contains("abc"), "{out}");
    assert!(out.contains("next line kept"), "{out}");

    let out = redact("header authorization=Basic dXNlcjpwYXNz");
    assert!(!out.contains("dXNlcjpwYXNz"), "{out}");
}

#[test]
fn url_queries_and_userinfo_are_removed_but_host_and_path_kept() {
    let out = redact("GET https://user:pw@api.corp.example/v1/items?key=s3cr3t&x=1 failed");
    assert!(
        out.contains("https://[redacted]@api.corp.example/v1/items?[redacted]"),
        "{out}"
    );
    assert!(!out.contains("s3cr3t") && !out.contains("user:pw"), "{out}");
    assert!(out.ends_with(" failed"), "{out}");
}

#[test]
fn credential_pairs_are_removed() {
    for text in [
        r#"body {"access_token": "tok-123", "ok": false}"#,
        "password=hunter2 rejected",
        "api_key: tok-123",
    ] {
        let out = redact(text);
        assert!(
            !out.contains("tok-123") && !out.contains("hunter2"),
            "{out}"
        );
    }
    // A key that merely contains a sensitive word is left alone.
    assert_eq!(redact("secret_missing: no"), "secret_missing: no");
}

#[test]
fn text_without_credentials_is_unchanged() {
    let text = "component http.fetch failed: status 503 from service";
    assert_eq!(redact(text), text);
}

#[test]
fn the_serialised_excerpt_never_carries_a_secret_uri_or_bearer_token() {
    let chain =
        "flow.call failed\ncaused by: http 401 with Bearer abc at secrets://default/acme/_/k";
    let excerpt = ErrorExcerpt::build(
        "01J0000000000000000000000",
        "component_failed",
        Some("crm"),
        2,
        chain,
    );
    let body = serde_json::to_string(&excerpt).unwrap();
    assert!(!body.contains("secrets://"), "{body}");
    assert!(!body.contains("Bearer abc"), "{body}");
    assert!(!body.contains("abc at"), "{body}");
    assert!(body.contains("caused by: "), "{body}");
    assert!(!excerpt.truncated);
}

#[test]
fn the_excerpt_is_capped_with_the_marker() {
    let long = "x".repeat(MAX_EXCERPT_BYTES * 2);
    let excerpt = ErrorExcerpt::build("r", "node_failed", None, 0, &long);
    assert!(excerpt.truncated);
    assert!(excerpt.redacted_excerpt.len() <= MAX_EXCERPT_BYTES);
    assert!(excerpt.redacted_excerpt.ends_with(TRUNCATION_MARKER));

    // Multibyte text is cut on a char boundary.
    let (cut, truncated) = cap("é".repeat(MAX_EXCERPT_BYTES));
    assert!(truncated && cut.len() <= MAX_EXCERPT_BYTES);

    let (kept, truncated) = cap("short".to_string());
    assert_eq!((kept.as_str(), truncated), ("short", false));
}

#[test]
fn the_safe_summary_uses_identifiers_only() {
    assert_eq!(
        safe_summary(Some("crm_lookup"), "timeout", 2),
        "Node `crm_lookup` failed after 2 retries: timeout"
    );
    assert_eq!(
        safe_summary(None, "flow_execution_failed", 1),
        "Flow failed after 1 retry: flow_execution_failed"
    );
    let long = "n".repeat(2000);
    assert!(safe_summary(Some(&long), "timeout", 0).len() <= MAX_SUMMARY_BYTES);

    let excerpt = ErrorExcerpt::build("r", "timeout", Some("a"), 0, "password=hunter2");
    assert!(!excerpt.safe_summary.contains("hunter2"));
}

#[test]
fn chain_text_joins_every_source() {
    let err = anyhow::anyhow!("root cause")
        .context("middle")
        .context("outer");
    assert_eq!(
        anyhow_chain_text(&err),
        "outer\ncaused by: middle\ncaused by: root cause"
    );
    let io = std::io::Error::other("disk");
    assert_eq!(chain_text(&io), "disk");
}
