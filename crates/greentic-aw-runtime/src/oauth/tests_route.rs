use std::sync::Arc;

use super::{
    AuthMode, ClientCredentials, ResolvedAuth, RouteAuthSpec, TokenProvider, parse_auth_mode,
    resolve_route_auth, route_token_key,
};

fn provider() -> Arc<TokenProvider> {
    Arc::new(TokenProvider::new())
}

const CREDS: &str = r#"{"client_id":"cid","client_secret":"csecret-XYZ"}"#;

#[test]
fn absent_bearer_and_header_are_the_static_modes() {
    for raw in [
        None,
        Some(""),
        Some("bearer"),
        Some("header"),
        Some(" bearer "),
    ] {
        assert_eq!(parse_auth_mode(raw).unwrap(), AuthMode::Static, "{raw:?}");
    }
    assert_eq!(
        parse_auth_mode(Some("oauth_client_credentials")).unwrap(),
        AuthMode::OAuthClientCredentials
    );
}

#[test]
fn an_unknown_mode_is_an_error_naming_it() {
    let err = parse_auth_mode(Some("mutual_tls")).unwrap_err();
    assert!(
        err.contains("mutual_tls") && err.contains("oauth_client_credentials"),
        "{err}"
    );
}

#[test]
fn credentials_parse_and_never_echo_the_input() {
    let ok = ClientCredentials::from_secret_bytes(CREDS.as_bytes()).unwrap();
    assert_eq!(ok.client_id, "cid");
    assert!(!format!("{ok:?}").contains("csecret-XYZ"));
    for bad in [
        &b"not json csecret-XYZ"[..],
        br#"{"client_id":"cid"}"#,
        br#"{"client_id":"","client_secret":"x"}"#,
        br#"{"client_id":1,"client_secret":"csecret-XYZ"}"#,
        &[0xff, 0xfe][..],
    ] {
        let err = ClientCredentials::from_secret_bytes(bad).unwrap_err();
        assert!(!err.contains("csecret-XYZ"), "{err}");
    }
}

#[test]
fn a_static_route_keeps_its_secret_as_the_token() {
    let spec = RouteAuthSpec::default();
    let resolved = resolve_route_auth(&spec, Some("tok-1"), "k".into(), provider()).unwrap();
    assert!(matches!(resolved, ResolvedAuth::Static { token: Some(ref t) } if t == "tok-1"));
}

#[test]
fn a_json_secret_on_a_bearer_route_fails_explicitly() {
    for mode in [None, Some("bearer"), Some("header")] {
        let spec = RouteAuthSpec {
            auth_mode: mode,
            ..Default::default()
        };
        let err = resolve_route_auth(&spec, Some(CREDS), "k".into(), provider()).unwrap_err();
        assert!(
            err.contains("client credentials") && err.contains("refusing"),
            "{err}"
        );
        assert!(!err.contains("csecret-XYZ"));
    }
}

#[test]
fn an_oauth_route_needs_an_endpoint_and_a_readable_secret() {
    let spec = RouteAuthSpec {
        auth_mode: Some("oauth_client_credentials"),
        oauth_endpoint: Some("https://auth.example.com/token"),
        oauth_scope: Some("mcp.read"),
        oauth_audience: Some("https://api.example"),
    };
    let resolved = resolve_route_auth(&spec, Some(CREDS), "k".into(), provider()).unwrap();
    let ResolvedAuth::OAuth(route) = resolved else {
        unreachable!("expected an OAuth route");
    };
    assert_eq!(route.key(), "k");
    assert!(!format!("{route:?}").contains("csecret-XYZ"));
    assert!(resolve_route_auth(&spec, None, "k".into(), provider()).is_err());
    assert!(resolve_route_auth(&spec, Some("plain-token"), "k".into(), provider()).is_err());
    let no_endpoint = RouteAuthSpec {
        oauth_endpoint: None,
        ..spec
    };
    assert!(resolve_route_auth(&no_endpoint, Some(CREDS), "k".into(), provider()).is_err());
    let bad_scheme = RouteAuthSpec {
        oauth_endpoint: Some("ftp://x/token"),
        ..spec
    };
    assert!(resolve_route_auth(&bad_scheme, Some(CREDS), "k".into(), provider()).is_err());
}

#[test]
fn an_unknown_mode_fails_the_route_without_reading_the_secret() {
    let spec = RouteAuthSpec {
        auth_mode: Some("future"),
        ..Default::default()
    };
    assert!(resolve_route_auth(&spec, Some("tok"), "k".into(), provider()).is_err());
}

#[test]
fn the_token_key_separates_every_part() {
    let a = route_token_key("mcp", "t", Some("team"), Some("u"), "id");
    let b = route_token_key("mcp", "t", None, Some("u"), "id");
    let c = route_token_key("a2a", "t", Some("team"), Some("u"), "id");
    assert!(a != b && a != c);
    assert_eq!(
        route_token_key("mcp", "t", Some(""), None, "id"),
        route_token_key("mcp", "t", None, Some(""), "id")
    );
}
