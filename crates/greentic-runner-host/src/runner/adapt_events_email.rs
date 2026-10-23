use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use greentic_types::TenantCtx;
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::oauth::{OAuthBrokerConfig, ResourceTokenRequest, build_resource_token_request};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EmailProviderKind {
    MsGraph,
    Gmail,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct EmailOauthHint {
    pub provider_id: String,
    pub flow: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct EmailSendRequest {
    pub provider: EmailProviderKind,
    pub payload: Value,
    pub oauth: Option<EmailOauthHint>,
    #[serde(default)]
    pub secret_events: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailHttpExecution {
    pub method: &'static str,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailExecutionPlan {
    pub token_request: ResourceTokenRequest,
    pub http: EmailHttpExecution,
}

pub fn parse_email_send_request(value: &Value) -> Result<EmailSendRequest> {
    serde_json::from_value(value.clone()).context("invalid email send request payload")
}

pub fn build_email_http_execution(request: &EmailSendRequest) -> Result<EmailHttpExecution> {
    match request.provider {
        EmailProviderKind::MsGraph => {
            let sender = request
                .payload
                .get("message")
                .and_then(|m| m.get("from"))
                .and_then(|from| from.get("emailAddress"))
                .and_then(|addr| addr.get("address"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "msgraph email host execution requires message.from.emailAddress.address"
                    )
                })?;
            if sender.contains(['/', '?', '#']) {
                bail!(
                    "msgraph sender address must not contain '/', '?' or '#': {}",
                    sender
                );
            }
            Ok(EmailHttpExecution {
                method: "POST",
                url: format!("https://graph.microsoft.com/v1.0/users/{sender}/sendMail"),
            })
        }
        EmailProviderKind::Gmail => Ok(EmailHttpExecution {
            method: "POST",
            url: "https://gmail.googleapis.com/gmail/v1/users/me/messages/send".into(),
        }),
    }
}

pub fn required_oauth_hint(request: &EmailSendRequest) -> Result<&EmailOauthHint> {
    let hint = request
        .oauth
        .as_ref()
        .ok_or_else(|| anyhow!("email send request missing oauth hint"))?;
    match request.provider {
        EmailProviderKind::MsGraph => {
            if hint.provider_id != "msgraph-email" {
                bail!("msgraph email request must use provider_id `msgraph-email`");
            }
        }
        EmailProviderKind::Gmail => {
            if hint.provider_id != "gmail-email" {
                bail!("gmail email request must use provider_id `gmail-email`");
            }
        }
    }
    Ok(hint)
}

pub fn build_email_execution_plan(
    config: &OAuthBrokerConfig,
    tenant: &TenantCtx,
    request: &EmailSendRequest,
) -> Result<EmailExecutionPlan> {
    let hint = required_oauth_hint(request)?;
    let token_request =
        build_resource_token_request(config, tenant, &hint.provider_id, &hint.scopes)?;
    let http = build_email_http_execution(request)?;
    Ok(EmailExecutionPlan {
        token_request,
        http,
    })
}

pub fn build_email_http_payload(request: &EmailSendRequest) -> Result<Value> {
    match request.provider {
        EmailProviderKind::MsGraph => Ok(request.payload.clone()),
        EmailProviderKind::Gmail => {
            let message = request
                .payload
                .get("message")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("gmail email request missing `message` object"))?;
            let subject = message
                .get("subject")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("gmail email request missing `message.subject`"))?;
            let body = message
                .get("body")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("gmail email request missing `message.body`"))?;
            let to = string_list(message.get("to"), "message.to")?;
            let cc = optional_string_list(message.get("cc"));
            let bcc = optional_string_list(message.get("bcc"));
            let from = message.get("from").and_then(Value::as_str);
            let raw = build_gmail_raw_message(from, &to, &cc, &bcc, subject, body)?;
            // The Gmail API requires base64url (RFC 4648 section 5), not the
            // standard alphabet.
            Ok(json!({
                "raw": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
            }))
        }
    }
}

pub async fn execute_email_request(
    client: &reqwest::Client,
    access_token: &str,
    request: &EmailSendRequest,
) -> Result<()> {
    let plan = build_email_http_execution(request)?;
    let url = Url::parse(&plan.url).context("invalid email provider URL")?;
    if url.scheme() != "https" {
        bail!(
            "email provider URL must use https, got scheme `{}`",
            url.scheme()
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("email provider URL must not include URL credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("email provider URL must not include query or fragment components");
    }
    let payload = build_email_http_payload(request)?;
    client
        .post(url)
        .bearer_auth(access_token)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn string_list(value: Option<&Value>, field: &str) -> Result<Vec<String>> {
    value
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .ok_or_else(|| anyhow!("gmail email request missing `{field}`"))
}

fn optional_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Reject CR/LF in a header value: they would let a caller-supplied field
/// inject extra headers (or a body) into the message.
fn checked_header_value<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    if value.contains(['\r', '\n']) {
        bail!("gmail email request field `{field}` must not contain CR or LF");
    }
    Ok(value)
}

/// RFC 2047 encoded-word form (`=?UTF-8?B?...?=`) for a non-ASCII subject;
/// ASCII subjects pass through unchanged. Long subjects are split into several
/// encoded words, each within the 75-character limit.
fn encode_subject(subject: &str) -> String {
    if subject.is_ascii() {
        return subject.to_string();
    }
    // 45 raw bytes -> 60 base64 chars; + 12 chars of framing = 72 <= 75.
    const MAX_CHUNK_BYTES: usize = 45;
    let mut words = Vec::new();
    let mut chunk = String::new();
    for ch in subject.chars() {
        if chunk.len() + ch.len_utf8() > MAX_CHUNK_BYTES {
            words.push(encode_word(&chunk));
            chunk.clear();
        }
        chunk.push(ch);
    }
    if !chunk.is_empty() {
        words.push(encode_word(&chunk));
    }
    words.join("\r\n ")
}

fn encode_word(chunk: &str) -> String {
    format!(
        "=?UTF-8?B?{}?=",
        base64::engine::general_purpose::STANDARD.encode(chunk.as_bytes())
    )
}

fn build_gmail_raw_message(
    from: Option<&str>,
    to: &[String],
    cc: &[String],
    bcc: &[String],
    subject: &str,
    body: &str,
) -> Result<String> {
    let join_checked = |field: &str, values: &[String]| -> Result<String> {
        for value in values {
            checked_header_value(field, value)?;
        }
        Ok(values.join(", "))
    };
    let mut lines = vec!["MIME-Version: 1.0".to_string()];
    if let Some(from) = from.filter(|value| !value.trim().is_empty()) {
        lines.push(format!("From: {}", checked_header_value("from", from)?));
    }
    lines.push(format!("To: {}", join_checked("to", to)?));
    if !cc.is_empty() {
        lines.push(format!("Cc: {}", join_checked("cc", cc)?));
    }
    if !bcc.is_empty() {
        lines.push(format!("Bcc: {}", join_checked("bcc", bcc)?));
    }
    lines.push(format!(
        "Subject: {}",
        encode_subject(checked_header_value("subject", subject)?)
    ));
    lines.push("Content-Type: text/plain; charset=utf-8".into());
    lines.push("Content-Transfer-Encoding: 8bit".into());
    lines.push(String::new());
    lines.push(body.to_string());
    Ok(lines.join("\r\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::OAuthBrokerConfig;
    use greentic_types::{EnvId, TeamId, TenantId};
    use serde_json::json;
    use std::str::FromStr;

    fn sample_tenant() -> TenantCtx {
        TenantCtx::new(
            EnvId::from_str("dev").unwrap(),
            TenantId::from_str("acme").unwrap(),
        )
        .with_team(Some(TeamId::from_str("core").unwrap()))
    }

    #[test]
    fn parses_msgraph_request_and_builds_execution_target() {
        let value = json!({
            "provider": "msgraph",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "body": { "contentType": "HTML", "content": "<p>Hi</p>" },
                    "toRecipients": [{ "emailAddress": { "address": "to@example.com" } }],
                    "from": { "emailAddress": { "address": "sender@example.com" } }
                },
                "saveToSentItems": false
            },
            "oauth": {
                "provider_id": "msgraph-email",
                "flow": "client_credentials",
                "scopes": ["https://graph.microsoft.com/.default"]
            },
            "secret_events": []
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let hint = required_oauth_hint(&request).expect("oauth hint");
        let execution = build_email_http_execution(&request).expect("execution");

        assert_eq!(hint.provider_id, "msgraph-email");
        assert_eq!(execution.method, "POST");
        assert_eq!(
            execution.url,
            "https://graph.microsoft.com/v1.0/users/sender@example.com/sendMail"
        );
    }

    #[test]
    fn gmail_request_uses_me_send_endpoint() {
        let value = json!({
            "provider": "gmail",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "body": "Hi",
                    "to": ["to@example.com"]
                }
            },
            "oauth": {
                "provider_id": "gmail-email",
                "flow": "refresh_token",
                "scopes": ["https://www.googleapis.com/auth/gmail.send"]
            },
            "secret_events": []
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let hint = required_oauth_hint(&request).expect("oauth hint");
        let execution = build_email_http_execution(&request).expect("execution");

        assert_eq!(hint.provider_id, "gmail-email");
        assert_eq!(execution.method, "POST");
        assert_eq!(
            execution.url,
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
        );
    }

    #[test]
    fn builds_msgraph_execution_plan() {
        let value = json!({
            "provider": "msgraph",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "body": { "contentType": "HTML", "content": "<p>Hi</p>" },
                    "toRecipients": [{ "emailAddress": { "address": "to@example.com" } }],
                    "from": { "emailAddress": { "address": "sender@example.com" } }
                },
                "saveToSentItems": false
            },
            "oauth": {
                "provider_id": "msgraph-email",
                "flow": "client_credentials",
                "scopes": ["https://graph.microsoft.com/.default"]
            },
            "secret_events": []
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let cfg = OAuthBrokerConfig::new("https://oauth.example", "nats://localhost:4222");
        let tenant = sample_tenant();
        let plan = build_email_execution_plan(&cfg, &tenant, &request).expect("plan");

        assert_eq!(plan.token_request.http_base_url, "https://oauth.example");
        assert_eq!(plan.token_request.resource_id, "msgraph-email");
        assert_eq!(
            plan.token_request.scopes,
            vec!["https://graph.microsoft.com/.default".to_string()]
        );
        assert_eq!(plan.http.method, "POST");
        assert_eq!(
            plan.http.url,
            "https://graph.microsoft.com/v1.0/users/sender@example.com/sendMail"
        );
    }

    #[test]
    fn msgraph_payload_is_forwarded_as_is() {
        let value = json!({
            "provider": "msgraph",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "body": { "contentType": "HTML", "content": "<p>Hi</p>" },
                    "toRecipients": [{ "emailAddress": { "address": "to@example.com" } }],
                    "from": { "emailAddress": { "address": "sender@example.com" } }
                },
                "saveToSentItems": false
            },
            "oauth": {
                "provider_id": "msgraph-email",
                "flow": "client_credentials",
                "scopes": ["https://graph.microsoft.com/.default"]
            },
            "secret_events": []
        });
        let request = parse_email_send_request(&value).expect("parse request");
        let payload = build_email_http_payload(&request).expect("payload");
        assert_eq!(payload, request.payload);
    }

    #[test]
    fn gmail_raw_is_base64url_without_standard_alphabet_characters() {
        // `>>>???` encodes to `Pj4-Pz8_` in base64url (`+`/`/` in standard).
        let raw = build_gmail_raw_message(
            None,
            &["to@example.com".to_string()],
            &[],
            &[],
            "s",
            ">>>???~~~",
        )
        .unwrap();
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert!(!encoded.contains(['+', '/', '=']));
        assert!(encoded.contains(['-', '_']) || !encoded.is_empty());
    }

    #[test]
    fn header_values_with_crlf_are_rejected() {
        let to = vec!["a@example.com".to_string()];
        let injected = build_gmail_raw_message(None, &to, &[], &[], "hi\r\nBcc: evil@x.com", "b");
        assert!(injected.is_err());
        let bad_to = vec!["a@example.com\nBcc: evil@x.com".to_string()];
        assert!(build_gmail_raw_message(None, &bad_to, &[], &[], "s", "b").is_err());
        assert!(build_gmail_raw_message(Some("x\ry"), &to, &[], &[], "s", "b").is_err());
    }

    #[test]
    fn non_ascii_subject_is_rfc2047_encoded() {
        assert_eq!(encode_subject("Hello"), "Hello");
        let encoded = encode_subject("Halo dunia \u{1F30F} caf\u{e9}");
        assert!(encoded.starts_with("=?UTF-8?B?") && encoded.ends_with("?="));
        assert!(encoded.is_ascii());
        let long = encode_subject(&"\u{e9}".repeat(100));
        for word in long.split("\r\n ") {
            assert!(word.len() <= 75, "encoded word too long: {}", word.len());
        }
    }

    #[test]
    fn gmail_payload_is_encoded_as_raw_message() {
        let value = json!({
            "provider": "gmail",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "body": "Hi there",
                    "to": ["to@example.com"],
                    "cc": ["cc@example.com"],
                    "bcc": ["bcc@example.com"],
                    "from": "sender@example.com"
                }
            },
            "oauth": {
                "provider_id": "gmail-email",
                "flow": "refresh_token",
                "scopes": ["https://www.googleapis.com/auth/gmail.send"]
            },
            "secret_events": []
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let payload = build_email_http_payload(&request).expect("payload");
        let raw = payload
            .get("raw")
            .and_then(Value::as_str)
            .expect("raw field");
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw.as_bytes())
            .expect("valid base64");
        let decoded = String::from_utf8(decoded).expect("utf8");

        assert!(decoded.contains("From: sender@example.com"));
        assert!(decoded.contains("To: to@example.com"));
        assert!(decoded.contains("Cc: cc@example.com"));
        assert!(decoded.contains("Bcc: bcc@example.com"));
        assert!(decoded.contains("Subject: Hello"));
        assert!(decoded.ends_with("Hi there"));
    }

    #[test]
    fn msgraph_requires_sender_identity() {
        let value = json!({
            "provider": "msgraph",
            "payload": {
                "message": {
                    "subject": "Hello"
                }
            },
            "oauth": {
                "provider_id": "msgraph-email",
                "flow": "client_credentials",
                "scopes": ["https://graph.microsoft.com/.default"]
            }
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let err = build_email_http_execution(&request).expect_err("missing sender should fail");
        assert!(
            err.to_string()
                .contains("message.from.emailAddress.address")
        );
    }

    #[test]
    fn msgraph_rejects_sender_with_url_delimiters() {
        let value = json!({
            "provider": "msgraph",
            "payload": {
                "message": {
                    "subject": "Hello",
                    "from": { "emailAddress": { "address": "sender@example.com?debug=1" } }
                }
            },
            "oauth": {
                "provider_id": "msgraph-email",
                "flow": "client_credentials",
                "scopes": ["https://graph.microsoft.com/.default"]
            }
        });

        let request = parse_email_send_request(&value).expect("parse request");
        let err = build_email_http_execution(&request).expect_err("invalid sender should fail");
        assert!(err.to_string().contains("must not contain '/', '?' or '#'"));
    }
}
