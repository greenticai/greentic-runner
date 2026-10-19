//! Forwarding the verified caller from a flow `a2a` node (optional key
//! `forward_caller`).
//!
//! With `forward_caller: true` (or the string `"true"`, as for `card`) and a
//! turn whose provider-verified caller block (`FlowContext::caller`, i.e.
//! `extensions.caller` of the inbound envelope) has `user_verified == true`,
//! the A2A `SendMessage` carries a whitelisted copy of it at
//! `message.metadata.caller`:
//!
//! ```json
//! { "sub": "..", "iss": "..?", "teams": [".."], "groups": [".."],
//!   "role": "..?", "user_verified": true }
//! ```
//!
//! Nothing is sent for an absent or unverified caller (never
//! `user_verified: false`), and nothing changes for a node that did not opt
//! in. No credential can enter the block: only the fields above are copied.
//! The value of the block is never logged, only the fact it was forwarded.

use serde_json::{Map, Value};

/// Largest serialized block, in bytes.
pub(crate) const MAX_BLOCK_BYTES: usize = 4096;
/// Largest `teams` / `groups` list.
pub(crate) const MAX_LIST_ITEMS: usize = 64;
/// Longest accepted string, in characters.
pub(crate) const MAX_STRING_CHARS: usize = 256;

/// Whether the node opted in: `forward_caller: true` or the text `"true"`
/// (a templated config value arrives as text). Anything else is off.
#[cfg_attr(not(feature = "agentic-worker"), allow(dead_code))]
pub(crate) fn wants_forward_caller(payload: &Value) -> bool {
    match payload.get("forward_caller") {
        Some(Value::Bool(on)) => *on,
        Some(Value::String(text)) => text.trim().eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// A trimmed, non-empty string within the length limit, else `None`.
fn clean(value: &Value) -> Option<String> {
    let text = value.as_str()?.trim();
    (!text.is_empty() && text.chars().count() <= MAX_STRING_CHARS).then(|| text.to_string())
}

fn clean_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(clean)
                .take(MAX_LIST_ITEMS)
                .collect()
        })
        .unwrap_or_default()
}

fn size(block: &Map<String, Value>) -> usize {
    serde_json::to_vec(block).map_or(usize::MAX, |bytes| bytes.len())
}

/// The block to forward for `caller`, or `None` when nothing may be sent.
///
/// `None` for no caller, a caller whose `user_verified` is not the boolean
/// `true`, and a caller without a usable `sub` (a subject that is blank or
/// over the length limit cannot be truncated into a different identity).
/// A singular `team` stands in for `teams` when `teams` is absent. Fields over
/// the limits are dropped or truncated and a warning (no values) is logged.
pub(crate) fn forwarded_caller(caller: Option<&Value>) -> Option<Value> {
    let caller = caller?.as_object()?;
    if caller.get("user_verified") != Some(&Value::Bool(true)) {
        return None;
    }
    let sub = clean(caller.get("sub")?)?;

    let mut teams = clean_list(caller.get("teams"));
    if caller.get("teams").is_none()
        && let Some(team) = caller.get("team").and_then(clean)
    {
        teams = vec![team];
    }
    let groups = clean_list(caller.get("groups"));
    let declared = |key: &str| {
        caller
            .get(key)
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };
    let mut trimmed = declared("teams") > teams.len() || declared("groups") > groups.len();

    let mut block = Map::new();
    block.insert("sub".into(), Value::String(sub));
    if let Some(iss) = caller.get("iss").and_then(clean) {
        block.insert("iss".into(), Value::String(iss));
    }
    block.insert("teams".into(), json_list(teams));
    block.insert("groups".into(), json_list(groups));
    if let Some(role) = caller.get("role").and_then(clean) {
        block.insert("role".into(), Value::String(role));
    }
    block.insert("user_verified".into(), Value::Bool(true));

    // Over the byte budget: shed the bulkiest optional material first.
    for key in ["groups", "teams", "role", "iss"] {
        if size(&block) <= MAX_BLOCK_BYTES {
            break;
        }
        if key == "groups" || key == "teams" {
            block.insert(key.into(), Value::Array(Vec::new()));
        } else {
            block.remove(key);
        }
        trimmed = true;
    }
    if trimmed {
        tracing::warn!("a2a node trimmed the forwarded caller block to its size limits");
    }
    Some(Value::Object(block))
}

fn json_list(items: Vec<String>) -> Value {
    Value::Array(items.into_iter().map(Value::String).collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn forward_caller_accepts_a_bool_or_the_text_true_and_nothing_else() {
        assert!(wants_forward_caller(&json!({"forward_caller": true})));
        assert!(wants_forward_caller(&json!({"forward_caller": " True "})));
        assert!(!wants_forward_caller(&json!({"forward_caller": false})));
        assert!(!wants_forward_caller(&json!({"forward_caller": "yes"})));
        assert!(!wants_forward_caller(&json!({"forward_caller": 1})));
        assert!(!wants_forward_caller(&json!({})));
    }

    #[test]
    fn no_caller_or_unverified_caller_forwards_nothing() {
        assert!(forwarded_caller(None).is_none());
        assert!(forwarded_caller(Some(&json!({"sub": "u"}))).is_none());
        assert!(forwarded_caller(Some(&json!({"sub": "u", "user_verified": false}))).is_none());
        assert!(forwarded_caller(Some(&json!({"sub": "u", "user_verified": "true"}))).is_none());
        assert!(forwarded_caller(Some(&json!({"user_verified": true}))).is_none());
        assert!(forwarded_caller(Some(&json!("x"))).is_none());
    }

    #[test]
    fn only_the_whitelisted_fields_are_copied() {
        let out = forwarded_caller(Some(&json!({
            "sub": "u-1", "iss": "https://idp", "teams": ["a"], "groups": ["g"],
            "role": "admin", "user_verified": true,
            "token": "secret", "authorization": "Bearer x", "email": "e@x"
        })))
        .unwrap();
        assert_eq!(
            out,
            json!({"sub": "u-1", "iss": "https://idp", "teams": ["a"], "groups": ["g"],
                   "role": "admin", "user_verified": true})
        );
    }

    #[test]
    fn a_singular_team_becomes_teams_only_when_teams_is_absent() {
        let out = forwarded_caller(Some(
            &json!({"sub": "u", "team": "t1", "user_verified": true}),
        ))
        .unwrap();
        assert_eq!(out["teams"], json!(["t1"]));
        let out = forwarded_caller(Some(
            &json!({"sub": "u", "team": "t1", "teams": ["x"], "user_verified": true}),
        ))
        .unwrap();
        assert_eq!(out["teams"], json!(["x"]));
        let out = forwarded_caller(Some(&json!({"sub": "u", "user_verified": true}))).unwrap();
        assert_eq!(out["teams"], json!([]));
        assert_eq!(out["groups"], json!([]));
        assert!(out.get("iss").is_none() && out.get("role").is_none());
    }

    #[test]
    fn lists_and_strings_are_bounded() {
        let many: Vec<String> = (0..100).map(|i| format!("g{i}")).collect();
        let long = "x".repeat(MAX_STRING_CHARS + 1);
        let out = forwarded_caller(Some(&json!({
            "sub": "u", "groups": many, "teams": ["ok", long.clone()],
            "role": long, "user_verified": true
        })))
        .unwrap();
        assert_eq!(out["groups"].as_array().unwrap().len(), MAX_LIST_ITEMS);
        assert_eq!(out["teams"], json!(["ok"]));
        assert!(out.get("role").is_none());
        // A subject over the limit is never truncated into another identity.
        assert!(forwarded_caller(Some(&json!({"sub": long, "user_verified": true}))).is_none());
    }

    #[test]
    fn the_serialized_block_never_exceeds_the_byte_budget() {
        let fat = "y".repeat(MAX_STRING_CHARS);
        let list: Vec<String> = (0..MAX_LIST_ITEMS).map(|_| fat.clone()).collect();
        let out = forwarded_caller(Some(&json!({
            "sub": "u", "iss": fat, "teams": list.clone(), "groups": list,
            "role": fat, "user_verified": true
        })))
        .unwrap();
        assert!(serde_json::to_vec(&out).unwrap().len() <= MAX_BLOCK_BYTES);
        assert_eq!(out["sub"], "u");
        assert_eq!(out["user_verified"], true);
    }
}
