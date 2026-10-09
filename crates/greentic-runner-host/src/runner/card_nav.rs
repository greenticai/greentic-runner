//! Card-navigation targets that name a FLOW NODE rather than a card asset.
//!
//! An adaptive card's button carries where to go next in
//! `routeToCardId` / `toCardId` / `nextCardId`. Usually that names another card
//! asset and the adaptive-card component renders it. Sometimes it names a
//! **flow node** instead — the pack's own graph continues there.
//!
//! The component prefers an inbound `nextCardId` over its node's configured
//! asset, so a target that names a node makes the first card node in the chain
//! fail with `AC_ASSET_NOT_FOUND`, which reaches the user as a generic service
//! error. The target has to be lifted out of the payload and handed to the
//! engine as the flow's entry node instead.
//!
//! greentic-start already does this for the messaging path it owns. The
//! in-process revision path never did, so the same card journey failed there —
//! navigation to a card worked, navigation to a flow node did not.

use serde_json::Value;

/// Metadata keys an adaptive card uses to say where to go next.
pub(crate) const CARD_NAV_META_KEYS: &[&str] = &["routeToCardId", "toCardId", "nextCardId"];

/// The card-navigation target carried by `metadata`, if any.
fn card_nav_target(metadata: &Value) -> Option<&str> {
    CARD_NAV_META_KEYS
        .iter()
        .find_map(|key| metadata.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|target| !target.is_empty())
}

/// The flow node a card-navigation target names, if it names one.
///
/// Returns `None` when there is no target, or when the target names something
/// that is not a node in this flow — a card asset, typically, which the
/// adaptive-card component must keep receiving so it can render it.
pub(crate) fn entry_node_from_card_nav(
    metadata: &Value,
    flow_node_ids: &[String],
) -> Option<String> {
    let target = card_nav_target(metadata)?;
    flow_node_ids
        .iter()
        .any(|node| node == target)
        .then(|| target.to_string())
}

/// Remove the navigation directives from a payload bound for the flow.
///
/// They say WHERE TO GO, not what to render; leaving them in makes the first
/// card node try to render a card whose id is a node id.
pub(crate) fn strip_card_nav_keys(metadata: &mut Value) {
    let Some(map) = metadata.as_object_mut() else {
        return;
    };
    for key in CARD_NAV_META_KEYS {
        map.remove(*key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn nodes() -> Vec<String> {
        ["welcome", "cap_company_name", "quote_result"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// The meridian journey's failing step: `nextCardId` names a flow node, so
    /// it must become the entry node instead of a card to render.
    #[test]
    fn a_target_naming_a_flow_node_becomes_the_entry_node() {
        let meta = json!({ "nextCardId": "cap_company_name" });
        assert_eq!(
            entry_node_from_card_nav(&meta, &nodes()),
            Some("cap_company_name".to_string())
        );
    }

    /// The journey's working step: `quote_page2` is a card asset, not a node.
    /// It must stay in the payload so the component can render it.
    #[test]
    fn a_target_naming_a_card_is_left_alone() {
        let meta = json!({ "nextCardId": "quote_page2" });
        assert_eq!(entry_node_from_card_nav(&meta, &nodes()), None);
    }

    /// All three keys are honoured, matching the messaging path.
    #[test]
    fn every_nav_key_is_recognised() {
        for key in CARD_NAV_META_KEYS {
            let meta = json!({ *key: "quote_result" });
            assert_eq!(
                entry_node_from_card_nav(&meta, &nodes()),
                Some("quote_result".to_string()),
                "key {key} was not recognised"
            );
        }
    }

    #[test]
    fn an_absent_or_blank_target_yields_nothing() {
        assert_eq!(entry_node_from_card_nav(&json!({}), &nodes()), None);
        assert_eq!(
            entry_node_from_card_nav(&json!({ "nextCardId": "   " }), &nodes()),
            None
        );
    }

    /// Stripping must remove every key, not just the one that matched — a card
    /// can carry more than one and any leftover re-triggers the bug.
    #[test]
    fn stripping_removes_every_nav_key() {
        let mut meta = json!({
            "nextCardId": "cap_company_name",
            "toCardId": "cap_company_name",
            "routeToCardId": "cap_company_name",
            "keep_me": "yes"
        });
        strip_card_nav_keys(&mut meta);
        for key in CARD_NAV_META_KEYS {
            assert!(meta.get(*key).is_none(), "{key} survived stripping");
        }
        assert_eq!(meta.get("keep_me").and_then(|v| v.as_str()), Some("yes"));
    }
}
