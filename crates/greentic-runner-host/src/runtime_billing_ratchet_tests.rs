//! The host's billing meter must reach EVERY agentic-worker builder of a
//! revision runtime. A builder that stops receiving it still compiles and
//! still runs — it just bills nothing, or bills around the credit gate — so
//! this is checked on the source text of `runtime.rs`, the one place the
//! meter is resolved and handed out.

const SOURCE: &str = include_str!("runtime.rs");

/// The text of the call starting at the first `needle`, up to its `.await`.
fn call_text(needle: &str) -> &'static str {
    let start = SOURCE
        .find(needle)
        .unwrap_or_else(|| panic!("runtime.rs no longer calls `{needle}`"));
    let rest = &SOURCE[start..];
    let end = rest
        .find(".await")
        .unwrap_or_else(|| panic!("`{needle}` call has no `.await`"));
    &rest[..end]
}

#[test]
fn load_revision_with_hands_the_hosts_meter_to_the_runtime() {
    let call = call_text("Self::load_revision_impl(\n            args.pack_refs");
    assert!(
        call.contains("options.billing_meter"),
        "load_revision_with must pass the host's meter on: {call}"
    );
}

#[test]
fn the_meter_is_resolved_once_from_the_hosts_choice() {
    assert!(
        SOURCE.contains("resolve_billing_meter(installed_billing_meter)"),
        "the installed meter must be what resolve_billing_meter decides from"
    );
}

#[test]
fn every_agentic_worker_builder_receives_the_resolved_meter() {
    for needle in [
        "build_agent_node_wiring_metered(",
        "build_agent_node_wiring_ephemeral_metered(",
        "build_graph_node_handler_metered(",
        "select_operala_handler_metered(",
    ] {
        let call = call_text(needle);
        assert!(
            call.contains("billing_meter.clone()"),
            "`{needle}` must receive the revision's resolved billing meter: {call}"
        );
    }
}

#[test]
fn every_dw_agent_builder_call_is_the_metered_one() {
    // The unmetered public wrappers resolve `None` and would silently drop the
    // host's meter if runtime.rs called them.
    for unmetered in [
        "agent_node::build_agent_node_wiring(",
        "agent_node::build_agent_node_wiring_ephemeral(",
        "graph_node::build_graph_node_handler(",
        "select_operala_handler_with_tools(",
    ] {
        assert!(
            !SOURCE.contains(unmetered),
            "runtime.rs must not call the unmetered `{unmetered}`"
        );
    }
}
