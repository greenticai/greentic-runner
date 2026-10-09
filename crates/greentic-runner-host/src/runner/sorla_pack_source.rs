//! Build the agent loop's [`greentic_aw_runtime::SorlaToolSource`] from the
//! `assets/sorla-routes.json` sidecars of the loaded packs, or — for a
//! caller that already knows which SoRs it wants (the designer's in-process
//! Test chat) — directly from a list of SoR keys.
//!
//! Mirrors `a2a_pack_source::a2a_source_from_packs` in shape: the pack is
//! one source of SoR names, deduplicated first-pack-wins, and the actual
//! address/credential resolution happens per call through each SoR's own
//! route document (`sorla_route::resolve_route`), never through anything
//! read here or baked into the pack.

use std::collections::HashSet;
use std::sync::Arc;

/// Build a worker's sorla tool source from the SoR requirements declared by
/// its loaded packs' `assets/sorla-routes.json` sidecars.
///
/// - `GREENTIC_AW_SORLA_TOOLS=0` disables this path (mirrors
///   [`super::agent_node::aw::sorla_source_from_env`], the
///   `GREENTIC_AW_SORX_URL` development override, which this repo prefers
///   whenever it is set — see the call site in `agent_node.rs`).
/// - `None` when disabled, when no pack is loaded, when no pack declares a
///   SoR, or when no secrets manager was supplied — a sorla route document
///   only ever resolves through one, so without it there is nothing to
///   build.
/// - SoR keys are deduplicated across packs, first pack wins, exactly like
///   [`super::a2a_pack_source::a2a_source_from_packs`]'s agent ids. `packs`
///   is one revision's pack list, so the dedup never mixes units.
pub(crate) async fn sorla_source_from_packs(
    packs: &[Arc<crate::pack::PackRuntime>],
    tenant: &str,
    secrets: Option<crate::secrets::DynSecretsManager>,
    unit: Option<&str>,
) -> Option<Arc<greentic_aw_runtime::SorlaToolSource>> {
    if std::env::var("GREENTIC_AW_SORLA_TOOLS").ok().as_deref() == Some("0") {
        tracing::info!("GREENTIC_AW_SORLA_TOOLS=0; pack-backed sorla tool source disabled");
        return None;
    }
    if packs.is_empty() {
        return None;
    }
    let secrets = secrets?;

    let mut seen = HashSet::new();
    let mut sors = Vec::new();
    for pack in packs {
        let Some(routes) = pack.sorla_routes() else {
            continue;
        };
        for route in routes.iter() {
            if seen.insert(route.sor.clone()) {
                sors.push(route.sor.clone());
            }
        }
    }
    if sors.is_empty() {
        return None;
    }

    sorla_source_for_sors(sors, secrets, tenant, unit).await
}

/// Build a sorla tool source over exactly the SoRs in `sors`, each resolved
/// through its own route document at
/// `secrets://default/<tenant>/<team|_>/sorla/<sor>`.
///
/// This is the seam [`sorla_source_from_packs`] delegates to once it has
/// collected a pack's declared SoRs, and it is also re-exported from the
/// crate root (`greentic_runner_host::sorla_source_for_sors`) for the
/// designer's in-process Test chat, which knows a worker's bound SoRs
/// directly and has no `PackRuntime` to read a sidecar from.
///
/// `None` only for an empty `sors` — there is nothing to discover. Building
/// the invoker itself is infallible: a SoR whose route document is missing
/// or invalid, or whose server is unreachable, is skipped and logged rather
/// than failing the whole source (see
/// [`super::sorx_invoker::routed::SorxRoutedInvoker::discover_all`]).
pub async fn sorla_source_for_sors(
    sors: Vec<String>,
    secrets: crate::secrets::DynSecretsManager,
    tenant: &str,
    unit: Option<&str>,
) -> Option<Arc<greentic_aw_runtime::SorlaToolSource>> {
    if sors.is_empty() {
        return None;
    }
    let requested = sors.len();
    let invoker = crate::runner::sorx_invoker::SorxRoutedInvoker::discover_all(
        sors,
        secrets,
        tenant.to_string(),
        unit.map(str::to_string),
    )
    .await;
    tracing::info!(
        tenant = %tenant,
        sors = requested,
        "sorla tool source constructed from route documents"
    );
    Some(Arc::new(greentic_aw_runtime::SorlaToolSource::new(
        Arc::new(invoker),
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::sorla_source_from_packs;
    use crate::runner::sorla_route::test_secrets;
    use crate::runner::sorx_invoker::fixtures::caps_response_one_business_action;

    /// A pack directory carrying `sidecar` as `assets/sorla-routes.json`.
    fn pack_with_sidecar(sidecar: &str) -> (tempfile::TempDir, Arc<crate::pack::PackRuntime>) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/sorla-routes.json"), sidecar).unwrap();
        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(dir.path()));
        (dir, pack)
    }

    #[allow(unsafe_code)]
    fn clear_gate() {
        // SAFETY: every caller is #[serial] (crate convention).
        unsafe {
            std::env::remove_var("GREENTIC_AW_SORLA_TOOLS");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn returns_none_without_routes() {
        clear_gate();
        assert!(
            sorla_source_from_packs(&[], "acme", None, None)
                .await
                .is_none(),
            "no packs at all"
        );

        let bare = tempfile::tempdir().unwrap();
        let bare_pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(bare.path()));
        let secrets = test_secrets::Handle::default().manager();
        assert!(
            sorla_source_from_packs(
                std::slice::from_ref(&bare_pack),
                "acme",
                Some(secrets),
                None
            )
            .await
            .is_none(),
            "a pack with no sorla-routes.json sidecar"
        );

        let (_dir, empty) = pack_with_sidecar("[]");
        assert!(
            sorla_source_from_packs(&[empty], "acme", None, None)
                .await
                .is_none(),
            "no secrets manager, even with a route declared"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn builds_from_routes() {
        clear_gate();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;

        let (_dir, pack) = pack_with_sidecar(r#"[{"sor":"landlord"}]"#);
        let secrets = test_secrets::Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            &format!(r#"{{"url":"{}","tenant":"acme"}}"#, server.uri()),
        )])
        .manager();

        let source = sorla_source_from_packs(&[pack], "acme", Some(secrets), None)
            .await
            .expect("a source is built");
        let tenant = greentic_aw_runtime::TenantContext::new("acme", "env");
        let catalog = source.catalog(&tenant).await;
        assert!(
            catalog
                .tool_entry("landlord", "record_rent_payment")
                .is_some(),
            "the discovered SoR's business action must be listed"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    async fn sorla_tools_zero_disables_pack_routes_too() {
        let (_dir, pack) = pack_with_sidecar(r#"[{"sor":"landlord"}]"#);
        let secrets = test_secrets::Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            r#"{"url":"http://127.0.0.1:1","tenant":"acme"}"#,
        )])
        .manager();

        // SAFETY: #[serial] serializes env-mutating tests (crate convention).
        unsafe {
            std::env::set_var("GREENTIC_AW_SORLA_TOOLS", "0");
        }
        let gated = sorla_source_from_packs(&[pack], "acme", Some(secrets), None).await;
        clear_gate();
        assert!(
            gated.is_none(),
            "GREENTIC_AW_SORLA_TOOLS=0 must disable the pack-routed source too"
        );
    }
}
