//! The `assets/mcp-routes.json` sidecar a `.gtpack` carries so an `mcp` flow
//! node can be resolved without reaching the tenant's admin.
//!
//! Every failure path must yield `None` (fall back to the admin catalog)
//! rather than an error: a pack that fails to LOAD takes every other node with
//! it, which is far worse than one MCP node reporting an error.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes;

fn pack_with_routes(json: &str) -> Vec<u8> {
    use std::io::Write;
    let mut out = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut out));
        zip.start_file::<_, ()>(
            "assets/mcp-routes.json",
            zip::write::SimpleFileOptions::default(),
        )
        .expect("start entry");
        zip.write_all(json.as_bytes()).expect("write");
        zip.finish().expect("finish");
    }
    out
}

#[test]
fn an_http_route_round_trips_from_the_sidecar() {
    let bytes = pack_with_routes(
        r#"[{"server_id":"srv-1","name":"meridian","transport":"http",
             "transport_url":"https://mcp.example.com/rpc",
             "auth_header_name":"Authorization"}]"#,
    );

    let routes = PackMcpRoutes::from_pack_bytes(&bytes).expect("routes present");
    let route = routes.get("srv-1").expect("srv-1 present");

    assert_eq!(
        route.transport_url.as_deref(),
        Some("https://mcp.example.com/rpc")
    );
    assert_eq!(route.auth_header_name.as_deref(), Some("Authorization"));
}

/// A pack built before this feature has no sidecar. That must read as "fall
/// back to the admin catalog", not as an error and not as "no MCP servers".
#[test]
fn a_pack_without_the_sidecar_yields_none() {
    let empty_pack = {
        let mut out = Vec::new();
        {
            let zip = zip::ZipWriter::new(std::io::Cursor::new(&mut out));
            zip.finish().expect("finish");
        }
        out
    };
    assert!(PackMcpRoutes::from_pack_bytes(&empty_pack).is_none());
}

/// A malformed sidecar must degrade to None with a warning rather than
/// aborting pack load. A pack that fails to load takes every other node with
/// it, which is a far worse failure than one MCP node erroring.
#[test]
fn a_malformed_sidecar_yields_none_rather_than_failing() {
    let bytes = pack_with_routes("{ not an array }");
    assert!(PackMcpRoutes::from_pack_bytes(&bytes).is_none());
}
