//! Multi-tenant context. Mandatory on every public AgentRuntime method
//! so cross-tenant access is a compile error, not a runtime check.

use serde::{Deserialize, Serialize};

/// Identifies the (tenant, environment) pair an agent step runs under.
/// Pass-by-value (cheap clone — two `String`s and one optional).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantContext {
    pub tenant_id: String,
    pub env_id: String,
    /// Optional session user for per-user LLM resolution (host-LLM seam).
    /// `None` for autonomous workers; the tenant-level provider is used then.
    #[serde(default)]
    pub user_email: Option<String>,
    /// Identity of the deployed unit this step's spend belongs to — emitted as
    /// the `project_id` billing dimension.
    ///
    /// The host fills this from the revision's `bundle_id`, which is exactly
    /// what greentic-designer records as `pack_name` in its `published_workers`
    /// join table; matching the two is what lets a product's authoring spend
    /// and runtime spend join.
    ///
    /// Deliberately NOT the agent id: an agent id is the key inside
    /// `manifest.agents` (`"greeter"`, `"assistant"`), which is not unique
    /// across packs — two packs each shipping an `assistant` would collapse
    /// into one bogus project row with summed credits.
    ///
    /// `None` when the identity is genuinely unknown (the tenant-only legacy
    /// pack path, the process-level NATS serve path, graph/tool call sites with
    /// no pack context). Consumers MUST omit the dimension rather than
    /// substitute a fallback or placeholder — cloud-commerce already groups a
    /// missing key under `"unknown"`.
    #[serde(default)]
    pub project_id: Option<String>,
    /// Who the turn is running on behalf of, as established by the SESSION —
    /// never by anything the model produced. See [`VerifiedCaller`].
    ///
    /// `None` on every path with no session identity to establish: autonomous
    /// workers, the process-level serve path, tests. A tool receiving the
    /// stamp built from a `None` here sees `user_verified: false`, which is
    /// the honest answer and the same one an anonymous caller gets.
    #[serde(default)]
    pub caller: Option<VerifiedCaller>,
}

/// The caller a tool may trust, as opposed to the one the model describes.
///
/// ## Why this type exists
///
/// An agent's tool call is a JSON object the LLM composed. Before this, that
/// was the ONLY thing a tool received, so a tool asking "who is calling" was
/// asking the model — which answers whatever the conversation suggests. It
/// really happened: a worker sent `sub: "user@example.com"`, `principals:
/// ["employee"]` for a caller who had sent neither, so the extension on the
/// other side had to ignore identity arguments entirely and serve only public
/// data. There was no channel through which a truthful answer could arrive.
///
/// This is that channel. It is populated by the host from the session and
/// travels beside the model's arguments, never inside them.
///
/// ## What `user_verified` actually asserts
///
/// That the messaging provider which received the turn verified the caller's
/// bearer token. The provider is WASM the operator installed, so it is inside
/// the trust boundary — but the boundary stops there. This flag means "the
/// provider vouched", not "this runtime proved it". A deployment that does not
/// trust its own provider cannot recover trust from this field.
///
/// Everything else is descriptive and only meaningful when `user_verified` is
/// true. A consumer MUST gate on the flag first: the other fields are carried
/// unconditionally so that an unverified caller is distinguishable from a
/// verified one with no groups, rather than being erased into the same shape.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifiedCaller {
    /// Whether the provider verified this caller's credential. `false` for an
    /// anonymous turn, and the default everywhere — a caller is unverified
    /// until something says otherwise, never the reverse.
    #[serde(default)]
    pub user_verified: bool,
    /// Stable subject id from the verified credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    /// Group / principal slugs the credential carries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    /// The team this turn is scoped to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    /// The caller's role, when the credential asserts one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>, env_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            env_id: env_id.into(),
            user_email: None,
            project_id: None,
            caller: None,
        }
    }

    /// Set an optional user email for per-user LLM resolution.
    #[must_use]
    pub fn with_user_email(mut self, email: Option<String>) -> Self {
        self.user_email = email;
        self
    }

    /// Set the deployed unit's identity (the revision `bundle_id`) that billing
    /// reports as `project_id`. Pass `None` when it is genuinely unknown — see
    /// [`TenantContext::project_id`] for why there is no fallback.
    #[must_use]
    pub fn with_project_id(mut self, project_id: Option<String>) -> Self {
        self.project_id = project_id;
        self
    }

    /// Attach the session-established caller. `None` leaves the turn
    /// anonymous, which is what every path with no session identity gets.
    #[must_use]
    pub fn with_caller(mut self, caller: Option<VerifiedCaller>) -> Self {
        self.caller = caller;
        self
    }

    /// The caller a tool may trust, defaulted to anonymous.
    ///
    /// Returns an owned value rather than an `Option` so a call site cannot
    /// accidentally treat "no caller" as "skip the stamp" — an absent caller
    /// must still be stamped, as `user_verified: false`, or a tool cannot tell
    /// an anonymous turn from a runtime too old to say.
    pub fn caller_or_anonymous(&self) -> VerifiedCaller {
        self.caller.clone().unwrap_or_default()
    }

    /// Prefix used by Redis key builders. Returns `aw:{tenant}:{env}`.
    pub fn key_prefix(&self) -> String {
        format!("aw:{}:{}", self.tenant_id, self.env_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_prefix_formats_as_expected() {
        let ctx = TenantContext::new("acme", "prod");
        assert_eq!(ctx.key_prefix(), "aw:acme:prod");
    }

    #[test]
    fn tenant_context_is_eq_and_hashable() {
        let a = TenantContext::new("acme", "prod");
        let b = TenantContext::new("acme", "prod");
        assert_eq!(a, b);
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn user_email_defaults_none_and_builder_sets_it() {
        let a = TenantContext::new("acme", "prod");
        assert_eq!(a.user_email, None);
        let b = TenantContext::new("acme", "prod").with_user_email(Some("u@x.com".into()));
        assert_eq!(b.user_email.as_deref(), Some("u@x.com"));
        // key_prefix is unaffected by user_email
        assert_eq!(b.key_prefix(), "aw:acme:prod");
    }

    #[test]
    fn project_id_defaults_none_and_builder_sets_it() {
        let a = TenantContext::new("acme", "prod");
        assert_eq!(
            a.project_id, None,
            "unknown pack identity must stay None, never a placeholder"
        );
        let b = TenantContext::new("acme", "prod").with_project_id(Some("customer.support".into()));
        assert_eq!(b.project_id.as_deref(), Some("customer.support"));
        // key_prefix is unaffected by project_id — Redis keys must not move.
        assert_eq!(b.key_prefix(), "aw:acme:prod");
    }
}
