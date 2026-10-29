use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use tempfile::TempPath;

mod azblob;
mod fs;
mod gcs;
mod http;
mod oci;
mod s3;

pub use azblob::AzBlobResolver;
pub use fs::FsResolver;
pub use gcs::GcsResolver;
pub use http::HttpResolver;
pub use oci::OciResolver;
pub use s3::S3Resolver;

/// Response from a resolver indicating where the artifact was stored.
pub struct FetchResponse {
    location: FetchLocation,
}

impl FetchResponse {
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            location: FetchLocation::Permanent(path),
        }
    }

    pub fn from_temp(path: TempPath) -> Self {
        Self {
            location: FetchLocation::Temporary(path),
        }
    }

    pub fn path(&self) -> &Path {
        match &self.location {
            FetchLocation::Permanent(path) => path,
            FetchLocation::Temporary(path) => path.as_ref(),
        }
    }
}

enum FetchLocation {
    Permanent(PathBuf),
    Temporary(TempPath),
}

/// Largest pack download accepted, in MiB. Override with
/// `GREENTIC_PACK_MAX_DOWNLOAD_MB`; `0` disables the cap.
pub(crate) const DEFAULT_MAX_PACK_DOWNLOAD_MB: u64 = 1024;
/// Largest pack index document accepted, in MiB. Override with
/// `GREENTIC_PACK_INDEX_MAX_MB`; `0` disables the cap.
pub(crate) const DEFAULT_MAX_INDEX_DOWNLOAD_MB: u64 = 32;
/// Whole-request deadline for a download when none is configured, in seconds.
/// Override with `GREENTIC_PACK_DOWNLOAD_TIMEOUT_SECS`; `0` disables it.
pub(crate) const DEFAULT_DOWNLOAD_TIMEOUT_SECS: u64 = 900;

/// A byte cap read from `var` (MiB), or `default_mb` when unset or not a
/// number. `None` means uncapped (`0`).
pub(crate) fn env_cap_bytes(var: &str, default_mb: u64) -> Option<u64> {
    let mb = std::env::var(var)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default_mb);
    (mb != 0).then(|| mb.saturating_mul(1024 * 1024))
}

/// A pack locator that is safe to put in an error chain, log line or admin
/// response. A locator may be a presigned S3 URL or an Azure SAS URL, whose
/// credential rides in the userinfo or the query/fragment: both are replaced,
/// the scheme, host and path are kept so the failure is still diagnosable.
pub fn redact_locator(locator: &str) -> String {
    if !locator.contains("://") {
        return locator.to_string();
    }
    // A string that merely contains `://` without a valid leading scheme
    // (`x/bot1:TOKEN?r=http://a`) is not a locator we can reason about.
    let Some(parts) = crate::url_split::split_url(locator) else {
        return "<invalid-locator>".to_string();
    };
    if parts.ambiguous {
        // An `@` after the authority: a password with a raw `/?#`. No host shown.
        return format!("{}[redacted]", parts.scheme_prefix);
    }
    let host = if parts.had_userinfo {
        format!("[redacted]@{}", parts.host)
    } else {
        parts.host.to_string()
    };
    let tail = parts.remainder;
    let path_end = tail.find(['?', '#']).unwrap_or(tail.len());
    let (path, query) = tail.split_at(path_end);
    if query.is_empty() {
        format!("{}{host}{path}", parts.scheme_prefix)
    } else {
        format!("{}{host}{path}?[redacted]", parts.scheme_prefix)
    }
}

/// Drop reqwest's `" for url (<url>)"` Display suffix from one error message.
/// The URL may be a presigned S3 / Azure SAS locator, so its query must never
/// reach an error chain.
///
/// The URL is never parsed for balanced parentheses (a key may hold a lone `(`
/// or `)` that no counting can tell from the closing one). Instead the URL ends
/// at the first `"): "` (reqwest's own separator before a chained reason), else
/// at a `)` that closes the whole message. A URL is not allowed a raw space, so
/// `"): "` cannot occur inside it. When neither is found the end is unknown and
/// everything from the suffix start on is dropped (fail-closed). Further
/// `" for url ("` suffixes in the remainder are removed the same way.
pub(crate) fn strip_url_suffix(message: &str) -> String {
    const MARKER: &str = " for url (";
    let Some(start) = message.find(MARKER) else {
        return message.to_string();
    };
    let tail = &message[start + MARKER.len()..];
    let rest = if let Some(end) = tail.find("): ") {
        Some(&tail[end + 1..])
    } else if tail.ends_with(')') {
        Some("")
    } else {
        None
    };
    match rest {
        Some(rest) => format!("{}{}", &message[..start], strip_url_suffix(rest)),
        None => message[..start].to_string(),
    }
}

/// The reason an I/O error gives, with every `" for url (...)"` suffix removed.
///
/// A body read that fails mid-stream surfaces as an `io::Error` WRAPPING a
/// `reqwest::Error`; going through `?` would keep the whole chain, and the
/// wrapped error's Display carries the full request URL. The chain is flattened
/// here to text instead.
pub(crate) fn body_read_reason(error: &std::io::Error) -> String {
    let mut parts: Vec<String> = vec![strip_url_suffix(&error.to_string())];
    let mut source = std::error::Error::source(error);
    while let Some(err) = source {
        let text = strip_url_suffix(&err.to_string());
        // `io::Error::other(e)` repeats `e` as both Display and source.
        if parts.last() != Some(&text) {
            parts.push(text);
        }
        source = err.source();
    }
    parts.join(": ")
}

/// Copy `reader` into `writer`, failing as soon as more than `cap` bytes
/// arrive. Returns the number of bytes copied. A read/write error is reported
/// by reason only (see [`body_read_reason`]), never with the request URL.
pub(crate) fn copy_capped<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
    cap: Option<u64>,
    what: &str,
) -> Result<u64> {
    use std::io::Read as _;
    let copy_failed = |error: std::io::Error| {
        anyhow::anyhow!("failed to copy {what}: {}", body_read_reason(&error))
    };
    let Some(cap) = cap else {
        return std::io::copy(reader, writer).map_err(copy_failed);
    };
    // Read one byte past the cap so "exactly cap" passes and "cap + 1" fails.
    let copied = std::io::copy(&mut reader.by_ref().take(cap.saturating_add(1)), writer)
        .map_err(copy_failed)?;
    if copied > cap {
        bail!("{what} exceeds the {cap} byte download limit");
    }
    Ok(copied)
}

pub trait PackResolver: Send + Sync {
    fn scheme(&self) -> &'static str;
    fn fetch(&self, locator: &str) -> Result<FetchResponse>;
}

#[derive(Default)]
pub struct ResolverRegistry {
    resolvers: HashMap<String, Arc<dyn PackResolver>>,
}

impl ResolverRegistry {
    pub fn register(&mut self, resolver: impl PackResolver + 'static) {
        self.resolvers
            .insert(resolver.scheme().to_string(), Arc::new(resolver));
    }

    pub fn register_builtin(
        &mut self,
        fs_root: PathBuf,
        network: Option<&greentic_config_types::NetworkConfig>,
    ) -> Result<()> {
        self.register_builtin_confined(fs_root, network, None)
    }

    /// Like [`Self::register_builtin`], but when `fs_allowed_roots` is `Some`
    /// the filesystem resolver rejects absolute locators outside `fs_root` and
    /// the listed roots.
    pub fn register_builtin_confined(
        &mut self,
        fs_root: PathBuf,
        network: Option<&greentic_config_types::NetworkConfig>,
        fs_allowed_roots: Option<Vec<PathBuf>>,
    ) -> Result<()> {
        let mut fs_resolver = FsResolver::new(fs_root);
        if let Some(roots) = fs_allowed_roots {
            fs_resolver = fs_resolver.with_allowed_roots(roots);
        }
        self.register(fs_resolver);
        self.register(HttpResolver::new("http", network)?);
        self.register(HttpResolver::new("https", network)?);
        self.register(OciResolver::new(network)?);
        self.register(S3Resolver::new(network)?);
        self.register(GcsResolver::new(network)?);
        self.register(AzBlobResolver::new(network)?);
        Ok(())
    }

    pub fn fetch(&self, reference: &str) -> Result<FetchResponse> {
        let parsed = ParsedReference::parse(reference)?;
        let resolver = self
            .resolvers
            .get(&parsed.scheme)
            .ok_or_else(|| anyhow!("no resolver registered for scheme `{}`", parsed.scheme))?;
        resolver.fetch(&parsed.locator)
    }
}

struct ParsedReference {
    scheme: String,
    locator: String,
}

impl ParsedReference {
    fn parse(input: &str) -> Result<Self> {
        let (scheme_part, rest) = input
            .split_once("://")
            .ok_or_else(|| anyhow!("reference `{input}` is missing a URI scheme"))?;
        if rest.is_empty() {
            bail!("reference `{input}` is missing a locator");
        }
        if let Some((logical, actual)) = scheme_part.split_once('+') {
            let locator = format!("{actual}://{rest}");
            return Ok(Self {
                scheme: logical.to_ascii_lowercase(),
                locator,
            });
        }
        Ok(Self {
            scheme: scheme_part.to_ascii_lowercase(),
            locator: input.to_string(),
        })
    }
}

#[cfg(test)]
mod download_cap_tests {
    use super::{body_read_reason, copy_capped, env_cap_bytes, redact_locator};

    #[test]
    fn copy_stops_past_the_cap_and_allows_exactly_the_cap() {
        let data = vec![7u8; 10];
        let mut out = Vec::new();
        let copied = copy_capped(&mut data.as_slice(), &mut out, Some(10), "x").expect("fits");
        assert_eq!(copied, 10);

        let mut out = Vec::new();
        let err = copy_capped(&mut data.as_slice(), &mut out, Some(9), "x").expect_err("over");
        assert!(err.to_string().contains("download limit"));
        assert!(out.len() <= 10, "must not keep reading past cap + 1");
    }

    /// What a failed streaming body read looks like: reqwest's Display ends in
    /// `" for url (<full url>)"`.
    #[derive(Debug)]
    struct UrlBearingError;

    impl std::fmt::Display for UrlBearingError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "error decoding response body for url (https://b.s3.amazonaws.com/a.gtpack?X-Amz-Signature=SECRET123)"
            )
        }
    }

    impl std::error::Error for UrlBearingError {}

    struct FailingReader;

    impl std::io::Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other(UrlBearingError))
        }
    }

    #[test]
    fn a_failed_body_read_does_not_carry_the_request_url() {
        for cap in [None, Some(1024)] {
            let mut out = Vec::new();
            let err =
                copy_capped(&mut FailingReader, &mut out, cap, "pack").expect_err("read must fail");
            let chain = format!("{err:#}");
            assert!(!chain.contains("SECRET123"), "{chain}");
            assert!(!chain.contains("amazonaws"), "{chain}");
            assert!(chain.contains("error decoding response body"), "{chain}");
        }
    }

    #[test]
    fn strip_url_suffix_handles_parentheses_inside_the_url() {
        use super::strip_url_suffix;
        assert_eq!(
            strip_url_suffix("failed for url (https://h/a(1).gtpack?sig=SECRET): reset"),
            "failed: reset"
        );
        assert_eq!(
            strip_url_suffix("failed for url (https://h/a(1.gtpack?sig=SECRET"),
            "failed"
        );
        assert_eq!(strip_url_suffix("plain"), "plain");
    }

    #[test]
    fn strip_url_suffix_is_fail_closed_for_lone_parentheses() {
        use super::strip_url_suffix;
        for message in [
            "failed for url (https://h/a).gtpack?sig=SECRET)",
            "failed for url (https://h/a).gtpack?sig=SECRET): reset",
            "failed for url (https://h/a(.gtpack?sig=SECRET)",
            "failed for url (https://h/a(.gtpack?sig=SECRET): reset",
            "failed for url (https://h/a(.gtpack?sig=SECRET",
            "failed for url (https://h/a).gtpack?sig=SECRET",
        ] {
            let out = strip_url_suffix(message);
            assert!(!out.contains("SECRET"), "{message} -> {out}");
            assert!(!out.contains("sig="), "{message} -> {out}");
            assert!(out.starts_with("failed"), "{out}");
        }
        assert_eq!(
            strip_url_suffix("failed for url (https://h/a).gtpack?sig=SECRET): reset"),
            "failed: reset"
        );
        assert_eq!(
            strip_url_suffix("failed for url (https://h/a(.gtpack?sig=SECRET)"),
            "failed"
        );
    }

    #[test]
    fn body_read_reason_flattens_without_repeating_the_wrapped_error() {
        let err = std::io::Error::other(UrlBearingError);
        assert_eq!(body_read_reason(&err), "error decoding response body");
    }

    #[test]
    fn locator_redaction_drops_userinfo_query_and_fragment() {
        let out = redact_locator(
            "https://user:pw@bucket.s3.amazonaws.com/packs/a.gtpack?X-Amz-Signature=abc&X-Amz-Credential=k#frag",
        );
        assert_eq!(
            out,
            "https://[redacted]@bucket.s3.amazonaws.com/packs/a.gtpack?[redacted]"
        );
        assert_eq!(
            redact_locator("https://acct.blob.core.windows.net/c/a.gtpack?sv=1&sig=zz"),
            "https://acct.blob.core.windows.net/c/a.gtpack?[redacted]"
        );
        assert_eq!(
            redact_locator("fs://./packs/a.gtpack"),
            "fs://./packs/a.gtpack"
        );
        assert_eq!(redact_locator("no-scheme"), "no-scheme");
    }

    #[test]
    fn redact_locator_is_fail_closed_on_delimiters_in_userinfo() {
        for input in [
            "https://deploy:pa/ss@host/p.gtpack",
            "https://deploy:pa?ss@host/p.gtpack",
            "https://deploy:pa#ss@host/p.gtpack",
        ] {
            assert_eq!(redact_locator(input), "https://[redacted]", "{input}");
        }
        assert_eq!(
            redact_locator("https://u:p@host/a.gtpack"),
            "https://[redacted]@host/a.gtpack"
        );
        let out = redact_locator("x/bot123:TOKEN?r=http://a");
        assert!(!out.contains("TOKEN"), "{out}");
    }

    #[test]
    fn no_cap_copies_everything() {
        let data = vec![1u8; 100];
        let mut out = Vec::new();
        copy_capped(&mut data.as_slice(), &mut out, None, "x").expect("uncapped");
        assert_eq!(out.len(), 100);
    }

    #[test]
    fn unset_variable_uses_the_default() {
        assert_eq!(
            env_cap_bytes("GREENTIC_TEST_UNSET_CAP_VAR", 2),
            Some(2 * 1024 * 1024)
        );
        assert_eq!(env_cap_bytes("GREENTIC_TEST_UNSET_CAP_VAR", 0), None);
    }
}
