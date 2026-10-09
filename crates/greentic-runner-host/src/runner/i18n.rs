//! Locale resolution + message lookup helpers used across the runner host.
//!
//! Vendored inline when greentic-i18n 0.5 dropped its library surface: the
//! crate is binary-only on crates.io now, and the lower-level greentic-i18n-lib
//! does not expose these runner-specific helpers. Semantics intentionally
//! match the 0.4 greentic-i18n lib so resolved locales and message keys are
//! unchanged.

use std::env;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct I18nText {
    pub message_key: String,
    pub fallback: String,
}

impl I18nText {
    pub fn new(message_key: impl Into<String>, fallback: impl Into<String>) -> Self {
        Self {
            message_key: message_key.into(),
            fallback: fallback.into(),
        }
    }
}

pub fn normalize_locale(value: &str) -> String {
    let lower = value.replace('_', "-").to_ascii_lowercase();
    match lower.split('-').next() {
        Some("en") => "en".to_string(),
        Some(primary) if !primary.is_empty() => primary.to_string(),
        _ => "en".to_string(),
    }
}

pub fn select_locale_with_sources(
    cli_locale: Option<&str>,
    explicit: Option<&str>,
    env_locale: Option<&str>,
    system_locale: Option<&str>,
) -> String {
    if let Some(value) = cli_locale.map(str::trim).filter(|value| !value.is_empty()) {
        return normalize_locale(value);
    }
    if let Some(value) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return normalize_locale(value);
    }
    if let Some(value) = env_locale.map(str::trim).filter(|value| !value.is_empty()) {
        return normalize_locale(value);
    }
    if let Some(value) = system_locale
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return normalize_locale(value);
    }
    "en".to_string()
}

pub fn select_locale(explicit: Option<&str>) -> String {
    let cli_locale = env::var("GREENTIC_LOCALE_CLI").ok();
    let env_locale = env::var("GREENTIC_LOCALE").ok();
    let system = system_locale();
    select_locale_with_sources(
        cli_locale.as_deref(),
        explicit,
        env_locale.as_deref(),
        system.as_deref(),
    )
}

/// English wording for `runner.flow.execution_failed` — what a chat user is
/// shown when a session flow fails terminally.
///
/// Exported so the engine can pass it as the last-resort `fallback` without
/// keeping a second copy of the literal; the rationale for the string itself
/// lives at its use site in `runner::engine`.
pub const FLOW_EXECUTION_FAILED_EN: &str =
    "Sorry — something went wrong while handling that. Please try again.";

pub fn resolve_message(key: &str, fallback: &str, locale: &str) -> String {
    let normalized = normalize_locale(locale);
    match normalized.as_str() {
        "en" => english_message(key).unwrap_or(fallback).to_string(),
        // Additive by construction: a key with no translated wording resolves
        // to `fallback` exactly as it did before this arm gained a lookup, so
        // adding a translation for one key cannot change any other key.
        other => translated_message(key, other)
            .unwrap_or(fallback)
            .to_string(),
    }
}

pub fn resolve_text(text: &I18nText, locale: &str) -> String {
    resolve_message(&text.message_key, &text.fallback, locale)
}

fn english_message(key: &str) -> Option<&'static str> {
    match key {
        "runner.operator.schema_hash_mismatch" => {
            Some("schema hash mismatch between request and resolved contract")
        }
        "runner.operator.contract_introspection_failed" => {
            Some("failed to introspect component contract")
        }
        "runner.operator.schema_ref_not_found" => Some("referenced schema not found in pack"),
        "runner.operator.schema_load_failed" => Some("failed to load referenced schema"),
        "runner.operator.new_state_schema_missing" => {
            Some("missing config schema required for new_state validation")
        }
        "runner.operator.new_state_schema_load_failed" => {
            Some("failed to load config schema for new_state validation")
        }
        "runner.operator.new_state_schema_unavailable" => {
            Some("new_state schema unavailable in strict mode")
        }
        "runner.operator.tenant_mismatch" => Some("request tenant does not match routed tenant"),
        "runner.operator.missing_provider_selector" => {
            Some("request must include provider_id or provider_type")
        }
        "runner.operator.provider_not_found" => Some("provider not found"),
        "runner.operator.op_not_found" => Some("operation not found"),
        "runner.operator.resolve_error" => Some("failed to resolve provider operation"),
        "runner.schema.unsupported_constraint" => Some("schema includes unsupported constraint"),
        "runner.schema.invalid_schema" => Some("invalid schema document"),
        "runner.schema.validation_failed" => Some("schema validation failed"),
        "runner.flow.required_var_missing" => Some("required flow variable not provided"),
        "runner.flow.execution_failed" => Some(FLOW_EXECUTION_FAILED_EN),
        _ => None,
    }
}

/// Non-English wordings, keyed by `(message key, primary-subtag locale)`.
///
/// Deliberately tiny, and deliberately not exhaustive: only strings a real end
/// user reads belong here, and only for the languages the platform already
/// ships UI catalogs for (`id`, `es`, `ja`, `zh`). Every other locale — and
/// every key absent from this table — falls through to the caller's English
/// `fallback` rather than being guessed at.
///
/// Locales arrive here already reduced by [`normalize_locale`], so match on the
/// primary subtag only. One consequence worth knowing: `zh-TW` normalizes to
/// `zh` and is therefore served the Simplified wording. That is a property of
/// the whole mechanism, not of this table.
fn translated_message(key: &str, locale: &str) -> Option<&'static str> {
    match (key, locale) {
        ("runner.flow.execution_failed", "id") => {
            Some("Maaf — terjadi kesalahan saat memproses permintaan itu. Silakan coba lagi.")
        }
        ("runner.flow.execution_failed", "es") => {
            Some("Lo sentimos: se produjo un error al procesar esa solicitud. Vuelve a intentarlo.")
        }
        ("runner.flow.execution_failed", "ja") => {
            Some("申し訳ありません。処理中に問題が発生しました。もう一度お試しください。")
        }
        ("runner.flow.execution_failed", "zh") => Some("抱歉，处理该请求时出现问题。请重试。"),
        _ => None,
    }
}

fn system_locale() -> Option<String> {
    for key in ["LC_ALL", "LANG", "LC_MESSAGES"] {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let stripped = trimmed.split('.').next().unwrap_or(trimmed);
            if !stripped.is_empty() {
                return Some(normalize_locale(stripped));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_message_uses_fallback_for_unknown_key() {
        let message = resolve_message("runner.unknown", "fallback message", "en");
        assert_eq!(message, "fallback message");
    }

    #[test]
    fn resolve_text_uses_key_and_fallback() {
        let text = I18nText::new("runner.operator.op_not_found", "fallback");
        let message = resolve_text(&text, "en");
        assert_eq!(message, "operation not found");
    }

    #[test]
    fn resolve_message_prefers_a_translated_wording_over_the_fallback() {
        // `runner.flow.execution_failed` is the one key with translations.
        let english = resolve_message("runner.flow.execution_failed", "unused fallback", "en-GB");
        assert_eq!(english, FLOW_EXECUTION_FAILED_EN);

        for locale in ["id", "id-ID", "es-419", "ja_JP", "zh-CN"] {
            let translated = resolve_message(
                "runner.flow.execution_failed",
                FLOW_EXECUTION_FAILED_EN,
                locale,
            );
            assert_ne!(
                translated, FLOW_EXECUTION_FAILED_EN,
                "`{locale}` must resolve to its own wording"
            );
            assert!(
                !translated.trim().is_empty(),
                "`{locale}` resolved to empty"
            );
        }
    }

    #[test]
    fn resolve_message_leaves_untranslated_keys_on_the_fallback() {
        // The non-`en` arm gained a lookup; that must not change any key which
        // has no translated wording — including the other real runner keys.
        for key in [
            "runner.operator.op_not_found",
            "runner.flow.required_var_missing",
            "runner.unknown",
        ] {
            assert_eq!(
                resolve_message(key, "fallback message", "nl-NL"),
                "fallback message",
                "`{key}` must still resolve to the caller's fallback"
            );
        }
    }

    #[test]
    fn normalize_locale_reduces_variants() {
        assert_eq!(normalize_locale("en-US"), "en");
        assert_eq!(normalize_locale("nl_NL"), "nl");
    }

    #[test]
    fn select_locale_prefers_explicit_over_env_and_system() {
        assert_eq!(
            select_locale_with_sources(None, Some("en-US"), Some("fr-FR"), Some("nl_NL.UTF-8")),
            "en"
        );
    }

    #[test]
    fn select_locale_prefers_cli_override() {
        assert_eq!(
            select_locale_with_sources(
                Some("it-IT"),
                Some("en-US"),
                Some("fr-FR"),
                Some("nl_NL.UTF-8")
            ),
            "it"
        );
    }

    #[test]
    fn select_locale_uses_env_over_system() {
        assert_eq!(
            select_locale_with_sources(None, None, Some("de-DE"), Some("nl_NL.UTF-8")),
            "de"
        );
    }

    #[test]
    fn select_locale_falls_back_to_system_then_en() {
        assert_eq!(
            select_locale_with_sources(None, None, None, Some("es_ES.UTF-8")),
            "es"
        );
        assert_eq!(select_locale_with_sources(None, None, None, None), "en");
    }
}
