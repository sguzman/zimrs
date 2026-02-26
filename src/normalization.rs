use std::collections::BTreeSet;

use deunicode::deunicode;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::config::ExtractionConfig;

static MULTI_WS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s+").expect("invalid whitespace regex"));

static NON_WORD_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[^\p{L}\p{N}]+").expect("invalid non-word regex"));

pub fn normalize_for_language(language: &str, text: &str, config: &ExtractionConfig) -> String {
    let plugin_name = config
        .language_normalizers
        .iter()
        .find(|(configured_language, _)| configured_language.eq_ignore_ascii_case(language))
        .map(|(_, plugin)| plugin.as_str())
        .unwrap_or(config.default_normalizer.as_str());

    apply_plugin(plugin_name, text)
}

pub fn canonicalize_lemma(value: &str) -> String {
    let transliterated = deunicode(value);
    let lowered = transliterated.to_lowercase();
    let cleaned = NON_WORD_RE.replace_all(&lowered, " ");
    collapse_ws(cleaned.as_ref())
}

pub fn generate_aliases(
    title: &str,
    language: Option<&str>,
    config: &ExtractionConfig,
) -> Vec<String> {
    let mut out = BTreeSet::new();

    let title_trimmed = collapse_ws(title);
    if title_trimmed.len() >= config.alias_min_length {
        out.insert(title_trimmed.clone());
        out.insert(title_trimmed.to_lowercase());

        let translit = deunicode(&title_trimmed);
        let translit = collapse_ws(&translit);
        if translit.len() >= config.alias_min_length {
            out.insert(translit);
        }
    }

    if let Some(language) = language {
        let plugin_normalized = normalize_for_language(language, title, config);
        if plugin_normalized.len() >= config.alias_min_length {
            out.insert(plugin_normalized);
        }
    }

    out.into_iter().collect()
}

fn apply_plugin(plugin_name: &str, text: &str) -> String {
    match plugin_name {
        "english_basic" => english_basic(text),
        "romance_basic" => romance_basic(text),
        "cjk_basic" => cjk_basic(text),
        _ => collapse_ws(text),
    }
}

fn english_basic(text: &str) -> String {
    let lowered = collapse_ws(text).to_lowercase();
    let without_infinitive = lowered.strip_prefix("to ").unwrap_or(&lowered);
    let without_article = without_infinitive
        .strip_prefix("a ")
        .or_else(|| without_infinitive.strip_prefix("an "))
        .or_else(|| without_infinitive.strip_prefix("the "))
        .unwrap_or(without_infinitive);
    collapse_ws(without_article)
}

fn romance_basic(text: &str) -> String {
    let lowered = collapse_ws(text).to_lowercase();
    let normalized_apostrophes = lowered.replace(['’', '`'], "'");
    collapse_ws(&normalized_apostrophes)
}

fn cjk_basic(text: &str) -> String {
    collapse_ws(text)
}

pub fn collapse_ws(text: &str) -> String {
    MULTI_WS_RE.replace_all(text.trim(), " ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtractionConfig;

    #[test]
    fn canonicalizes_lemma() {
        assert_eq!(canonicalize_lemma(" Café-au-lait "), "cafe au lait");
    }

    #[test]
    fn english_plugin_strips_prefixes() {
        let value = english_basic("To The Example");
        assert_eq!(value, "example");
    }

    #[test]
    fn generates_aliases() {
        let cfg = ExtractionConfig::default();
        let aliases = generate_aliases("Café", Some("English"), &cfg);
        assert!(aliases.iter().any(|v| v == "Café"));
        assert!(aliases.iter().any(|v| v == "café"));
    }
}
