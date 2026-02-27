use std::collections::{BTreeSet, HashMap};

use html_escape::decode_html_entities;
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};
use zim::{MimeType, Namespace};

use crate::config::ExtractionConfig;
use crate::normalization::{canonicalize_lemma, generate_aliases, normalize_for_language};

static HEADING_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)<h(?P<level>[2-5])[^>]*>\s*(?:<span[^>]*class="[^"]*mw-headline[^"]*"[^>]*>\s*)?(?P<title>.*?)(?:</span>)?\s*</h[2-5]>"#,
    )
    .expect("invalid heading regex")
});

static TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?is)<[^>]+>"#).expect("invalid HTML tag regex"));

static TAG_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<\s*(?P<close>/)?\s*(?P<name>[a-zA-Z0-9]+)(?P<attrs>[^>]*)>"#)
        .expect("invalid HTML tag token regex")
});

static MULTI_WS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"\s+"#).expect("invalid whitespace regex"));

static RELATION_LABEL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(synonyms?|antonyms?|translations?|derived terms?|related terms?)\b")
        .expect("invalid relation label regex")
});

static NOISE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)<(?:sup|span|div|small)[^>]*class=\"[^\"]*(?:reference|mw-editsection|noprint|maintenance-line|mw-reflink-text)[^\"]*\"[^>]*>.*?</(?:sup|span|div|small)>"#,
    )
    .expect("invalid noise regex")
});

static BRACKET_REF_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"\[[0-9]+\]"#).expect("invalid bracket ref regex"));

static SPLIT_TERMS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\s*(?:,|;|\||•|\u{00B7}|\u{2022}|/|→|\u{2192})\s*")
        .expect("invalid relation split regex")
});

#[derive(Debug, Clone)]
pub struct ExtractedDefinition {
    pub language: String,
    pub order_in_language: i64,
    pub text: String,
    pub normalized_text: String,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct ExtractedRelation {
    pub language: String,
    pub relation_type: String,
    pub order_in_type: i64,
    pub source_text: String,
    pub target_term: String,
    pub normalized_target: String,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct ExtractedAlias {
    pub language: Option<String>,
    pub alias: String,
    pub normalized_alias: String,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct ExtractedPage {
    pub url: String,
    pub title: String,
    pub namespace: String,
    pub mime_type: String,
    pub cluster_idx: Option<u32>,
    pub blob_idx: Option<u32>,
    pub redirect_url: Option<String>,
    pub content_sha256: Option<String>,
    pub raw_html: Option<String>,
    pub plain_text: Option<String>,
    pub extraction_confidence: f64,
    pub definitions: Vec<ExtractedDefinition>,
    pub relations: Vec<ExtractedRelation>,
    pub aliases: Vec<ExtractedAlias>,
}

#[derive(Debug, Clone)]
pub struct HtmlExtraction {
    pub plain_text: Option<String>,
    pub extraction_confidence: f64,
    pub definitions: Vec<ExtractedDefinition>,
    pub relations: Vec<ExtractedRelation>,
    pub aliases: Vec<ExtractedAlias>,
}

#[derive(Debug, Clone)]
struct Heading {
    start: usize,
    end: usize,
    level: u8,
    title: String,
}

#[derive(Debug, Clone)]
struct ListItemFragment {
    start: usize,
    end: usize,
    raw_html: String,
}

pub fn namespace_code(namespace: Namespace) -> &'static str {
    match namespace {
        Namespace::Layout => "-",
        Namespace::Articles => "A",
        Namespace::ArticleMetaData => "B",
        Namespace::UserContent => "C",
        Namespace::ImagesFile => "I",
        Namespace::ImagesText => "J",
        Namespace::Metadata => "M",
        Namespace::CategoriesText => "U",
        Namespace::CategoriesArticleList => "V",
        Namespace::CategoriesArticle => "W",
        Namespace::FulltextIndex => "X",
    }
}

pub fn mime_type_label(mime: &MimeType) -> String {
    match mime {
        MimeType::Type(raw) => raw.clone(),
        MimeType::Redirect => "redirect".to_owned(),
        MimeType::LinkTarget => "link-target".to_owned(),
        MimeType::DeletedEntry => "deleted".to_owned(),
    }
}

pub fn normalize_text(fragment: &str) -> String {
    let without_noise = NOISE_RE.replace_all(fragment, " ");
    let without_tags = TAG_RE.replace_all(without_noise.as_ref(), " ");
    let without_ref = BRACKET_REF_RE.replace_all(without_tags.as_ref(), " ");
    let decoded = decode_html_entities(without_ref.as_ref());
    MULTI_WS_RE
        .replace_all(decoded.as_ref(), " ")
        .trim()
        .to_owned()
}

pub fn html_to_plain_text(html: &str) -> String {
    normalize_text(html)
}

pub fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let output = hasher.finalize();
    format!("{output:x}")
}

pub fn extract_from_html(title: &str, html: &str, config: &ExtractionConfig) -> HtmlExtraction {
    let plain_text = config.store_plain_text.then(|| html_to_plain_text(html));

    if !config.parse_language_sections {
        let aliases = if config.include_title_as_alias {
            build_aliases(title, None, config)
        } else {
            Vec::new()
        };

        return HtmlExtraction {
            plain_text,
            extraction_confidence: 0.0,
            definitions: Vec::new(),
            relations: Vec::new(),
            aliases,
        };
    }

    let headings = extract_headings(html, 2, 5);
    let language_headings: Vec<&Heading> = headings.iter().filter(|h| h.level == 2).collect();

    if language_headings.is_empty() {
        let aliases = if config.include_title_as_alias {
            build_aliases(title, None, config)
        } else {
            Vec::new()
        };

        return HtmlExtraction {
            plain_text,
            extraction_confidence: 0.0,
            definitions: Vec::new(),
            relations: Vec::new(),
            aliases,
        };
    }

    let allowlist: BTreeSet<String> = config
        .language_allowlist
        .iter()
        .map(|value| value.to_lowercase())
        .collect();

    let relation_type_lookup: HashMap<String, String> = config
        .relation_types
        .iter()
        .map(|item| (normalize_relation_type(item), item.to_lowercase()))
        .collect();

    let mut definitions = Vec::new();
    let mut relations = Vec::new();
    let mut confidence_total = 0.0_f64;
    let mut confidence_count = 0_u64;
    let mut language_set = BTreeSet::new();

    for (idx, heading) in language_headings.iter().enumerate() {
        let language = normalize_text(&heading.title);
        if language.is_empty() {
            continue;
        }

        if !allowlist.is_empty() && !allowlist.contains(&language.to_lowercase()) {
            continue;
        }

        let section_end = language_headings
            .get(idx + 1)
            .map(|h| h.start)
            .unwrap_or(html.len());

        if heading.end >= section_end || section_end > html.len() {
            continue;
        }

        let section_html = &html[heading.end..section_end];
        let section_headings = extract_headings(section_html, 3, 5);

        let mut relation_ranges: Vec<(usize, usize, String)> = Vec::new();
        if config.parse_relations {
            for (section_heading_idx, section_heading) in section_headings.iter().enumerate() {
                let normalized_label = normalize_relation_type(&section_heading.title);
                let Some(relation_type) = relation_type_lookup.get(&normalized_label) else {
                    continue;
                };

                let range_end = section_headings
                    .get(section_heading_idx + 1)
                    .map(|next| next.start)
                    .unwrap_or(section_html.len());

                if section_heading.end >= range_end || range_end > section_html.len() {
                    continue;
                }

                relation_ranges.push((section_heading.end, range_end, relation_type.to_string()));
            }
        }

        let list_items = extract_list_items(section_html, config.nested_list_depth_limit);

        let mut def_order = 0_i64;
        for fragment in &list_items {
            if def_order as usize >= config.max_definitions_per_language {
                break;
            }

            let inside_relation = relation_ranges
                .iter()
                .any(|(start, end, _)| fragment.start >= *start && fragment.end <= *end);
            if inside_relation {
                continue;
            }

            let text = normalize_text(&fragment.raw_html);
            if text.len() < config.min_definition_chars {
                continue;
            }
            if RELATION_LABEL_RE.is_match(&text) {
                continue;
            }

            let normalized = normalize_for_language(&language, &text, config);
            let confidence = score_definition(&text, &normalized);
            if confidence < config.confidence_threshold {
                continue;
            }

            language_set.insert(language.clone());
            confidence_total += confidence;
            confidence_count += 1;

            definitions.push(ExtractedDefinition {
                language: language.clone(),
                order_in_language: def_order,
                text,
                normalized_text: normalized,
                confidence,
            });
            def_order += 1;
        }

        if config.parse_relations {
            for (range_start, range_end, relation_type) in relation_ranges {
                let subsection = &section_html[range_start..range_end];
                let relation_items = extract_list_items(subsection, config.nested_list_depth_limit);
                let mut relation_order = 0_i64;

                for item in relation_items {
                    if relation_order as usize >= config.max_relations_per_type {
                        break;
                    }

                    let source_text = normalize_text(&item.raw_html);
                    if source_text.len() < config.min_definition_chars / 2 {
                        continue;
                    }

                    for target_term in split_relation_terms(&source_text) {
                        let normalized_target =
                            normalize_for_language(&language, &target_term, config);
                        let confidence = score_relation(&target_term, &normalized_target);
                        if confidence < config.confidence_threshold {
                            continue;
                        }

                        relations.push(ExtractedRelation {
                            language: language.clone(),
                            relation_type: relation_type.clone(),
                            order_in_type: relation_order,
                            source_text: source_text.clone(),
                            target_term,
                            normalized_target,
                            confidence,
                        });
                        relation_order += 1;
                        confidence_total += confidence;
                        confidence_count += 1;

                        if relation_order as usize >= config.max_relations_per_type {
                            break;
                        }
                    }
                }
            }
        }
    }

    let aliases = if config.include_title_as_alias {
        let primary_language = language_set.first().cloned();
        build_aliases(title, primary_language.as_deref(), config)
    } else {
        Vec::new()
    };

    HtmlExtraction {
        plain_text,
        extraction_confidence: if confidence_count == 0 {
            0.0
        } else {
            confidence_total / confidence_count as f64
        },
        definitions,
        relations,
        aliases,
    }
}

fn build_aliases(
    title: &str,
    primary_language: Option<&str>,
    config: &ExtractionConfig,
) -> Vec<ExtractedAlias> {
    let mut out = Vec::new();

    for alias in generate_aliases(title, primary_language, config) {
        let normalized_alias = canonicalize_lemma(&alias);
        if normalized_alias.len() < config.alias_min_length {
            continue;
        }

        out.push(ExtractedAlias {
            language: primary_language.map(ToOwned::to_owned),
            alias,
            normalized_alias,
            source: "title".to_owned(),
        });
    }

    out
}

fn extract_headings(html: &str, min_level: u8, max_level: u8) -> Vec<Heading> {
    let mut out = Vec::new();

    for captures in HEADING_RE.captures_iter(html) {
        let Some(matched) = captures.get(0) else {
            continue;
        };

        let Some(level_raw) = captures.name("level") else {
            continue;
        };

        let Ok(level) = level_raw.as_str().parse::<u8>() else {
            continue;
        };

        if level < min_level || level > max_level {
            continue;
        }

        let Some(title_match) = captures.name("title") else {
            continue;
        };

        out.push(Heading {
            start: matched.start(),
            end: matched.end(),
            level,
            title: normalize_text(title_match.as_str()),
        });
    }

    out
}

fn extract_list_items(html: &str, depth_limit: usize) -> Vec<ListItemFragment> {
    let mut out = Vec::new();
    let mut list_depth = 0_usize;
    let mut li_depth = 0_usize;
    let mut current_start = None;
    let mut current_list_depth = 0_usize;

    for captures in TAG_TOKEN_RE.captures_iter(html) {
        let Some(matched) = captures.get(0) else {
            continue;
        };

        let tag_name = captures
            .name("name")
            .map(|value| value.as_str().to_ascii_lowercase())
            .unwrap_or_default();
        let is_closing = captures.name("close").is_some();
        let attrs = captures
            .name("attrs")
            .map(|m| m.as_str())
            .unwrap_or_default();
        let is_self_closing = attrs.trim_end().ends_with('/') || is_void_tag(&tag_name);

        if !is_closing {
            if is_list_tag(&tag_name) {
                list_depth += 1;
            }

            if tag_name == "li" {
                li_depth += 1;
                if li_depth == 1 {
                    current_start = Some(matched.end());
                    current_list_depth = list_depth;
                }
            }

            if is_self_closing {
                if tag_name == "li" {
                    if li_depth == 1
                        && current_list_depth <= depth_limit
                        && let Some(start) = current_start.take()
                    {
                        out.push(ListItemFragment {
                            start,
                            end: matched.start(),
                            raw_html: String::new(),
                        });
                    }
                    li_depth = li_depth.saturating_sub(1);
                }

                if is_list_tag(&tag_name) {
                    list_depth = list_depth.saturating_sub(1);
                }
            }

            continue;
        }

        if tag_name == "li" {
            if li_depth == 1
                && current_list_depth <= depth_limit
                && let Some(start) = current_start.take()
            {
                let end = matched.start();
                if start <= end && end <= html.len() {
                    out.push(ListItemFragment {
                        start,
                        end,
                        raw_html: html[start..end].to_owned(),
                    });
                }
            }
            li_depth = li_depth.saturating_sub(1);
            continue;
        }

        if is_list_tag(&tag_name) {
            list_depth = list_depth.saturating_sub(1);
        }
    }

    out
}

fn is_list_tag(tag: &str) -> bool {
    matches!(tag, "ol" | "ul" | "dl")
}

fn is_void_tag(tag: &str) -> bool {
    matches!(
        tag,
        "br" | "img" | "hr" | "meta" | "link" | "input" | "source" | "track" | "wbr"
    )
}

fn score_definition(text: &str, normalized: &str) -> f64 {
    let mut score = 0.2_f64;
    let word_count = text.split_whitespace().count();

    if (4..=42).contains(&word_count) {
        score += 0.30;
    } else if word_count > 42 {
        score -= 0.10;
    }

    if (24..=350).contains(&text.len()) {
        score += 0.25;
    }

    if normalized != text {
        score += 0.10;
    }

    if text.chars().filter(|c| c.is_alphabetic()).count() >= 8 {
        score += 0.15;
    }

    if RELATION_LABEL_RE.is_match(text) {
        score -= 0.35;
    }

    score.clamp(0.0, 1.0)
}

fn score_relation(text: &str, normalized: &str) -> f64 {
    let mut score = 0.3_f64;

    if text.len() >= 2 && text.len() <= 80 {
        score += 0.30;
    }
    if normalized != text {
        score += 0.10;
    }
    if text.chars().any(|c| c.is_alphabetic()) {
        score += 0.20;
    }

    score.clamp(0.0, 1.0)
}

fn normalize_relation_type(label: &str) -> String {
    let lowered = normalize_text(label).to_lowercase();

    match lowered.as_str() {
        "synonym" | "synonyms" => "synonyms".to_owned(),
        "antonym" | "antonyms" => "antonyms".to_owned(),
        "translation" | "translations" => "translations".to_owned(),
        other => other.to_owned(),
    }
}

fn split_relation_terms(text: &str) -> Vec<String> {
    let mut out = BTreeSet::new();

    for item in SPLIT_TERMS_RE.split(text) {
        let item = item.trim_matches(|c: char| {
            c.is_whitespace() || matches!(c, ':' | ',' | ';' | '.' | '(' | ')' | '[' | ']')
        });

        if item.len() < 2 || RELATION_LABEL_RE.is_match(item) {
            continue;
        }

        out.insert(item.to_owned());
    }

    if out.is_empty() {
        let fallback = normalize_text(text);
        if fallback.len() >= 2 {
            out.insert(fallback);
        }
    }

    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtractionConfig;

    #[test]
    fn extracts_plain_text() {
        let value = "<div>Hello <b>World</b> &amp; universe</div>";
        let text = html_to_plain_text(value);
        assert_eq!(text, "Hello World & universe");
    }

    #[test]
    fn extracts_nested_list_definitions() {
        let html = r#"
            <h2><span class="mw-headline">English</span></h2>
            <ol>
              <li>First definition<ul><li>Nested usage should not be promoted</li></ul></li>
              <li>Second definition</li>
            </ol>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.min_definition_chars = 5;
        let extracted = extract_from_html("test", html, &cfg);

        assert_eq!(extracted.definitions.len(), 2);
        assert!(extracted.definitions[0].text.contains("First definition"));
    }

    #[test]
    fn matches_language_heading_without_span() {
        let html = r#"
            <h2 id="English">English</h2>
            <ol>
              <li>Definition text long enough to survive the confidence filter.</li>
            </ol>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.min_definition_chars = 15;
        let extracted = extract_from_html("test", html, &cfg);

        assert_eq!(extracted.definitions.len(), 1);
        assert_eq!(extracted.definitions[0].language, "English");
    }

    #[test]
    fn extracts_relations() {
        let html = r#"
            <h2><span class="mw-headline">English</span></h2>
            <h3><span class="mw-headline">Synonyms</span></h3>
            <ul><li>alpha, beta; gamma</li></ul>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.min_definition_chars = 2;
        let extracted = extract_from_html("test", html, &cfg);

        assert!(extracted.relations.iter().any(|r| r.target_term == "alpha"));
        assert!(extracted.relations.iter().any(|r| r.target_term == "beta"));
        assert!(extracted.relations.iter().any(|r| r.target_term == "gamma"));
    }

    #[test]
    fn respects_allowlist() {
        let html = r#"
            <h2><span class="mw-headline">English</span></h2>
            <ol><li>English text only</li></ol>
            <h2><span class="mw-headline">French</span></h2>
            <ol><li>Texte français</li></ol>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.language_allowlist = vec!["French".to_owned()];
        cfg.min_definition_chars = 3;

        let extracted = extract_from_html("test", html, &cfg);
        assert_eq!(extracted.definitions.len(), 1);
        assert_eq!(extracted.definitions[0].language, "French");
    }
}
