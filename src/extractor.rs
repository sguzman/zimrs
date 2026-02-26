use html_escape::decode_html_entities;
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};
use zim::{MimeType, Namespace};

use crate::config::ExtractionConfig;

static H2_HEADLINE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)<h2[^>]*>.*?<span[^>]*class=\"[^\"]*mw-headline[^\"]*\"[^>]*>(?P<lang>.*?)</span>.*?</h2>"#,
    )
    .expect("invalid headline regex")
});

static LI_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?is)<li[^>]*>(?P<value>.*?)</li>"#).expect("invalid li regex"));

static TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?is)<[^>]+>"#).expect("invalid HTML tag regex"));

static MULTI_WS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"\s+"#).expect("invalid whitespace regex"));

#[derive(Debug, Clone)]
pub struct ExtractedDefinition {
    pub language: String,
    pub order_in_language: i64,
    pub text: String,
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
    pub definitions: Vec<ExtractedDefinition>,
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
    let without_tags = TAG_RE.replace_all(fragment, " ");
    let decoded = decode_html_entities(without_tags.as_ref());
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

pub fn extract_definitions(html: &str, config: &ExtractionConfig) -> Vec<ExtractedDefinition> {
    if !config.parse_language_sections {
        return Vec::new();
    }

    let allowlist: Vec<String> = config
        .language_allowlist
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    let mut headings: Vec<(usize, usize, String)> = Vec::new();
    for captures in H2_HEADLINE_RE.captures_iter(html) {
        let Some(matched) = captures.get(0) else {
            continue;
        };

        let Some(language_match) = captures.name("lang") else {
            continue;
        };

        let language = normalize_text(language_match.as_str());
        if language.is_empty() {
            continue;
        }

        headings.push((matched.start(), matched.end(), language));
    }

    if headings.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();

    for (index, (_, section_start, language)) in headings.iter().enumerate() {
        if !allowlist.is_empty()
            && !allowlist
                .iter()
                .any(|item| item == &language.to_lowercase())
        {
            continue;
        }

        let section_end = headings
            .get(index + 1)
            .map(|(next_start, _, _)| *next_start)
            .unwrap_or(html.len());

        if *section_start >= section_end || section_end > html.len() {
            continue;
        }

        let section_html = &html[*section_start..section_end];

        let mut count_for_language = 0_i64;
        for captures in LI_RE.captures_iter(section_html) {
            let Some(body) = captures.name("value") else {
                continue;
            };

            let text = normalize_text(body.as_str());
            if text.len() < config.min_definition_chars {
                continue;
            }

            out.push(ExtractedDefinition {
                language: language.clone(),
                order_in_language: count_for_language,
                text,
            });
            count_for_language += 1;

            if count_for_language as usize >= config.max_definitions_per_language {
                break;
            }
        }
    }

    out
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
    fn extracts_language_definitions() {
        let html = r#"
            <h2><span class="mw-headline">English</span></h2>
            <ol>
              <li>First english meaning</li>
              <li>Second english meaning</li>
            </ol>
            <h2><span class="mw-headline">Spanish</span></h2>
            <ol>
              <li>Significado de prueba</li>
            </ol>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.min_definition_chars = 5;
        cfg.max_definitions_per_language = 10;

        let definitions = extract_definitions(html, &cfg);
        assert_eq!(definitions.len(), 3);
        assert_eq!(definitions[0].language, "English");
        assert_eq!(definitions[1].language, "English");
        assert_eq!(definitions[2].language, "Spanish");
    }

    #[test]
    fn respects_language_allowlist() {
        let html = r#"
            <h2><span class="mw-headline">English</span></h2>
            <ol><li>English text only</li></ol>
            <h2><span class="mw-headline">French</span></h2>
            <ol><li>Texte français</li></ol>
        "#;

        let mut cfg = ExtractionConfig::default();
        cfg.language_allowlist = vec!["French".to_owned()];
        cfg.min_definition_chars = 3;

        let definitions = extract_definitions(html, &cfg);
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].language, "French");
    }
}
