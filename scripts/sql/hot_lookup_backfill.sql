-- Rebuild/refresh hot-language lookup projection.
-- Safe to rerun: uses deterministic grouping + upsert.

WITH hot_aliases AS (
    SELECT
        a.language,
        a.normalized_alias,
        MIN(a.alias) AS alias,
        a.page_id
    FROM dictionary.lemma_aliases a
    WHERE a.language IN ('English', 'Spanish', 'German', 'French')
    GROUP BY a.language, a.normalized_alias, a.page_id
),
first_defs AS (
    SELECT DISTINCT ON (d.page_id, d.language)
        d.page_id,
        d.language,
        d.definition_text
    FROM dictionary.definitions d
    WHERE d.language IN ('English', 'Spanish', 'German', 'French')
    ORDER BY d.page_id, d.language, d.def_order
)
INSERT INTO dictionary.hot_lookup (
    language,
    normalized_alias,
    alias,
    page_id,
    title,
    url,
    primary_definition,
    updated_at
)
SELECT
    h.language,
    h.normalized_alias,
    h.alias,
    h.page_id,
    p.title,
    p.url,
    f.definition_text,
    now()
FROM hot_aliases h
JOIN dictionary.pages p ON p.id = h.page_id
LEFT JOIN first_defs f
    ON f.page_id = h.page_id
   AND f.language = h.language
ON CONFLICT (language, normalized_alias, page_id)
DO UPDATE SET
    alias = EXCLUDED.alias,
    title = EXCLUDED.title,
    url = EXCLUDED.url,
    primary_definition = EXCLUDED.primary_definition,
    updated_at = now();

ANALYZE dictionary.hot_lookup;
