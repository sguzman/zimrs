-- Layer 1 validation and health checks for hot-language optimization.

-- Projection row count.
SELECT COUNT(*) AS hot_rows FROM dictionary.hot_lookup;

-- Projection/source parity (distinct source key count must match projection row count).
SELECT
    (SELECT COUNT(*)
     FROM (
         SELECT a.language, a.normalized_alias, a.page_id
         FROM dictionary.lemma_aliases a
         WHERE a.language IN ('English', 'Spanish', 'German', 'French')
         GROUP BY 1,2,3
     ) s) AS source_distinct,
    (SELECT COUNT(*) FROM dictionary.hot_lookup) AS hot_count;

-- Orphan/duplicate integrity checks.
SELECT COUNT(*) AS hot_orphans
FROM dictionary.hot_lookup h
LEFT JOIN dictionary.pages p ON p.id = h.page_id
WHERE p.id IS NULL;

SELECT COUNT(*) AS hot_duplicate_keys
FROM (
    SELECT language, normalized_alias, page_id, COUNT(*)
    FROM dictionary.hot_lookup
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
) d;

-- Check required indexes exist.
SELECT schemaname, indexname
FROM pg_indexes
WHERE schemaname = 'dictionary'
  AND indexname IN (
      'idx_hot_lookup_lang_norm',
      'idx_hot_lookup_lang_page',
      'idx_aliases_hot_norm_page',
      'idx_definitions_hot_page_order'
  )
ORDER BY indexname;

-- Baseline latency probes (canonical vs hot projection).
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.id, p.title, p.url
FROM dictionary.lemma_aliases a
JOIN dictionary.pages p ON p.id = a.page_id
WHERE a.language = 'English' AND a.normalized_alias = 'house'
ORDER BY p.id
LIMIT 20;

EXPLAIN (ANALYZE, BUFFERS)
SELECT page_id AS id, title, url
FROM dictionary.hot_lookup
WHERE language = 'English' AND normalized_alias = 'house'
ORDER BY page_id
LIMIT 20;
