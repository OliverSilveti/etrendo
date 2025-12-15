-- ============================================================
-- REFRESH (OTTO)
-- Upsert latest listing state into Silver current.
--
-- Key:
--   (category_label, product_key)
--
-- Dedupe:
-- - Prefer rows WITH page_number
-- - Then latest extracted_at
--
-- Update behavior:
-- - Do NOT overwrite existing values with NULL (page_number, title, price, etc.)
-- ============================================================

MERGE `etrendo-prd.otto_silver.otto_product_listing_coffee_machines_current` T
USING (
  WITH cleaned AS (
    SELECT
      category_label,

      REGEXP_REPLACE(TRIM(link), r'\?.*$', '') AS canonical_link,

      CAST(
        ABS(
          FARM_FINGERPRINT(
            REGEXP_REPLACE(TRIM(link), r'\?.*$', '')
          )
        ) AS STRING
      ) AS product_key,

      NULLIF(TRIM(title), "") AS title,
      price_raw,

      SAFE_CAST(
        REPLACE(
          REPLACE(REGEXP_EXTRACT(price_raw, r'[\d\.,]+'), '.', ''),
          ',', '.'
        ) AS NUMERIC
      ) AS price,

      SAFE_CAST(page_number AS INT64) AS page_number,
      extracted_at

    FROM `etrendo-prd.otto_bronze.otto_product_listing_coffee_machines`
    WHERE link IS NOT NULL
      AND TRIM(link) != ""
  ),

  -- Choose 1 row per product_key
  chosen AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        cleaned.*,
        ROW_NUMBER() OVER (
          PARTITION BY category_label, product_key
          ORDER BY
            page_number IS NOT NULL DESC,
            extracted_at DESC
        ) AS rn
      FROM cleaned
    )
    WHERE rn = 1
  )

  SELECT
    category_label,
    product_key,
    canonical_link,
    title,
    price_raw,
    price,
    page_number,
    extracted_at,
    extracted_at AS last_seen_at,
    CURRENT_TIMESTAMP() AS silver_updated_at
  FROM chosen
) S
ON T.category_label = S.category_label
AND T.product_key = S.product_key

WHEN MATCHED AND S.extracted_at > T.extracted_at THEN
  UPDATE SET
    -- keep old values if new is NULL
    canonical_link = COALESCE(S.canonical_link, T.canonical_link),
    title         = COALESCE(S.title, T.title),
    price_raw     = COALESCE(S.price_raw, T.price_raw),
    price         = COALESCE(S.price, T.price),
    page_number   = COALESCE(S.page_number, T.page_number),

    extracted_at  = S.extracted_at,
    last_seen_at  = S.last_seen_at,
    is_active     = TRUE,
    silver_updated_at = S.silver_updated_at

WHEN NOT MATCHED THEN
  INSERT (
    category_label, product_key, canonical_link,
    title, price_raw, price, page_number, extracted_at,
    first_seen_at, last_seen_at, is_active,
    silver_updated_at
  )
  VALUES (
    S.category_label, S.product_key, S.canonical_link,
    S.title, S.price_raw, S.price, S.page_number, S.extracted_at,
    S.extracted_at, S.last_seen_at, TRUE,
    S.silver_updated_at
  );
