-- ============================================================
-- ONE-TIME BOOTSTRAP (OTTO)
--
-- Purpose:
-- Build the Silver "current" table from ALL historical Bronze data.
--
-- Key strategy (MVP-safe):
-- - canonical_link = URL without query params
-- - product_key = stable hash of canonical_link
--
-- Dedup rule:
-- 1) Prefer rows WITH page_number (if available)
-- 2) Otherwise take the latest extracted_at
-- ============================================================

-- 1) Ensure dataset exists
CREATE SCHEMA IF NOT EXISTS `etrendo-prd.otto_silver`;

-- 2) Drop table if it exists (one-time only)
DROP TABLE IF EXISTS `etrendo-prd.otto_silver.otto_product_listing_coffee_machines_current`;

-- 3) Rebuild table from Bronze
CREATE TABLE `etrendo-prd.otto_silver.otto_product_listing_coffee_machines_current`
CLUSTER BY category_label, product_key
AS
WITH cleaned AS (
  SELECT
    category_label,

    -- Canonical URL (removes tracking params)
    REGEXP_REPLACE(TRIM(link), r'\?.*$', '') AS canonical_link,

    -- Stable surrogate key (NOT a business product ID)
    CAST(
      ABS(
        FARM_FINGERPRINT(
          REGEXP_REPLACE(TRIM(link), r'\?.*$', '')
        )
      ) AS STRING
    ) AS product_key,

    NULLIF(TRIM(title), "") AS title,
    price_raw,

    -- Optional price parsing (safe if NULL / malformed)
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

-- ------------------------------------------------------------
-- Choose ONE row per product_key
-- Prefer rows that have page_number, then newest extracted_at
-- ------------------------------------------------------------
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
),

-- ------------------------------------------------------------
-- First time the product ever appeared in Bronze
-- ------------------------------------------------------------
first_seen AS (
  SELECT
    category_label,
    CAST(
      ABS(
        FARM_FINGERPRINT(
          REGEXP_REPLACE(TRIM(link), r'\?.*$', '')
        )
      ) AS STRING
    ) AS product_key,
    MIN(extracted_at) AS first_seen_at
  FROM `etrendo-prd.otto_bronze.otto_product_listing_coffee_machines`
  WHERE link IS NOT NULL
    AND TRIM(link) != ""
  GROUP BY 1,2
)

-- ------------------------------------------------------------
-- Final Silver table
-- ------------------------------------------------------------
SELECT
  c.category_label,
  c.product_key,
  c.canonical_link,
  c.title,
  c.price_raw,
  c.price,
  c.page_number,
  c.extracted_at,

  f.first_seen_at,
  c.extracted_at AS last_seen_at,
  TRUE AS is_active,

  CURRENT_TIMESTAMP() AS silver_updated_at
FROM chosen c
LEFT JOIN first_seen f
USING (category_label, product_key);
