-- ============================================================
-- PURPOSE (ONE-TIME BOOTSTRAP)
-- Build the Silver "current" table from ALL historical Bronze rows.
--
-- What this does:
-- 1) Creates the amazon_silver dataset if needed
-- 2) Drops the current table if it exists (to avoid spec conflicts)
-- 3) Recreates the table by selecting ONE "winning" row per product key
--
-- Business key:
--   (category_label, node, asin)
--
-- Winner selection rule:
-- 1) latest extracted_at wins
-- 2) if both sponsored + organic exist, sponsored wins
-- ============================================================

-- 1) Create dataset (BigQuery calls datasets "schemas")
CREATE SCHEMA IF NOT EXISTS `etrendo-prd.amazon_silver`;

-- 2) Drop current table (safe for one-time bootstrapping)
DROP TABLE IF EXISTS `etrendo-prd.amazon_silver.amazon_product_listing_coffee_machines_current`;

-- 3) Create table from historical Bronze data
CREATE TABLE `etrendo-prd.amazon_silver.amazon_product_listing_coffee_machines_current`
CLUSTER BY category_label, node, asin
AS
WITH
-- ------------------------------------------------------------
-- Step A: Clean & normalize raw Bronze types
-- ------------------------------------------------------------
cleaned AS (
  SELECT
    category_label,

    -- node sometimes comes as string → normalize
    SAFE_CAST(node AS INT64) AS node,

    -- ASIN is the product identifier
    asin,

    extracted_at,

    -- TRUE = sponsored, FALSE = organic
    is_sponsored,

    -- delivery is ARRAY<STRING> → convert to a readable single string
    ARRAY_TO_STRING(delivery, " | ") AS delivery,

    -- money: use NUMERIC in silver
    SAFE_CAST(extracted_price AS NUMERIC) AS extracted_price,

    price_raw,
    bought_last_month,

    SAFE_CAST(reviews AS INT64) AS reviews,

    -- clean text fields
    NULLIF(TRIM(title), "") AS title,
    link,

    SAFE_CAST(position AS INT64) AS position,
    SAFE_CAST(rating AS FLOAT64) AS rating,
    currency,
    SAFE_CAST(page_number AS INT64) AS page_number

  FROM `etrendo-prd.amazon_bronze.amazon_product_listing_coffee_machines`
  WHERE asin IS NOT NULL AND asin != ""
),

-- ------------------------------------------------------------
-- Step B: Choose ONE row per product key
-- We keep only the "winner" row per (category_label,node,asin)
-- ------------------------------------------------------------
chosen AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      cleaned.*,

      -- Ranking logic:
      -- 1) newest extracted_at first
      -- 2) sponsored preferred over organic
      ROW_NUMBER() OVER (
        PARTITION BY category_label, node, asin
        ORDER BY extracted_at DESC, is_sponsored DESC
      ) AS rn

    FROM cleaned
  )
  WHERE rn = 1
),

-- ------------------------------------------------------------
-- Step C: Compute first_seen_at across full Bronze history
-- (first time we ever saw the product in this category/node)
-- ------------------------------------------------------------
first_seen AS (
  SELECT
    category_label,
    SAFE_CAST(node AS INT64) AS node,
    asin,
    MIN(extracted_at) AS first_seen_at
  FROM `etrendo-prd.amazon_bronze.amazon_product_listing_coffee_machines`
  WHERE asin IS NOT NULL AND asin != ""
  GROUP BY 1,2,3
)

-- ------------------------------------------------------------
-- Step D: Build final Silver table rows
-- ------------------------------------------------------------
SELECT
  c.category_label,
  c.node,
  c.asin,

  c.extracted_at,
  c.is_sponsored,

  c.delivery,
  c.extracted_price,
  c.price_raw,
  c.bought_last_month,
  c.reviews,
  c.title,
  c.link,
  c.position,
  c.rating,
  c.currency,
  c.page_number,

  -- first time seen
  f.first_seen_at,

  -- last time seen == the chosen row’s extracted_at
  c.extracted_at AS last_seen_at,

  -- brand new table = everything starts active
  TRUE AS is_active,

  -- audit timestamp for when Silver was generated
  CURRENT_TIMESTAMP() AS silver_updated_at

FROM chosen c
LEFT JOIN first_seen f
USING (category_label, node, asin);



