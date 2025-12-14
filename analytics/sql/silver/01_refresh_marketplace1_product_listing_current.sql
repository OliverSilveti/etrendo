-- ============================================================
-- PURPOSE
-- Refresh the Silver "current" table from Bronze listings.
--
-- Rules:
-- 1. One row per (category_label, node, asin)
-- 2. Prefer SPONSORED listings over ORGANIC if both exist
-- 3. Always keep the latest observation (extracted_at)
-- 4. Track freshness via last_seen_at
-- ============================================================

MERGE `etrendo-prd.amazon_silver.amazon_product_listing_coffee_machines_current` T
USING (

  -- ----------------------------------------------------------
  -- Step 1: Clean and normalize raw Bronze data
  -- - Fix inconsistent types (strings → numbers)
  -- - Convert delivery ARRAY<STRING> → single STRING
  -- - Remove invalid rows (missing ASIN)
  -- ----------------------------------------------------------
  WITH cleaned AS (
    SELECT
      category_label,

      -- Node sometimes arrives as string → normalize to INT
      SAFE_CAST(node AS INT64) AS node,

      asin,

      extracted_at,

      -- TRUE = sponsored, FALSE = organic
      is_sponsored,

      -- Flatten delivery array into readable text
      ARRAY_TO_STRING(delivery, " | ") AS delivery,

      -- Prices & numeric cleanup
      SAFE_CAST(extracted_price AS NUMERIC) AS extracted_price,
      price_raw,
      bought_last_month,

      SAFE_CAST(reviews AS INT64) AS reviews,

      -- Clean text fields
      NULLIF(TRIM(title), "") AS title,
      link,

      SAFE_CAST(position AS INT64) AS position,
      SAFE_CAST(rating AS FLOAT64) AS rating,
      currency,
      SAFE_CAST(page_number AS INT64) AS page_number

    FROM `etrendo-prd.amazon_bronze.amazon_product_listing_coffee_machines`
    WHERE asin IS NOT NULL AND asin != ""
  ),

  -- ----------------------------------------------------------
  -- Step 2: Deduplicate
  --
  -- For each (category_label, node, asin):
  -- - Take the latest extracted_at
  -- - If both sponsored & organic exist, sponsored WINS
  -- ----------------------------------------------------------
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
  )

  -- ----------------------------------------------------------
  -- Step 3: Final shape used for MERGE
  -- ----------------------------------------------------------
  SELECT
    category_label,
    node,
    asin,

    extracted_at,
    is_sponsored,

    delivery,
    extracted_price,
    price_raw,
    bought_last_month,
    reviews,
    title,
    link,
    position,
    rating,
    currency,
    page_number,

    -- Used to determine "active/inactive"
    extracted_at AS last_seen_at,

    -- Audit column
    CURRENT_TIMESTAMP() AS silver_updated_at

  FROM chosen

) S

-- ------------------------------------------------------------
-- Step 4: MERGE condition (business key)
-- ------------------------------------------------------------
ON  T.category_label = S.category_label
AND T.node           = S.node
AND T.asin           = S.asin

-- ------------------------------------------------------------
-- Step 5: Update existing product if newer data arrived
-- ------------------------------------------------------------
WHEN MATCHED AND S.extracted_at > T.extracted_at THEN
  UPDATE SET
    extracted_at      = S.extracted_at,
    is_sponsored      = S.is_sponsored,
    delivery          = S.delivery,
    extracted_price   = S.extracted_price,
    price_raw         = S.price_raw,
    bought_last_month = S.bought_last_month,
    reviews           = S.reviews,
    title             = S.title,
    link              = S.link,
    position          = S.position,
    rating            = S.rating,
    currency          = S.currency,
    page_number       = S.page_number,
    last_seen_at      = S.last_seen_at,
    is_active         = TRUE,
    silver_updated_at = S.silver_updated_at

-- ------------------------------------------------------------
-- Step 6: Insert brand-new products never seen before
-- ------------------------------------------------------------
WHEN NOT MATCHED THEN
  INSERT (
    category_label, node, asin,
    extracted_at, is_sponsored,
    delivery, extracted_price, price_raw, bought_last_month,
    reviews, title, link, position, rating, currency, page_number,
    first_seen_at, last_seen_at, is_active,
    silver_updated_at
  )
  VALUES (
    S.category_label, S.node, S.asin,
    S.extracted_at, S.is_sponsored,
    S.delivery, S.extracted_price, S.price_raw, S.bought_last_month,
    S.reviews, S.title, S.link, S.position, S.rating, S.currency, S.page_number,
    S.extracted_at, S.last_seen_at, TRUE,
    S.silver_updated_at
  );
