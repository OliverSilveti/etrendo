CREATE SCHEMA IF NOT EXISTS `etrendo-prd.amazon_silver_stg`;

CREATE OR REPLACE TABLE `etrendo-prd.amazon_silver_stg.amazon_product_details_coffee_machines_flat`
PARTITION BY DATE(extracted_at)
CLUSTER BY asin AS
WITH src AS (
  SELECT *
  FROM `etrendo-prd.amazon_bronze.amazon_product_details_coffee_machines`
  WHERE asin IS NOT NULL AND TRIM(asin) != ''
),
typed AS (
  SELECT
    -- =========================
    -- KEYS
    -- =========================
    NULLIF(TRIM(asin), '') AS asin,
    NULLIF(TRIM(category_label), '') AS category_label,
    extracted_at,
    NULLIF(TRIM(node), '') AS node,

    -- =========================
    -- PRODUCT IDENTITY
    -- =========================
    NULLIF(TRIM(title), '') AS title,
    NULLIF(TRIM(brand), '') AS brand,
    NULLIF(TRIM(product_name), '') AS product_name,
    NULLIF(TRIM(url), '') AS url,
    NULLIF(TRIM(parent_asin), '') AS parent_asin,
    NULLIF(TRIM(manufacturer), '') AS manufacturer,

    -- =========================
    -- STATUS / DEBUG
    -- =========================
    NULLIF(TRIM(page_type), '') AS page_type,
    NULLIF(TRIM(stock), '') AS stock,
    NULLIF(TRIM(item_status), '') AS item_status,
    SAFE_CAST(status_code AS INT64) AS status_code,
    NULLIF(TRIM(error_message), '') AS error_message,

    -- =========================
    -- RATINGS
    -- =========================
    SAFE_CAST(rating AS FLOAT64) AS rating,
    SAFE_CAST(review_count AS INT64) AS review_count,

    -- =========================
    -- PRICING (RAW TYPED)
    -- =========================
    NULLIF(TRIM(currency), '') AS currency,
    SAFE_CAST(price AS NUMERIC) AS price,
    SAFE_CAST(price_upper AS NUMERIC) AS price_upper,
    SAFE_CAST(price_initial AS NUMERIC) AS price_initial,
    SAFE_CAST(price_shipping AS NUMERIC) AS price_shipping,
    SAFE_CAST(price_buybox AS NUMERIC) AS price_buybox,
    SAFE_CAST(price_sns AS NUMERIC) AS price_sns,

    -- =========================
    -- BUY BOX SELLER (KEY PART)
    -- =========================
    NULLIF(TRIM(JSON_VALUE(buybox, '$[0].seller_id')), '') AS buybox_seller_id,
    NULLIF(TRIM(JSON_VALUE(buybox, '$[0].seller_name')), '') AS buybox_seller_name,
    NULLIF(TRIM(JSON_VALUE(buybox, '$[0].stock')), '') AS buybox_stock,
    SAFE_CAST(JSON_VALUE(buybox, '$[0].price') AS NUMERIC) AS buybox_price_from_json,

    -- Safer Amazon flag (Amazon.de, Amazon EU S.a.r.l., etc.)
    REGEXP_CONTAINS(
      LOWER(COALESCE(NULLIF(TRIM(JSON_VALUE(buybox, '$[0].seller_name')), ''), '')),
      r'\bamazon\b'
    ) AS buybox_is_amazon,

    -- =========================
    -- OTHER USEFUL FLAGS
    -- =========================
    SAFE_CAST(is_prime_eligible AS BOOL) AS is_prime_eligible,
    SAFE_CAST(has_videos AS BOOL) AS has_videos,
    NULLIF(TRIM(main_image), '') AS main_image,
    NULLIF(TRIM(bullet_points), '') AS bullet_points,
    NULLIF(TRIM(sales_volume), '') AS sales_volume,
    NULLIF(TRIM(coupon), '') AS coupon,

    -- =========================
    -- SEMI-STRUCTURED (KEEP)
    -- =========================
    description,
    category,
    images,
    delivery,
    buybox,
    rating_stars_distribution,
    product_details,
    product_overview,
    technical_details,
    sales_rank,
    variation_selected_dimensions,
    variation_all,
    sns_discounts,

    -- =========================
    -- RAW PAYLOAD (ALWAYS KEEP)
    -- =========================
    payload_raw

  FROM src
)
SELECT
  t.*,

  -- =========================
  -- CLEANED PRICING (SENTINEL FIX)
  -- =========================
  CASE WHEN t.price > 0 THEN t.price ELSE NULL END AS price_clean,
  CASE WHEN t.price_shipping > 0 THEN t.price_shipping ELSE NULL END AS price_shipping_clean,
  CASE WHEN t.buybox_price_from_json > 0 THEN t.buybox_price_from_json ELSE NULL END AS buybox_price_from_json_clean,

  CASE
    WHEN (CASE WHEN t.price > 0 THEN t.price ELSE NULL END) IS NULL THEN NULL
    ELSE (CASE WHEN t.price > 0 THEN t.price ELSE NULL END)
         + COALESCE((CASE WHEN t.price_shipping > 0 THEN t.price_shipping ELSE NULL END), 0)
  END AS total_price_clean,

  -- =========================
  -- EXTRACTION QUALITY LABEL (THE KEY)
  -- =========================
  CASE
    WHEN t.price > 0 OR t.buybox_price_from_json > 0 THEN 'COMPLETE'
    WHEN t.payload_raw IS NOT NULL
         AND (t.price IS NULL OR t.price <= 0)
         AND (t.buybox_price_from_json IS NULL OR t.buybox_price_from_json <= 0)
      THEN 'INCOMPLETE_PRICE_EXTRACTION'
    ELSE 'UNKNOWN'
  END AS pdp_extraction_state

FROM typed t;
