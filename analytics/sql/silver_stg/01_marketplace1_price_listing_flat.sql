CREATE SCHEMA IF NOT EXISTS `etrendo-prd.amazon_silver_stg`;

CREATE OR REPLACE TABLE `etrendo-prd.amazon_silver_stg.amazon_price_listing_coffee_machines_flat`
PARTITION BY DATE(extracted_at)
CLUSTER BY asin AS
SELECT
  extracted_at,

  SAFE_CAST(node_label AS INT64) AS node_label,
  NULLIF(TRIM(category_label), '') AS category_label,
  NULLIF(TRIM(asin), '') AS asin,

  -- NEW: offer position in the offers list (1..N)
  SAFE_CAST(offer_position AS INT64) AS offer_position,

  -- NEW: pricing status (you define values in ingestion)
  -- Examples: 'BUYBOX', 'OTHER', 'UNAVAILABLE', 'OOS'
  NULLIF(TRIM(pricing_status), '') AS pricing_status,

  NULLIF(TRIM(delivery_date), '') AS delivery_date,
  NULLIF(TRIM(delivery), '') AS delivery_text,
  NULLIF(TRIM(delivery_type), '') AS delivery_type,

  NULLIF(TRIM(seller_link), '') AS seller_link,
  NULLIF(TRIM(url), '') AS url,

  NULLIF(TRIM(seller_id), '') AS seller_id,
  NULLIF(TRIM(seller), '') AS seller_name,

  NULLIF(TRIM(condition), '') AS condition,
  NULLIF(TRIM(currency), '') AS currency,

  SAFE_CAST(price AS NUMERIC) AS price,
  SAFE_CAST(price_shipping AS NUMERIC) AS price_shipping,
  SAFE_CAST(price AS NUMERIC) + COALESCE(SAFE_CAST(price_shipping AS NUMERIC), 0) AS total_price,

  SAFE_CAST(rating_count AS INT64) AS rating_count,
  SAFE_CAST(review_count AS INT64) AS review_count,
  NULLIF(TRIM(title), '') AS title

FROM `etrendo-prd.amazon_bronze.amazon_price_listing_coffee_machines`
WHERE asin IS NOT NULL AND TRIM(asin) != '';