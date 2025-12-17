-- ============================================================
-- PURPOSE
-- Mark products as active/inactive based on recency.
--
-- Why we need this separate script:
-- - MERGE only sees rows that appear in the source query.
-- - If a product is missing in the latest scrape, MERGE cannot "notice" it.
-- - So we derive activity from time: last_seen_at.
--
-- MVP rule:
-- - active if seen in last N days
-- ============================================================

DECLARE ACTIVE_WINDOW_DAYS INT64 DEFAULT 7;

UPDATE `etrendo-prd.amazon_silver.amazon_product_listing_coffee_machines_current`
SET
  is_active = last_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ACTIVE_WINDOW_DAYS DAY)
WHERE TRUE;
