-- ============================================================
-- ACTIVE FLAG (OTTO)
-- Mark inactive if not seen recently (MVP freshness rule).
-- ============================================================

DECLARE ACTIVE_WINDOW_DAYS INT64 DEFAULT 7;

UPDATE `etrendo-prd.otto_silver.otto_product_listing_coffee_machines_current`
SET is_active = last_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ACTIVE_WINDOW_DAYS DAY)
WHERE TRUE;
