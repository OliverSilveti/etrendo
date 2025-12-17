CREATE SCHEMA IF NOT EXISTS `etrendo-prd.amazon_silver`;

CREATE OR REPLACE TABLE
  `etrendo-prd.amazon_silver.amazon_product_details_coffee_machines_current`
CLUSTER BY asin AS

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY asin
      ORDER BY extracted_at DESC
    ) AS rn
  FROM `etrendo-prd.amazon_silver_stg.amazon_product_details_coffee_machines_flat`
)

SELECT
  * EXCEPT (rn)
FROM ranked
WHERE rn = 1;