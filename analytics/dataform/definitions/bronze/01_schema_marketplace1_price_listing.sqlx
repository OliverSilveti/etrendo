CREATE OR REPLACE EXTERNAL TABLE `etrendo-prd.amazon_bronze.amazon_price_listing_coffee_machines` (
  asin STRING,
  title STRING,
  url STRING,
  review_count FLOAT64,
  seller STRING,
  price FLOAT64,
  currency STRING,
  price_shipping FLOAT64,
  condition STRING,
  rating_count FLOAT64,
  seller_id STRING,
  seller_link STRING,
  delivery STRING,
  delivery_type STRING,
  delivery_date STRING,
  offer_position FLOAT64,
  delivery_option_position FLOAT64,
  category_label STRING,
  node_label STRING,
  extracted_at TIMESTAMP,
  pricing_status STRING,
  error_message STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://amazon-pricing-listing-raw-etrendo-prd/coffee_machines-13528250031/*.jsonl']
);