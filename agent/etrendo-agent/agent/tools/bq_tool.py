import logging
from typing import List, Optional
from google.cloud import bigquery
from google.adk.tools import BaseTool
from agent.config import config

logger = logging.getLogger(__name__)

class BigQueryTool(BaseTool):
    # Name is expected by ADK tooling when inspecting available tools.
    name: str = "bigquery_tool"

    def __init__(self):
        try:
            self.project_id = config["bigquery"]["project_id"]
            self.dataset_id = config["bigquery"]["dataset_id"]
            self.table_id = config["bigquery"]["table_id"]
            self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            self.bq_client = bigquery.Client(project=self.project_id)
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def _execute_query(self, query: str, empty_message: Optional[str] = None) -> str:
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()

            # Convert results to a string format suitable for the LLM
            data_rows = []
            headers = [field.name for field in results.schema]
            data_rows.append(",".join(headers))

            for row in results:
                row_values = [str(val) for val in row.values()]
                data_rows.append(",".join(row_values))

            # If only headers exist, return a clear no-data message
            if len(data_rows) == 1:
                return empty_message or "No data found for the requested filters."

            return "\n".join(data_rows)
        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            return f"Error fetching data from BigQuery: {e}"

    def get_daily_pulse(self, seller_name: str) -> str:
        """
        Fetches data to answer 'How am I doing today vs yesterday?' for a specific seller.
        This includes Buy Box status, pricing, and stock for the seller's products.
        """
        query = f"""
            WITH relevant_asins AS (
                SELECT DISTINCT asin
                FROM `{self.full_table_id}`
                WHERE 
                    snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
                    AND LOWER(buybox_seller_name) = LOWER('{seller_name}')
            )
            SELECT
                snapshot_date,
                asin,
                pdp_total_price,
                buybox_seller_name,
                buybox_stock
            FROM
                `{self.full_table_id}`
            WHERE
                snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
                AND asin IN (SELECT asin FROM relevant_asins)
            ORDER BY snapshot_date DESC, asin
        """
        return self._execute_query(
            query,
            empty_message=f"No recent data found for seller '{seller_name}' in the last 2 days.",
        )

    def get_price_competitiveness(self, asin: str) -> str:
        """
        Fetches pricing data for a specific ASIN over the last 14 days to assess price competitiveness.
        """
        # Ensure ASIN is uppercase for consistency
        asin = asin.upper()
        query = f"""
            SELECT
                snapshot_date,
                asin,
                pdp_total_price,
                buybox_seller_name
            FROM
                `{self.full_table_id}`
            WHERE
                snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                AND asin = '{asin}'
            ORDER BY snapshot_date DESC
        """
        return self._execute_query(
            query,
            empty_message=f"No pricing data found for ASIN '{asin}' in the last 14 days.",
        )

    def get_stock_status(self, seller_name: str) -> str:
        """
        Fetches stock status for products where the specified seller is the Buy Box winner.
        """
        query = f"""
            SELECT
                snapshot_date,
                asin,
                buybox_seller_name,
                buybox_stock
            FROM
                `{self.full_table_id}`
            WHERE
                snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
                AND LOWER(buybox_seller_name) = LOWER('{seller_name}')
            ORDER BY snapshot_date DESC, asin
        """
        return self._execute_query(
            query,
            empty_message=f"No stock status found for seller '{seller_name}' in the last 2 days.",
        )

    def get_buy_box_changes(self, seller_name: str, days: int = 2) -> str:
        """
        Fetches data to identify Buy Box wins and losses for a seller compared to the previous day.
        """
        # Clamp days to a reasonable range to avoid overly large scans.
        days = max(1, min(days, 30))
        query = f"""
            WITH two_days AS (
                SELECT
                    *, 
                    LAG(buybox_seller_name, 1) OVER (PARTITION BY asin ORDER BY snapshot_date) as prev_day_buybox_seller
                FROM `{self.full_table_id}`
                WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
            )
            SELECT
                t.snapshot_date,
                t.asin,
                t.buybox_seller_name,
                t.prev_day_buybox_seller
            FROM two_days t
            WHERE 
                (LOWER(t.buybox_seller_name) = LOWER('{seller_name}') AND LOWER(t.prev_day_buybox_seller) != LOWER('{seller_name}'))
                OR (LOWER(t.buybox_seller_name) != LOWER('{seller_name}') AND LOWER(t.prev_day_buybox_seller) = LOWER('{seller_name}'))
            ORDER BY t.snapshot_date DESC, t.asin
        """
        return self._execute_query(
            query,
            empty_message=f"No Buy Box changes found for seller '{seller_name}' in the last {days} days.",
        )

    def get_general_data(self) -> str:
        """
        Fetches a general overview of the data from the last 14 days, limited to 100 rows.
        Use this tool if no other tool is suitable for the user's query.
        """
        query = f"""
            SELECT
                snapshot_date,
                asin,
                category_label,
                pdp_total_price,
                buybox_seller_name,
                buybox_is_amazon,
                buybox_stock,
                seller_id_count,
                listing_position
            FROM
                `{self.full_table_id}`
            WHERE
                snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            LIMIT 100
        """
        return self._execute_query(
            query,
            empty_message="No general data found in the last 14 days.",
        )

    def analyze_product_performance(self, asin: str, seller_name: str) -> str:
        """
        Analyzes a product's performance to provide a strategic recommendation.
        Calculates price gap against the Buy Box and headroom for winning products.
        
        Args:
            asin: The Amazon Standard Identification Number.
            seller_name: The name of the seller requesting the analysis.
        """
        # Using specific logic to join offers summary with price listing
        asin = asin.upper()
        query = f"""
            WITH my_price AS (
              SELECT asin, extracted_at, MIN(total_price) AS my_price
              FROM
                `etrendo-prd.amazon_silver_stg.amazon_coffee_machines_price_listing_flat`
              WHERE LOWER(seller_name) = LOWER('{seller_name}')
              AND DATE(extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
              GROUP BY asin, extracted_at
            )
            SELECT
              s.asin,
              s.extracted_at,
              s.buybox_seller_name AS buybox_seller_id,
              s.pdp_total_price AS buybox_price,
              s.min_total_price,
              s.buybox_is_amazon,
              m.my_price,

              -- Derived fields
              (m.my_price - s.pdp_total_price) AS price_gap,
              (m.my_price - s.min_total_price) AS headroom,
              (LOWER(s.buybox_seller_name) = LOWER('{seller_name}') OR s.buybox_seller_name IS NULL) AS am_i_buybox

            FROM `etrendo-prd.amazon_gold.amazon_coffee_machines_snapshot_category_daily` s
            LEFT JOIN my_price m
              ON s.asin = m.asin 
              AND DATE(s.extracted_at) = DATE(m.extracted_at)
            WHERE s.asin = '{asin}'
            AND DATE(s.extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            ORDER BY s.extracted_at DESC
            LIMIT 100
        """
        
        # Execute query
        try:
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            if not results:
                return f"No recent data found for ASIN {asin} (Seller: {seller_name})."
                
            row = results[0]
            
            # Extract values
            am_i_buybox = row.get('am_i_buybox', False)
            price_gap = row.get('price_gap')
            headroom = row.get('headroom')
            buybox_is_amazon = row.get('buybox_is_amazon')
            buybox_price = row.get('buybox_price')
            my_price = row.get('my_price')
            
            status = "WINNING" if am_i_buybox else "LOSING"
            reason = ""
            action = ""

            # --- Business Logic Rules ---
            
            if status == "LOSING":
                if price_gap is None:
                     reason = "Your price data is missing for this date."
                     action = "Check if your listing is active."
                elif price_gap > 0.10:
                    reason = f"You are losing because you are overpriced by €{price_gap:.2f}."
                    action = f"Decrease price to €{buybox_price:.2f} to compete."
                elif abs(price_gap) <= 0.05:
                    reason = "Your price is aligned with the Buy Box, but you are not winning."
                    action = "Check non-price factors: Shipping speed (FBA vs FBM), Seller Rating, or Stock location."
                elif price_gap < -0.05:
                    reason = f"You are cheaper by €{abs(price_gap):.2f}, but still losing."
                    action = "Do NOT lower price further. Improve delivery speed or seller metrics."
                
                if buybox_is_amazon:
                    reason += " (Note: Amazon holds the Buy Box, making it harder to win)."

            else: # WINNING
                if headroom and headroom >= 0.50:
                    reason = f"You are winning, and you have €{headroom:.2f} headroom above the minimum price."
                    action = "Test a small price increase to improve margins."
                else:
                    reason = "You are winning the Buy Box."
                    action = "Maintain current strategy. No price action required."

            return f"**Analysis for {asin}**\n\n**Status:** {status}\n**Reason:** {reason}\n**Recommendation:** {action}\n\n*Data Context: Buy Box: €{buybox_price}, Your Price: €{my_price}*"

        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return f"Error analyzing product: {e}"
