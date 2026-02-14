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
            ORDER BY snapshot_date DESC, pdp_total_price DESC
        """
        return self._execute_query(
            query,
            empty_message=f"No recent data found for seller '{seller_name}' in the last 2 days.",
        )

    def get_price_competitiveness(self, asin: str) -> str:
        """
        Fetches pricing data for a specific ASIN over the last 14 days to assess price competitiveness.
        """
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
                t.prev_day_buybox_seller,
                t.pdp_total_price
            FROM two_days t
            WHERE 
                (LOWER(t.buybox_seller_name) = LOWER('{seller_name}') AND LOWER(t.prev_day_buybox_seller) != LOWER('{seller_name}'))
                OR (LOWER(t.buybox_seller_name) != LOWER('{seller_name}') AND LOWER(t.prev_day_buybox_seller) = LOWER('{seller_name}'))
            ORDER BY t.snapshot_date DESC, t.pdp_total_price DESC
            LIMIT 50
        """
        return self._execute_query(
            query,
            empty_message=f"No Buy Box changes found for seller '{seller_name}' in the last {days} days.",
        )

    def get_general_data(self) -> str:
        """
        Fetches a general overview of the data from the last 14 days, limited to 100 rows.
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
                min_total_price
            FROM
                `{self.full_table_id}`
            WHERE
                snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            ORDER BY snapshot_date DESC, pdp_total_price DESC
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
        """
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
              s.snapshot_date,
              s.buybox_seller_name,
              s.pdp_total_price AS buybox_price,
              s.min_total_price,
              s.buybox_is_amazon,
              m.my_price,

              -- Derived fields
              (m.my_price - s.pdp_total_price) AS price_gap,
              (m.my_price - s.min_total_price) AS headroom,
              (LOWER(s.buybox_seller_name) = LOWER('{seller_name}') OR s.buybox_seller_name IS NULL) AS am_i_buybox

            FROM `{self.full_table_id}` s
            LEFT JOIN my_price m
              ON s.asin = m.asin 
              AND s.snapshot_date = DATE(m.extracted_at)
            WHERE s.asin = '{asin}'
            AND s.snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            ORDER BY s.snapshot_date DESC
            LIMIT 1000
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            if not results:
                return f"No recent data found for ASIN {asin} (Seller: {seller_name})."
                
            row = results[0]
            
            # Extract values
            am_i_buybox = row.get('am_i_buybox', False)
            buybox_price = float(row.get('buybox_price') or 0)
            my_price = row.get('my_price')
            min_price = float(row.get('min_total_price') or 0)
            
            # Use SQL derived fields as default
            price_gap = row.get('price_gap')
            headroom = row.get('headroom')

            # --- Intelligence Overlay (Common Sense) ---
            if am_i_buybox:
                # If winning, my_price is buybox_price even if join failed
                if my_price is None or my_price == 0:
                    my_price = buybox_price
                
                # If winning, gap is 0 by definition
                price_gap = 0.0
                
                # If winning, headroom is my_price - market_min
                if headroom is None:
                    headroom = my_price - min_price
            
            # Final conversion to float for display
            my_price_display = float(my_price) if my_price else None
            price_gap = float(price_gap) if price_gap is not None else None
            headroom = float(headroom) if headroom is not None else None

            buybox_is_amazon = row.get('buybox_is_amazon')
            
            status = "WINNING" if am_i_buybox else "LOSING"
            reason = ""
            action = ""

            if status == "LOSING":
                if price_gap is None:
                     reason = "Your specific price data is missing in the offers table for this date."
                     action = f"Verify your listing status. Ensure your price is competitive with the Buy Box price of €{buybox_price:.2f}."
                elif price_gap > 0.10:
                    reason = f"You are losing because you are overpriced by €{price_gap:.2f}."
                    action = f"Decrease price to €{buybox_price:.2f} to regain the Buy Box immediately."
                elif abs(price_gap) <= 0.05:
                    reason = "Your price is aligned with the Buy Box, but you are not winning."
                    action = "Check non-price factors: Shipping speed (FBA vs FBM) or Seller Rating."
                elif price_gap < -0.05:
                    reason = f"You are cheaper by €{abs(price_gap):.2f}, but still losing."
                    action = "Do NOT lower price further. Focus on improving delivery speed or account health."
                
                if buybox_is_amazon:
                    reason += " (Note: Buy Box held by Amazon)."

            else: # WINNING
                if headroom and headroom >= 0.50:
                    reason = f"You are winning, and you have €{headroom:.2f} headroom above the minimum market price."
                    action = "Test a small price increase (€0.20 - €0.50) to improve margins."
                else:
                    reason = f"You are winning the Buy Box at €{buybox_price:.2f}."
                    action = "Maintain current strategy. No price action required."

            return f"### Analysis for {asin}\n\n**Status:** {status}\n**Diagnosis:** {reason}\n**Top Action:** {action}\n\n*Market Context: Buy Box: €{buybox_price:.2f}, Your Price: €{my_price_display if my_price_display else 'N/A'}*"

        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return f"Error analyzing product: {e}"

    def get_portfolio_health_check(self, seller_name: str) -> str:
        """
        Provides a high-level summary of the seller's entire portfolio health.
        Segments products by 'Winning', 'At Risk' (recently lost), and 'Losing'.
        Uses window functions to get the latest status for every product.
        Filters strictly for ASINs where the user is listed in the price_listing_flat table.
        """
        query = f"""
            WITH my_active_asins AS (
              -- Identify "My Portfolio" by finding where I am listed as a seller recently
              SELECT DISTINCT asin 
              FROM `etrendo-prd.amazon_silver_stg.amazon_coffee_machines_price_listing_flat`
              WHERE LOWER(seller_name) = LOWER('{seller_name}')
              AND DATE(extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ),
            latest_snapshots AS (
              SELECT 
                s.asin,
                s.snapshot_date,
                s.buybox_seller_name,
                s.pdp_total_price,
                -- Get the PREVIOUS winner to detect changes
                LAG(s.buybox_seller_name) OVER (PARTITION BY s.asin ORDER BY s.snapshot_date) as prev_winner,
                -- Rank rows to get the LATEST snapshot per ASIN
                ROW_NUMBER() OVER (PARTITION BY s.asin ORDER BY s.snapshot_date DESC) as recency_rank
              FROM `{self.full_table_id}` s
              JOIN my_active_asins m ON s.asin = m.asin  -- STRICT FILTER: Only include my products
              WHERE s.snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            ),
            my_portfolio AS (
              SELECT 
                *,
                (LOWER(buybox_seller_name) = LOWER('{seller_name}')) as is_winning,
                (LOWER(prev_winner) = LOWER('{seller_name}') AND LOWER(buybox_seller_name) != LOWER('{seller_name}')) as just_lost
              FROM latest_snapshots
              WHERE recency_rank = 1  -- Only the latest status for each ASIN
            )
            SELECT 
              COUNT(asin) as total_products,
              COUNTIF(is_winning) as winning_count,
              COUNTIF(just_lost) as at_risk_count,
              STRING_AGG(CASE WHEN NOT is_winning THEN asin END, ', ' LIMIT 20) as losing_asins,
              STRING_AGG(CASE WHEN is_winning THEN asin END, ', ' LIMIT 20) as winning_asins
            FROM my_portfolio
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            if not results:
                return f"No portfolio data found for {seller_name}."
            
            row = results[0]
            total = row.get('total_products', 0)
            win = row.get('winning_count', 0)
            risk = row.get('at_risk_count', 0)
            losing_list = row.get('losing_asins', 'None')
            winning_list = row.get('winning_asins', 'None')
            rate = (win / total * 100) if total > 0 else 0
            
            return (
                f"### Portfolio Health: {seller_name}\n"
                f"- **Total Products Monitored:** {total}\n"
                f"- **Buy Box Win Rate:** {rate:.1f}%\n"
                f"- **At Risk (Lost today):** {risk} products\n\n"
                f"**Detailed Breakdown:**\n"
                f"- **Winning:** {winning_list}\n"
                f"- **Losing:** {losing_list}\n\n"
                f"**Intelligence Insight:** " + 
                ("Your portfolio is healthy." if rate > 70 else "Significant revenue is at risk. Priority: Check Buy Box losses.")
            )
        except Exception as e:
            logger.error(f"Portfolio check failed: {e}")
            return f"Error fetching portfolio: {e}"

    def get_asin_raw_history(self, asin: str) -> str:
        """
        Returns the raw market history for an ASIN for the last 14 days.
        Use this when you need to perform free-form reasoning about market trends.
        """
        asin = asin.upper()
        query = f"""
            SELECT 
                snapshot_date,
                min_total_price,
                buybox_seller_name,
                buybox_is_amazon
            FROM `{self.full_table_id}`
            WHERE asin = '{asin}'
            AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            ORDER BY snapshot_date DESC
        """
        return self._execute_query(query)

    def get_competitor_landscape(self, seller_name: str, asins: Optional[List[str]] = None) -> str:
        """
        Identifies active rivals (Buy Box winners) and silent threats (listed sellers) 
        for the given seller's products.
        
        Args:
            seller_name: The name of the seller (e.g. 'AeroPress').
            asins: Optional list of ASINs to focus on. If empty, analyzes entire portfolio.
        """
        # Format ASIN filter
        asin_filter = ""
        if asins:
            formatted_asins = ", ".join([f"'{a.upper()}'" for a in asins])
            asin_filter = f"AND asin IN ({formatted_asins})"
            
        # SQL 1: Find who is winning Buy Box on our products (Rivals)
        rivals_query = f"""
            SELECT 
                buybox_seller_name as competitor,
                COUNT(*) as win_count,
                AVG(pdp_total_price) as avg_price
            FROM `{self.full_table_id}`
            WHERE LOWER(buybox_seller_name) != LOWER('{seller_name}')
            AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            {asin_filter}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        """
        
        # SQL 2: Find who else is listed on our products (Shadow Competitors)
        shadow_query = f"""
            SELECT 
                seller_name as competitor,
                COUNT(*) as listing_presence,
                MIN(total_price) as min_offered_price
            FROM `etrendo-prd.amazon_silver_stg.amazon_coffee_machines_price_listing_flat`
            WHERE LOWER(seller_name) != LOWER('{seller_name}')
            AND DATE(extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            {asin_filter}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        """
        
        try:
            rivals_data = self._execute_query(rivals_query)
            shadow_data = self._execute_query(shadow_query)
            
            return (
                f"### Competitor Analysis for {seller_name}\n\n"
                f"**1. Active Rivals (Holding the Buy Box):**\n"
                f"{rivals_data}\n\n"
                f"**2. Shadow Competitors (Listed behind the scenes):**\n"
                f"{shadow_data}\n\n"
                f"**Intelligence Note:** Rivals are actively taking sales. Shadow competitors are threats if they lower prices or if you go out of stock."
            )
        except Exception as e:
            logger.error(f"Competitor analysis failed: {e}")
            return f"Error analyzing competition: {e}"
