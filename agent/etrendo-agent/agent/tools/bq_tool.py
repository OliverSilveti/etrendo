import logging
from typing import List, Optional
from google.cloud import bigquery
from google.adk.tools import BaseTool
from agent.config import config

logger = logging.getLogger(__name__)

class BigQueryTool(BaseTool):
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

    def _execute_query(self, query: str) -> str:
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
                    AND buybox_seller_name = '{seller_name}'
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
        return self._execute_query(query)

    def get_price_competitiveness(self, asin: str) -> str:
        """
        Fetches pricing data for a specific ASIN over the last 14 days to assess price competitiveness.
        """
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
        return self._execute_query(query)

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
                AND buybox_seller_name = '{seller_name}'
            ORDER BY snapshot_date DESC, asin
        """
        return self._execute_query(query)

    def get_buy_box_changes(self, seller_name: str) -> str:
        """
        Fetches data to identify Buy Box wins and losses for a seller compared to the previous day.
        """
        query = f"""
            WITH two_days AS (
                SELECT
                    *, 
                    LAG(buybox_seller_name, 1) OVER (PARTITION BY asin ORDER BY snapshot_date) as prev_day_buybox_seller
                FROM `{self.full_table_id}`
                WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
            )
            SELECT
                t.snapshot_date,
                t.asin,
                t.buybox_seller_name,
                t.prev_day_buybox_seller
            FROM two_days t
            WHERE 
                (t.buybox_seller_name = '{seller_name}' AND t.prev_day_buybox_seller != '{seller_name}')
                OR (t.buybox_seller_name != '{seller_name}' AND t.prev_day_buybox_seller = '{seller_name}')
            ORDER BY t.snapshot_date DESC, t.asin
        """
        return self._execute_query(query)

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
        return self._execute_query(query)