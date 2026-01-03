
import logging
from typing import List, Optional
from google.cloud import bigquery
from agent.config import config

logger = logging.getLogger(__name__)

# Initialize BigQuery Client
try:
    PROJECT_ID = config["bigquery"]["project_id"]
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    raise

def fetch_context(asins: Optional[List[str]] = None, seller_name: Optional[str] = None) -> str:
    """
    Fetches recent data from BigQuery for context.
    If ASINs are provided, filters by them.
    If seller_name is provided, fetches ASINs where this seller has won the Buy Box recently.
    """
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]
    full_table_id = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    base_query = f"""
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
            `{full_table_id}`
        WHERE
            snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    """

    if asins:
        formatted_asins = ", ".join([f"'{asin}'" for asin in asins])
        query = base_query + f" AND asin IN ({formatted_asins})"
    elif seller_name:
        # If seller_name is provided, we first find the ASINs relevant to this seller
        # Then we fetch the full history for those ASINs to allow for comparison (Win/Loss analysis)
        query = f"""
            WITH relevant_asins AS (
                SELECT DISTINCT asin
                FROM `{full_table_id}`
                WHERE 
                    snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                    AND buybox_seller_name = '{seller_name}'
            )
            {base_query}
            AND asin IN (SELECT asin FROM relevant_asins)
        """
    else:
        query = base_query + " LIMIT 200" # Increased limit for general queries


    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        
        # Convert results to a string format suitable for the LLM
        data_rows = []
        # Get headers
        headers = [field.name for field in results.schema]
        data_rows.append(",".join(headers))

        for row in results:
            # Convert row values to string and join with comma
            row_values = [str(val) for val in row.values()]
            data_rows.append(",".join(row_values))
        
        return "\n".join(data_rows)
    except Exception as e:
        logger.error(f"BigQuery query failed: {e}")
        return "Error fetching data from BigQuery."
