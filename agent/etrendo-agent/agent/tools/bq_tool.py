
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

def fetch_context(asins: Optional[List[str]] = None) -> str:
    """
    Fetches recent data from BigQuery for context.
    If ASINs are provided, filters by them. Otherwise, fetches top 10 rows.
    """
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]
    full_table_id = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    # Basic query to fetch last 7 days of data
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
            `{full_table_id}`
        WHERE
            snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    """

    if asins:
        formatted_asins = ", ".join([f"'{asin}'" for asin in asins])
        query += f" AND asin IN ({formatted_asins})"
    else:
        query += " LIMIT 50" # Limit to 50 rows if no specific ASINs to avoid huge context

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
