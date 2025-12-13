import os
import json
import argparse
import sys
import time
import random
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

import yaml
import requests
import pandas as pd
from google.cloud import storage
from serpapi import GoogleSearch

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from YAML files
try:
    with open("ingestion/config/gcp_config.yaml", "r") as f:
        gcp_config = yaml.safe_load(f)
    with open("ingestion/config/sources.yaml", "r") as f:
        sources_config = yaml.safe_load(f)
except FileNotFoundError as e:
    raise ValueError(f"Configuration file not found: {e}")

# Read the API key
try:
    # First, try reading from the secret volume path (for cloud environments)
    with open("/etc/secrets/marketplace1-ingestion-serpapi-key", "r") as f:
        SERPAPI_API_KEY = f.read().strip()
    logging.info("Successfully loaded API key from mounted secret.")
except FileNotFoundError:
    # If the file is not found, fall back to fetching from Google Secret Manager (for local development)
    logging.info("Secret file not found. Falling back to Google Secret Manager.")
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = gcp_config.get("project_id")
        secret_name = "marketplace1-ingestion-serpapi-key"
        
        if not project_id:
            raise ValueError("GCP 'project_id' not found in gcp_config.yaml.")
            
        secret_version_path = client.secret_version_path(project_id, secret_name, "latest")
        response = client.access_secret_version(request={"name": secret_version_path})
        SERPAPI_API_KEY = response.payload.data.decode("UTF-8").strip()
        logging.info("Successfully loaded API key from Google Secret Manager.")
    except Exception as e:
        raise ValueError(f"Failed to fetch secret from Google Secret Manager. Please ensure you are authenticated (`gcloud auth application-default login`) and have permissions. Error: {e}")


if not SERPAPI_API_KEY:
    raise ValueError("SERPAPI_API_KEY is empty. Please check the secret value or environment variable.")

def fetch_all_product_pages(config: dict) -> list:
    """
    Fetches all product pages from Amazon using SerpAPI until the max_pages limit is reached.

    Args:
        config (dict): A dictionary containing search parameters.

    Returns:
        list: A list of all pages returned by the API.
    """
    api_key = config.get("api_key")
    if not api_key or api_key == "YOUR_SERPAPI_KEY":
        sys.exit("❌ ERROR: You must set your SerpAPI key in the config.")

    node = config.get("node")
    if not node:
        sys.exit("❌ ERROR: You must provide a category node ID in the config.")

    max_pages = config.get("max_pages")
    hard_limit = 100

    # Determine the effective number of pages to fetch
    if max_pages is None or max_pages <= 0 or max_pages > hard_limit:
        if max_pages and max_pages > hard_limit:
            logging.info(f"Capping request of {max_pages} pages to the hard limit of {hard_limit}.")
        effective_max_pages = hard_limit
    else:
        effective_max_pages = max_pages

    all_pages = []
    page_num = 1
    extracted_at = datetime.now(timezone.utc).isoformat()
    total_products = 0

    # Initial parameters for the first page
    params = {
        "engine": "amazon",
        "amazon_domain": config["amazon_domain"],
        "language": config["language"],
        "delivery_zip": config["delivery_zip"],
        "node": node,
        "s": config["sort"],
        "page": 1,
        "api_key": api_key
    }

    search = GoogleSearch(params)

    while True:
        logging.info(f"\n🌍 Fetching page {page_num}...")
        results = search.get_dict()

        # On the first page, log the total number of pages available from the API
        if page_num == 1 and "total_pages" in results.get("serpapi_pagination", {}):
            total_pages = results["serpapi_pagination"]["total_pages"]
            logging.info(f"ℹ️ API reports a total of {total_pages} pages available for this query.")

        if "error" in results:
            sys.exit(f"❌ API Error: {results['error']}")

        # add sponsorship flag + timestamp
        for p in results.get("organic_results", []):
            url_link = p.get("link", "")
            p["is_sponsored"] = "sspa/click" in url_link
            p["extracted_at"] = extracted_at

        all_pages.append(results)
        num_found = len(results.get("organic_results", []))
        total_products += num_found
        logging.info(f"📦 Found {num_found} products on page {page_num}")

        if effective_max_pages and page_num >= effective_max_pages:
            logging.info(f"⏹️ Reached page limit of {effective_max_pages}.")
            break

        # Check for and prepare the next page request
        if "next" not in results.get("serpapi_pagination", {}):
            logging.info("✅ No more pages.")
            break
        pagination = results["serpapi_pagination"]

        # Follow SerpAPI's next URL to capture all required params (e.g., rh filters)
        next_url = pagination["next"]
        next_qs = parse_qs(urlparse(next_url).query)
        # Flatten single-value lists
        next_params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in next_qs.items()}

        # Update the search params for the next call
        search.params_dict.update(next_params)

        # Keep our page counter in sync with what SerpAPI says
        try:
            page_num = int(next_params.get("page", page_num + 1))
        except ValueError:
            page_num += 1
        search.params_dict["page"] = page_num

        # Polite wait to avoid getting blocked
        wait_time = random.uniform(1, 3)
        logging.info(f"⏳ Waiting {wait_time:.1f}s before next page...")
        time.sleep(wait_time)

    return all_pages


def normalize_products_to_dataframe(pages: list, category_label: str) -> pd.DataFrame:
    """
    Normalizes the raw list of pages from SerpAPI into a clean Pandas DataFrame.

    Args:
        pages (list): The list of page results from fetch_all_product_pages.
        category_label (str): A label for the product category being scraped.

    Returns:
        pd.DataFrame: A DataFrame containing all products.
    """
    products = []
    for page_index, page in enumerate(pages, start=1):
        organic_results = page.get("organic_results", [])
        for p in organic_results:
            price_info = p.get("price")
            products.append({
                "page_number": page_index,
                "category_label": category_label,
                "node": page.get("search_parameters", {}).get("node"),
                "position": p.get("position"),
                "asin": p.get("asin"),
                "title": p.get("title"),
                "link": p.get("link", "").replace(r"\/", "/"),
                "rating": p.get("rating"),
                "reviews": p.get("reviews"),
                "bought_last_month": p.get("bought_last_month"),
                "price_raw": price_info.get("raw") if isinstance(price_info, dict) else None,
                "currency": price_info.get("currency") if isinstance(price_info, dict) else None,
                "extracted_price": p.get("extracted_price"),
                "delivery": p.get("delivery"),
                "is_sponsored": p.get("is_sponsored", False),
                "extracted_at": p.get("extracted_at")
            })
    
    if not products:
        return pd.DataFrame()

    return pd.DataFrame(products)


def save_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Saves a DataFrame to a GCS bucket as a JSON file."""
    if df.empty:
        logging.warning("DataFrame is empty, skipping upload to GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert DataFrame to JSON string and upload
    json_data = df.to_json(orient="records", 
                           #indent=2,
                           lines=True,
                           force_ascii=False)
    blob.upload_from_string(json_data, content_type='application/json')
    logging.info(f"Data saved to gs://{bucket_name}/{destination_blob_name}")

def main(args):
    """
    Main function to orchestrate the fetching and processing of Amazon products.
    """
    print("--- Script starting ---")

    # --- Get source configuration ---
    source_name = args.source_name
    source = next((s for s in sources_config["sources"] if s["name"] == source_name), None)
    if not source:
        sys.exit(f"❌ ERROR: Source '{source_name}' not found in sources.yaml")

    if not source.get("enabled", True):
        logging.warning(f"Source '{source_name}' is disabled. Exiting.")
        return

    CRAWL_CONFIG = source["parameters"]
    CRAWL_CONFIG["api_key"] = SERPAPI_API_KEY

    # --- Override with Command-Line Arguments ---
    if args.max_pages is not None:
        CRAWL_CONFIG["max_pages"] = args.max_pages
        if args.max_pages > 0:
            logging.info(f"Limiting fetch to a maximum of {args.max_pages} pages.")
        else:
            logging.info("Fetching all available pages (no page limit).")

    if args.node:
        CRAWL_CONFIG["node"] = args.node
        logging.info(f"Overriding node with command-line value: {args.node}")


    # 1. Fetch data from SerpAPI
    logging.info(f"Starting Amazon product fetch job for source '{source_name}' (node '{CRAWL_CONFIG['node']}') ...")
    raw_pages = fetch_all_product_pages(CRAWL_CONFIG)

    print(f"--- API Fetch Complete: Received {len(raw_pages)} page(s) of data. ---")

    if not raw_pages:
        logging.warning("No data was fetched. Exiting.")
        return

    # 2. Normalize data into a DataFrame
    df = normalize_products_to_dataframe(raw_pages, CRAWL_CONFIG["category_label"])
    print("--- Data Normalization Complete. Sample of the data: ---")
    print(df.head())
    print("-" * 50)
    logging.info(f"Successfully normalized {len(df)} products into a DataFrame.")

    # 3. Save the results to Google Cloud Storage
    # Prefer per-source bucket (no fallback to avoid cross-job collisions)
    bucket_name = CRAWL_CONFIG.get("gcs_bucket_name")
    if not bucket_name:
        logging.error("GCS bucket name not found in source parameters. Cannot upload to GCS.")
        sys.exit("GCS bucket name not set in source config.")

    category_label = CRAWL_CONFIG["category_label"]
    node = CRAWL_CONFIG["node"]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.json"
    destination_blob_name = f"{category_label}-{node}/{filename}"

    save_to_gcs(df, bucket_name, destination_blob_name)
    print(f"--- Script finished successfully. ---")


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser so it can be reused by wrapper entrypoints."""
    parser = argparse.ArgumentParser(description="Fetch Amazon product data using SerpAPI.")
    parser.add_argument("source_name", type=str, help="The name of the source to process from sources.yaml.")
    parser.add_argument("--max_pages", type=int, help="Maximum number of pages to fetch. Overrides the source config. 0 means no limit.")
    parser.add_argument("--node", type=str, help="Amazon category node to scrape. Overrides the source config.")
    return parser


def run(argv=None):
    """Parse CLI args (including when invoked via a wrapper) and execute."""
    if isinstance(argv, argparse.Namespace):
        args = argv
    else:
        parser = build_parser()
        args = parser.parse_args(argv)
    main(args)


if __name__ == "__main__":
    run()
