import os
import json
import argparse
import sys
import time
import random
import logging
from datetime import datetime, timezone

import requests
import pandas as pd
from google.cloud import storage
from serpapi import GoogleSearch

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the API key from the secret volume
try:
    with open("/etc/secrets/marketplace1-ingestion-serpapi-key", "r") as f:
        SERPAPI_API_KEY = f.read().strip()
except FileNotFoundError:
    raise ValueError("SERPAPI_API_KEY secret not found. Please make sure it is mounted correctly.")

if not SERPAPI_API_KEY:
    raise ValueError("SERPAPI_API_KEY is empty. Please check the secret value.")

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
        "s": config["sort"], "page": 1, "api_key": api_key
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
        
        # Update the search object for the next page
        search.params_dict.update(results.get("serpapi_pagination", {}))
        page_num += 1

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
    json_data = df.to_json(orient="records", indent=2, force_ascii=False)
    blob.upload_from_string(json_data, content_type='application/json')
    logging.info(f"Data saved to gs://{bucket_name}/{destination_blob_name}")

def main(args):
    """
    Main function to orchestrate the fetching and processing of Amazon products.
    """
    print("--- Script starting ---")

    # --- Base Configuration (from notebook) ---
    CRAWL_CONFIG = {
        "api_key": SERPAPI_API_KEY,
        "amazon_domain": "amazon.de",
        "language": "en_GB",
        "delivery_zip": "22085",
        "node": "16075991",
        "sort": "exact-aware-popularity-rank",
        "category_label": "washing_machines",
    }

    # --- Override with Command-Line Arguments ---
    # This allows for flexible test runs, e.g., fetching only 1 page.
    # A value of 0 means no limit.
    CRAWL_CONFIG["max_pages"] = args.max_pages
    if args.max_pages > 0:
        logging.info(f"Limiting fetch to a maximum of {args.max_pages} pages.")
    else:
        logging.info("Fetching all available pages (no page limit).")

    if args.node:
        CRAWL_CONFIG["node"] = args.node
        logging.info(f"Overriding node with command-line value: {args.node}")


    # 1. Fetch data from SerpAPI
    logging.info(f"Starting Amazon product fetch job for node '{CRAWL_CONFIG['node']}'...")
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
    # Get bucket name from environment variable.
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not bucket_name:
        logging.error("GCS_BUCKET_NAME environment variable not set. Cannot upload to GCS.")
        sys.exit("GCS_BUCKET_NAME not set.")

    category_label = CRAWL_CONFIG["category_label"]
    node = CRAWL_CONFIG["node"]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.json"
    destination_blob_name = f"{category_label}-{node}/{filename}"

    save_to_gcs(df, bucket_name, destination_blob_name)
    print(f"--- Script finished successfully. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Amazon product data using SerpAPI.")
    parser.add_argument("--max_pages", type=int, default=0, help="Maximum number of pages to fetch. 0 means no limit (default: 0 to fetch all).")
    parser.add_argument("--node", type=str, help="Amazon category node to scrape.")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory to save the output file.")
    
    args = parser.parse_args()
    main(args)