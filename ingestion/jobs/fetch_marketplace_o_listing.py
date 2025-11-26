import os
import json
import argparse
import sys
import time
import random
import logging
from datetime import datetime, timezone

import yaml
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

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

# --- Secret Management ---
# Placeholder for secret management. In a production environment, this should be
# handled securely, for example, by reading from a mounted secret volume or

# using a secret manager service. In the notebook, a User-Agent was hardcoded,
# which is not ideal. We will load it from the source configuration.
UA = None

def polite_session(user_agent):
    """Creates a requests session with a User-Agent."""
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    return s

def get_soup(session, url):
    """Fetches a URL and returns a BeautifulSoup object."""
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()  # Raise an exception for bad status codes
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

    return BeautifulSoup(r.content, 'html.parser')

def fetch_product_urls(config: dict) -> list:
    """
    Fetches all product URLs from a category page.
    
    Args:
        config (dict): A dictionary containing the source configuration.

    Returns:
        list: A list of product URLs.
    """
    session = polite_session(config["user_agent"])
    category_url = config["category_url"]
    logging.info(f"Fetching product URLs from {category_url}")

    soup = get_soup(session, category_url)
    if not soup:
        return []

    # This is an assumption based on common e-commerce website structures.
    # The actual implementation might need to be adjusted based on the
    # real HTML of otto.de.
    product_links = soup.find_all('a', {'class': 'product-link'})
    
    product_urls = [link['href'] for link in product_links]
    
    # Make URLs absolute
    absolute_product_urls = []
    for url in product_urls:
        if not url.startswith('http'):
            from urllib.parse import urljoin
            url = urljoin(category_url, url)
        absolute_product_urls.append(url)

    logging.info(f"Found {len(absolute_product_urls)} product URLs.")
    return absolute_product_urls


def fetch_product_details(session, product_url: str) -> dict:
    """
    Fetches and extracts product details from a product page.

    Args:
        session: A requests session object.
        product_url (str): The URL of the product page.

    Returns:
        dict: A dictionary containing the product details.
    """
    logging.info(f"Fetching details for {product_url}")
    soup = get_soup(session, product_url)
    if not soup:
        return None

    # These are assumptions about the HTML structure of the product page.
    # The selectors will need to be verified and adjusted.
    try:
        title = soup.find('h1', {'class': 'product-title'}).get_text(strip=True)
        price_raw = soup.find('span', {'class': 'price-final'}).get_text(strip=True)
        
        # Example of extracting more details
        # description = soup.find('div', {'id': 'description'}).get_text(strip=True)
        
        product_data = {
            "title": title,
            "price_raw": price_raw,
            "link": product_url,
            "extracted_at": datetime.now(timezone.utc).isoformat()
        }
        return product_data

    except AttributeError as e:
        logging.error(f"Error parsing product details for {product_url}: {e}")
        return None

def normalize_products_to_dataframe(products: list, category_label: str) -> pd.DataFrame:
    """
    Normalizes a list of product dictionaries into a clean Pandas DataFrame.

    Args:
        products (list): A list of product dictionaries.
        category_label (str): A label for the product category.

    Returns:
        pd.DataFrame: A DataFrame containing all products.
    """
    if not products:
        return pd.DataFrame()

    for p in products:
        p["category_label"] = category_label

    return pd.DataFrame(products)

def save_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Saves a DataFrame to a GCS bucket as a JSON file."""
    if df.empty:
        logging.warning("DataFrame is empty, skipping upload to GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    json_data = df.to_json(orient="records", lines=True, force_ascii=False)
    blob.upload_from_string(json_data, content_type='application/jsonl')
    logging.info(f"Data saved to gs://{bucket_name}/{destination_blob_name}")


def main(args):
    """
    Main function to orchestrate the fetching and processing of products.
    """
    logging.info("--- Script starting ---")

    # --- Get source configuration ---
    source_name = args.source_name
    source = next((s for s in sources_config["sources"] if s["name"] == source_name), None)
    if not source:
        sys.exit(f"❌ ERROR: Source '{source_name}' not found in sources.yaml")

    if not source.get("enabled", True):
        logging.warning(f"Source '{source_name}' is disabled. Exiting.")
        return

    CRAWL_CONFIG = source["parameters"]
    global UA
    UA = CRAWL_CONFIG.get("user_agent")
    if not UA:
        sys.exit("❌ ERROR: 'user_agent' not found in source parameters.")


    # 1. Fetch product URLs
    product_urls = fetch_product_urls(CRAWL_CONFIG)

    if not product_urls:
        logging.warning("No product URLs found. Exiting.")
        return
        
    # Limit for testing
    if args.max_products:
        product_urls = product_urls[:args.max_products]
        logging.info(f"Limiting processing to {args.max_products} products.")

    # 2. Fetch product details
    session = polite_session(UA)
    all_products = []
    for url in product_urls:
        details = fetch_product_details(session, url)
        if details:
            all_products.append(details)
        
        # Polite wait
        wait_time = random.uniform(1, 3)
        logging.info(f"⏳ Waiting {wait_time:.1f}s...")
        time.sleep(wait_time)


    if not all_products:
        logging.warning("No product details were successfully fetched. Exiting.")
        return

    # 3. Normalize data
    df = normalize_products_to_dataframe(all_products, CRAWL_CONFIG["category_label"])
    logging.info(f"Successfully normalized {len(df)} products.")
    print(df.head())

    # 4. Save to GCS
    bucket_name = gcp_config.get("ingestion", {}).get("gcs_bucket_name")
    if not bucket_name:
        logging.error("GCS bucket name not found in gcp_config.yaml. Cannot upload to GCS.")
        sys.exit("GCS bucket name not set in config.")
        
    category_label = CRAWL_CONFIG["category_label"]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.jsonl"
    destination_blob_name = f"{category_label}/{filename}"
    
    save_to_gcs(df, bucket_name, destination_blob_name)
    logging.info(f"--- Script finished successfully. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch product data from a marketplace.")
    parser.add_argument("source_name", type=str, help="The name of the source to process from sources.yaml.")
    parser.add_argument("--max_products", type=int, help="Maximum number of products to process. For testing purposes.")
    
    args = parser.parse_args()
    main(args)