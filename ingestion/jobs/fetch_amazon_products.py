import os
import json
import argparse
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from serpapi import GoogleSearch

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
if not SERPAPI_API_KEY:
    raise ValueError("SERPAPI_API_KEY not found in environment variables. Please check your .env file.")

OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME")
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD")


def fetch_all_product_pages(config: dict) -> list:
    """
    Fetches all product pages from Amazon using SerpAPI until the max_pages limit is reached.

    Args:
        config (dict): A dictionary containing search parameters.

    Returns:
        list: A list of all pages returned by the API.
    """
    search_params = {
        "api_key": SERPAPI_API_KEY, # From .env
        "engine": config.get("engine", "amazon"),
        "amazon_domain": config.get("amazon_domain"),
        "search_term": config.get("search_term"),
        "node": config.get("node"),
        "sort": config.get("sort"),
        "language": config.get("language"),
        "delivery_zip": config.get("delivery_zip"),
    }
    # Use a proxy to avoid IP-based blocking, which is the likely cause of the error.
    if OXYLABS_USERNAME and OXYLABS_PASSWORD:
        proxy_url = f"http://{OXYLABS_USERNAME}:{OXYLABS_PASSWORD}@de.oxylabs.io:10000"
        search_params["proxy"] = proxy_url
        logging.info("Using Oxylabs proxy to route SerpAPI request.")

    # Filter out any None values from the config
    search_params = {k: v for k, v in search_params.items() if v is not None}

    logging.info(f"Constructed search parameters: {json.dumps(search_params, indent=2)}")

    search = GoogleSearch(search_params)
    all_pages = []
    page_num = 1

    while True:
        logging.info(f"Fetching page {page_num} for node {config['node']}...")
        results = search.get_dict()

        if "error" in results:
            logging.error(f"SerpAPI Error: {results['error']}")
            break

        all_pages.append(results)

        # Check for next page
        if "next" not in results.get("serpapi_pagination", {}):
            logging.info("No more pages to fetch. Reached the end.")
            break

        if page_num >= config["max_pages"]:
            logging.info(f"Reached max_pages limit of {config['max_pages']}.")
            break

        # Prepare for the next iteration
        search.params_dict.update(results.get("serpapi_pagination", {}))
        page_num += 1

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
            products.append({
                "page_number": page_index,
                "category_label": category_label,
                "node": page.get("search_parameters", {}).get("node"),
                "position": p.get("position"),
                "asin": p.get("asin"),
                "title": p.get("title"),
                "link": p.get("link"),
                "rating": p.get("rating"),
                "reviews": p.get("reviews"),
                "bought_last_month": p.get("bought_last_month"),
                "price_raw": p.get("price", {}).get("raw"),
                "currency": p.get("price", {}).get("currency"),
                "extracted_price": p.get("extracted_price"),
                "delivery": p.get("delivery"),
                "is_sponsored": p.get("is_sponsored", False),
                "extracted_at": p.get("extracted_at")
            })
    
    if not products:
        return pd.DataFrame()

    return pd.DataFrame(products)


def main(args):
    """
    Main function to orchestrate the fetching and processing of Amazon products.
    """
    print("--- Starting script ---")

    # --- Base Configuration (from notebook) ---
    CRAWL_CONFIG = {
        "amazon_domain": "amazon.de",
        "language": "en_GB",
        "search_term": "washing machine",
        "delivery_zip": "22085",
        "node": "16075991",
        "sort": "exact-aware-popularity-rank",
        "category_label": "washing_machines",
    }

    # --- Override with Command-Line Arguments ---
    # This allows for flexible test runs, e.g., fetching only 1 page.
    if args.max_pages:
        CRAWL_CONFIG["max_pages"] = args.max_pages
        logging.info(f"Overriding max_pages with command-line value: {args.max_pages}")
    else:
        # Default to 100 if not specified
        CRAWL_CONFIG["max_pages"] = 100

    if args.node:
        CRAWL_CONFIG["node"] = args.node
        logging.info(f"Overriding node with command-line value: {args.node}")

    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = "output"


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

    # 3. Save the results
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{CRAWL_CONFIG['category_label']}_{timestamp}.parquet"
    output_path = os.path.join(output_dir, filename)
    
    df.to_parquet(output_path, index=False)
    logging.info(f"Data saved to {output_path}")
    print(f"--- Script finished successfully. Output saved to {output_path} ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Amazon product data using SerpAPI.")
    parser.add_argument("--max_pages", type=int, help="Maximum number of pages to fetch.")
    parser.add_argument("--node", type=str, help="Amazon category node to scrape.")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory to save the output file.")
    
    args = parser.parse_args()
    main(args)