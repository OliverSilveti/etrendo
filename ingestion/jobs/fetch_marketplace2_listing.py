import os
import json
import argparse
import sys
import time
import random
import logging
from datetime import datetime, timezone
import re
import html
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, urljoin

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

    def extract_products_from_soup(soup_obj, base_url):
        """Extract product links and names from a listing page (mirrors notebook logic)."""
        products = []
        for link in soup_obj.find_all("a", href=True):
            href = link["href"]
            if href.startswith("/p/"):
                url = urljoin(base_url, href)
                name = link.get_text(strip=True)
                if not name:
                    # Derive a human-ish title from slug if anchor text is empty
                    slug = href.rstrip("/").split("/")[-1]
                    name = slug.replace("-", " ").replace("_", " ")
                products.append({"title": name, "link": url})
        seen = set()
        unique = []
        for p in products:
            if p["link"] not in seen:
                seen.add(p["link"])
                unique.append(p)
        return unique

    def detect_pagination_and_step(soup_obj, urls_page1_count):
        total_pages = 1
        layout_hint = None
        offsets = []
        items = soup_obj.select("ul.reptile_paging li.reptile_paging__item")
        page_numbers = []
        for li in items:
            if li.find("span", class_="reptile_paging__dots") or "bottom-next" in (li.get("id") or ""):
                continue
            btn = li.find("button")
            if btn:
                txt = (btn.get_text() or "").strip()
                if txt.isdigit():
                    page_numbers.append(int(txt))
                dp = btn.get("data-page")
                if dp:
                    try:
                        dp_json = json.loads(html.unescape(dp))
                    except json.JSONDecodeError:
                        continue
                    if "o" in dp_json:
                        try:
                            offsets.append(int(dp_json["o"]))
                        except Exception:
                            pass
                    if "l" in dp_json and not layout_hint:
                        layout_hint = dp_json["l"]
        if page_numbers:
            total_pages = max(page_numbers)
        offset_step = None
        if offsets:
            offsets = sorted(set([o for o in offsets if isinstance(o, int) and o > 0]))
            if len(offsets) >= 1:
                diffs = []
                prev = 0
                for o in offsets:
                    if o > prev:
                        diffs.append(o - prev)
                        prev = o
                diffs = [d for d in diffs if d > 0]
                if diffs:
                    offset_step = min(diffs)
        if not offset_step:
            offset_step = max(1, urls_page1_count)
        return total_pages, offset_step, layout_hint

    def build_page_url(base_url, offset, layout_hint=None):
        parts = urlsplit(base_url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q["o"] = str(offset)
        if layout_hint and "l" not in q:
            q["l"] = layout_hint
        new_query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    all_products = []
    page1 = extract_products_from_soup(soup, category_url)
    logging.info(f"Page 1 products found: {len(page1)}")
    all_products.extend(page1)

    total_pages, offset_step, layout_hint = detect_pagination_and_step(soup, len(page1))
    logging.info(f"Detected total_pages={total_pages}, offset_step={offset_step}, layout_hint={layout_hint}")

    if total_pages > 1:
        for page_idx in range(2, total_pages + 1):
            offset = (page_idx - 1) * offset_step
            page_url = build_page_url(category_url, offset, layout_hint)
            logging.info(f"Fetching product URLs from page {page_idx}: {page_url}")
            page_soup = get_soup(session, page_url)
            if not page_soup:
                logging.warning(f"Skipping page {page_idx} due to fetch error.")
                break
            page_products = extract_products_from_soup(page_soup, category_url)
            logging.info(f"Page {page_idx} products found: {len(page_products)}")
            all_products.extend(page_products)
            time.sleep(random.uniform(1, 3))

    # Final de-dup
    final_products = []
    seen_all = set()
    for p in all_products:
        if p["link"] not in seen_all:
            seen_all.add(p["link"])
            final_products.append(p)

    logging.info(f"Found {len(final_products)} product URLs.")
    return final_products


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
    try:
        resp = session.get(product_url, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching detail page {product_url}: {e}")
        return None

    html = resp.text
    soup = BeautifulSoup(html, 'html.parser')

    title = None
    price_raw = None

    # Prefer structured data (JSON-LD) which Otto exposes on product pages.
    def _walk_ldjson(obj):
        if isinstance(obj, dict):
            yield obj
            for v in obj.values():
                yield from _walk_ldjson(v)
        elif isinstance(obj, list):
            for item in obj:
                yield from _walk_ldjson(item)

    # First pass: BeautifulSoup scripts
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        for obj in _walk_ldjson(data):
            if not isinstance(obj, dict):
                continue
            obj_type = obj.get("@type")
            if obj_type == "Product" or (isinstance(obj_type, list) and "Product" in obj_type):
                title = title or obj.get("name")
                offers = obj.get("offers")
                if isinstance(offers, dict):
                    price_raw = price_raw or offers.get("price") or offers.get("priceCurrency") or offers.get("lowPrice") or offers.get("highPrice")
                elif isinstance(offers, list):
                    for offer in offers:
                        if isinstance(offer, dict):
                            price_raw = price_raw or offer.get("price") or offer.get("priceCurrency") or offer.get("lowPrice") or offer.get("highPrice")
                            if price_raw:
                                break
                if title and price_raw:
                    break
        if title and price_raw:
            break

    # Second pass: regex extraction over raw HTML in case the script tag was malformed for the parser
    if not title or not price_raw:
        for match in re.finditer(
            r'<script[^>]*type=["\']application/ld\\+json["\'][^>]*>(.*?)</script>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            try:
                data = json.loads(match.group(1))
            except (json.JSONDecodeError, TypeError):
                continue
            for obj in _walk_ldjson(data):
                if not isinstance(obj, dict):
                    continue
                obj_type = obj.get("@type")
                if obj_type == "Product" or (isinstance(obj_type, list) and "Product" in obj_type):
                    title = title or obj.get("name")
                    offers = obj.get("offers")
                    if isinstance(offers, dict):
                        price_raw = price_raw or offers.get("price") or offers.get("priceCurrency") or offers.get("lowPrice") or offers.get("highPrice")
                    elif isinstance(offers, list):
                        for offer in offers:
                            if isinstance(offer, dict):
                                price_raw = price_raw or offer.get("price") or offer.get("priceCurrency") or offer.get("lowPrice") or offer.get("highPrice")
                                if price_raw:
                                    break
                    if title and price_raw:
                        break
            if title and price_raw:
                break

    # Fallbacks to meta tags / visible text
    if not title:
        meta_name = soup.find("meta", {"itemprop": "name"})
        if meta_name and meta_name.get("content"):
            title = meta_name["content"]
    if not title:
        og_title = soup.find("meta", {"property": "og:title"})
        if og_title and og_title.get("content"):
            title = og_title["content"]
    if not price_raw:
        meta_price = soup.find("meta", {"itemprop": "price"})
        if meta_price and meta_price.get("content"):
            price_raw = meta_price["content"]
    if not price_raw:
        price_span = soup.find("span", {"data-automation-id": lambda v: v and "price" in v})
        if price_span:
            price_raw = price_span.get_text(strip=True)
    if not price_raw:
        meta_price_amt = soup.find("meta", {"property": "product:price:amount"})
        if meta_price_amt and meta_price_amt.get("content"):
            price_raw = meta_price_amt["content"]
    if not price_raw:
        # Regex fallback: look for JSON price fields in the raw HTML (covers cases where the script is mangled)
        m = re.search(r'"price"\\s*:\\s*"?(\\d+[\\.,]\\d+)"?', html)
        if m:
            price_raw = m.group(1)

    # Last-resort fallback: derive a human-ish title from the slug so we don't drop the record entirely.
    if not title:
        slug = product_url.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ").replace("_", " ")
        logging.warning(f"Derived title from slug for {product_url}: {title}")

    if not title:
        logging.error(f"Error parsing product details for {product_url}: missing title.")
        return None

    product_data = {
        "title": title,
        "price_raw": price_raw,
        "link": product_url,
        "extracted_at": datetime.now(timezone.utc).isoformat()
    }
    return product_data

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


    # 1. Fetch product URLs (and listing titles)
    product_listing_entries = fetch_product_urls(CRAWL_CONFIG)

    if not product_listing_entries:
        logging.warning("No product URLs found. Exiting.")
        return
        
    # Limit for testing
    if args.max_products:
        product_listing_entries = product_listing_entries[:args.max_products]
        logging.info(f"Limiting processing to {args.max_products} products.")

    # 2. Build product records from listing data (skip detail pages for speed, like the notebook)
    all_products = []
    for entry in product_listing_entries:
        product_data = {
            "title": entry["title"],
            "price_raw": None,
            "link": entry["link"],
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }
        all_products.append(product_data)

    # Detail-page extraction retained for future use; currently unused to mirror notebook speed/output.
    # session = polite_session(UA)
    # for url in product_urls:
    #     details = fetch_product_details(session, url)
    #     if details:
    #         all_products.append(details)
    #     wait_time = random.uniform(1, 3)
    #     logging.info(f"⏳ Waiting {wait_time:.1f}s...")
    #     time.sleep(wait_time)


    if not all_products:
        logging.warning("No product details were successfully fetched. Exiting.")
        return

    # 3. Normalize data
    df = normalize_products_to_dataframe(all_products, CRAWL_CONFIG["category_label"])
    logging.info(f"Successfully normalized {len(df)} products.")
    print(df.head())

    # Original GCS-only flow retained for reference:
    # bucket_name = gcp_config.get("ingestion", {}).get("gcs_bucket_name")
    # if not bucket_name:
    #     logging.error("GCS bucket name not found in gcp_config.yaml. Cannot upload to GCS.")
    #     sys.exit("GCS bucket name not set in config.")
    # category_label = CRAWL_CONFIG["category_label"]
    # timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    # filename = f"{timestamp}.jsonl"
    # destination_blob_name = f"{category_label}/{filename}"
    # save_to_gcs(df, bucket_name, destination_blob_name)

    # 4. Save results
    category_label = CRAWL_CONFIG["category_label"]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{category_label}_{timestamp}.jsonl"

    if args.no_upload:
        # Local-only branch for dry runs to avoid writing to GCS
        os.makedirs(args.local_dir, exist_ok=True)
        local_path = os.path.join(args.local_dir, filename)
        df.to_json(local_path, orient="records", lines=True, force_ascii=False)
        logging.info(f"Data saved locally to {local_path}")
    else:
        bucket_name = gcp_config.get("ingestion", {}).get("gcs_bucket_name")
        if not bucket_name:
            logging.error("GCS bucket name not found in gcp_config.yaml. Cannot upload to GCS.")
            sys.exit("GCS bucket name not set in config.")

        destination_blob_name = f"{category_label}/{timestamp}.jsonl"
        save_to_gcs(df, bucket_name, destination_blob_name)

    logging.info(f"--- Script finished successfully. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch product data from a marketplace.")
    parser.add_argument("source_name", type=str, help="The name of the source to process from sources.yaml.")
    parser.add_argument("--max_products", type=int, help="Maximum number of products to process. For testing purposes.")
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip GCS upload and save output locally instead.",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        default="local_output",
        help="Directory to save JSONL output when --no-upload is set.",
    )
    
    args = parser.parse_args()
    main(args)
