import argparse
import json
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import yaml
from google.cloud import bigquery, secretmanager, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load shared configs
try:
    with open("ingestion/config/gcp_config.yaml", "r") as f:
        gcp_config = yaml.safe_load(f)
    with open("ingestion/config/sources.yaml", "r") as f:
        sources_config = yaml.safe_load(f)
except FileNotFoundError as e:
    raise ValueError(f"Configuration file not found: {e}")


def load_secret(secret_name: str) -> str:
    """Load a secret from mounted file or Secret Manager."""
    file_path = Path("/etc/secrets") / secret_name
    if file_path.exists():
        logging.info("Loaded secret '%s' from mounted file.", secret_name)
        return file_path.read_text().strip()

    project_id = gcp_config.get("project_id")
    if not project_id:
        raise ValueError("GCP 'project_id' missing in gcp_config.yaml.")

    client = secretmanager.SecretManagerServiceClient()
    secret_version_path = client.secret_version_path(project_id, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_version_path})
    logging.info("Loaded secret '%s' from Secret Manager.", secret_name)
    return response.payload.data.decode("utf-8").strip()


def read_input_from_file(path: str, max_items: Optional[int] = None) -> List[str]:
    """Read non-empty lines from a local file."""
    lines: List[str] = []
    with open(path, "r") as f:
        for line in f:
            val = line.strip()
            if val:
                lines.append(val)
                if max_items and len(lines) >= max_items:
                    break
    return lines


def read_input_from_bigquery(table: str, column: str, where: Optional[str], max_items: Optional[int], distinct: bool = False) -> List[str]:
    """Read values from a BigQuery table/column with optional DISTINCT, WHERE, and limit."""
    client = bigquery.Client()
    select_kw = "DISTINCT" if distinct else ""
    sql = f"SELECT {select_kw} {column} AS val FROM `{table}`".replace("  ", " ")
    if where:
        sql += f" WHERE {where}"
    if max_items:
        sql += f" LIMIT {max_items}"
    logging.info("Running BigQuery query: %s", sql)
    rows = client.query(sql).result()
    return [row.val for row in rows if row.val]


def call_oxylabs(endpoint: str, username: str, password: str, asin: str, source: str, domain: str) -> Dict:
    """Call Oxylabs realtime API for a single ASIN and return parsed JSON."""
    payload = {
        "source": source,
        "domain": domain,
        "query": asin,
        "parse": True,
    }
    try:
        resp = requests.post(endpoint, auth=(username, password), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error("Oxylabs call failed for %s: %s", asin, e)
        status_code = None
        if hasattr(e, "response") and e.response is not None:
            status_code = e.response.status_code
        return {"error": str(e), "status_code": status_code}


def normalize_records(raw_records: List[Dict], input_items: List[str], category_label: str, node_label: Optional[str]) -> pd.DataFrame:
    """Pair inputs with raw API responses, add metadata, and flatten key fields."""
    rows = []
    extracted_at = datetime.now(timezone.utc).isoformat()

    def extract_content(payload: Dict) -> Optional[Dict]:
        try:
            results = payload.get("results", [])
            if results:
                return results[0].get("content")
        except Exception:
            return None
        return None

    for asin, payload in zip(input_items, raw_records):
        content = extract_content(payload) or {}
        category_value = content.get("category") or category_label
        rows.append(
            {
                "asin": asin,
                "category_label": category_label,
                "extracted_at": extracted_at,
                "node": node_label,
                # Commonly useful flattened fields from Oxylabs content (best-effort)
                "title": content.get("title"),
                "price": content.get("price"),
                "currency": content.get("currency"),
                "rating": content.get("rating"),
                "review_count": content.get("review_count"),
                "buybox_price": content.get("buybox_price"),
                "buybox_currency": content.get("buybox_currency"),
                "buybox_shipping": content.get("buybox_shipping"),
                "url": content.get("url"),
                "brand": content.get("brand"),
                "category": category_value,
                # Full raw payload serialized (avoid empty struct issues with external tables)
                "payload_raw": json.dumps(payload, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def save_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Save a DataFrame as JSONL to GCS."""
    if df.empty:
        logging.warning("DataFrame is empty, skipping upload to GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    data = df.to_json(orient="records", lines=True, force_ascii=False)
    blob.upload_from_string(data, content_type="application/jsonl")
    logging.info("Data saved to gs://%s/%s", bucket_name, destination_blob_name)


def process_asin(
    endpoint: str,
    username: str,
    password: str,
    asin: str,
    oxylabs_source: str,
    domain: str,
    throttle_min: float,
    throttle_max: float,
) -> Dict:
    """Call Oxylabs for one ASIN with a built-in throttle pause."""
    payload = call_oxylabs(endpoint, username, password, asin, oxylabs_source, domain)
    # Keep a pause per request to avoid hammering the endpoint when using threads
    time.sleep(random.uniform(throttle_min, throttle_max))
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Amazon product details via Oxylabs.")
    parser.add_argument("source_name", type=str, help="The source name in sources.yaml (e.g., marketplace1_product_details).")
    parser.add_argument("--input-file", type=str, help="Local file with one ASIN per line. Use '-' for stdin.")
    parser.add_argument("--bq-table", type=str, help="BigQuery table in project.dataset.table format.")
    parser.add_argument("--bq-column", type=str, default="asin", help="Column name to read from BigQuery.")
    parser.add_argument("--bq-where", type=str, help="Optional WHERE clause (without 'WHERE').")
    parser.add_argument("--bq-distinct", action="store_true", help="Deduplicate values from BigQuery using DISTINCT.")
    parser.add_argument("--max-items", type=int, help="Optional cap on items to process.")
    parser.add_argument("--no-upload", action="store_true", help="Save locally instead of GCS.")
    parser.add_argument("--local-dir", type=str, default="local_output", help="Local output directory when --no-upload is set.")
    parser.add_argument("--category-label", type=str, help="Override category label for output path/naming.")
    parser.add_argument("--max-workers", type=int, help="Number of concurrent Oxylabs requests (default from config or 6).")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    source = next((s for s in sources_config["sources"] if s["name"] == args.source_name), None)
    if not source:
        sys.exit(f"❌ Source '{args.source_name}' not found in sources.yaml")
    if not source.get("enabled", True):
        logging.warning("Source '%s' is disabled. Exiting.", args.source_name)
        return

    cfg = source["parameters"]
    endpoint = cfg.get("oxylabs_endpoint", "https://realtime.oxylabs.io/v1/queries")
    oxylabs_source = cfg.get("oxylabs_source", "amazon_product")
    domain = cfg.get("domain", "de")
    category_label = args.category_label or cfg.get("category_label", args.source_name)
    node_label = cfg.get("node") or cfg.get("node_label")
    username = load_secret(cfg.get("oxylabs_username_secret_name", "marketplace1-price-oxylabs-username"))
    password = load_secret(cfg.get("oxylabs_password_secret_name", "marketplace1-price-oxylabs-password"))
    throttle_min = cfg.get("throttle_min_seconds", 0.3)
    throttle_max = cfg.get("throttle_max_seconds", 0.8)
    max_workers = args.max_workers or cfg.get("max_workers") or 6
    if max_workers < 1:
        max_workers = 1

    # Exactly one input source must be provided
    provided_sources = [bool(args.input_file), bool(args.bq_table)]
    if sum(provided_sources) != 1:
        sys.exit("❌ Provide exactly one input source: --input-file or --bq-table.")

    inputs: List[str] = []
    if args.input_file:
        if args.input_file == "-":
            inputs = [line.strip() for line in sys.stdin if line.strip()]
        else:
            inputs = read_input_from_file(args.input_file, args.max_items)
    elif args.bq_table:
        inputs = read_input_from_bigquery(args.bq_table, args.bq_column, args.bq_where, args.max_items, distinct=args.bq_distinct)

    if not inputs:
        logging.warning("No inputs to process. Exiting.")
        return

    logging.info("Processing %d ASINs via Oxylabs with up to %d workers.", len(inputs), max_workers)
    failures: List[str] = []
    payloads_by_asin: Dict[str, Dict] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_asin,
                endpoint,
                username,
                password,
                asin,
                oxylabs_source,
                domain,
                throttle_min,
                throttle_max,
            ): asin
            for asin in inputs
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            asin = futures[future]
            try:
                payload = future.result()
            except Exception as exc:
                logging.error("Unexpected error for %s: %s", asin, exc)
                payload = {"error": str(exc)}
                failures.append(asin)

            if payload is None:
                payload = {"error": "Empty payload"}
                failures.append(asin)
            elif "error" in payload and "results" not in payload:
                failures.append(asin)

            payloads_by_asin[asin] = payload

            if idx % 20 == 0 or idx == len(inputs):
                logging.info("Processed %d/%d ASINs.", idx, len(inputs))

    # Preserve original order for downstream normalization
    payloads: List[Dict] = [payloads_by_asin.get(asin, {"error": "Missing result"}) for asin in inputs]

    df = normalize_records(payloads, inputs, category_label, node_label)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.jsonl"

    if args.no_upload:
        Path(args.local_dir).mkdir(parents=True, exist_ok=True)
        local_path = Path(args.local_dir) / filename
        df.to_json(local_path, orient="records", lines=True, force_ascii=False)
        logging.info("Saved locally to %s", local_path)
    else:
        bucket_name = cfg.get("gcs_bucket_name")
        if not bucket_name:
            sys.exit("❌ gcs_bucket_name missing in source parameters.")
        folder = f"{category_label}-{node_label}" if node_label else category_label
        destination_blob_name = f"{folder}/{filename}"
        save_to_gcs(df, bucket_name, destination_blob_name)

    logging.info("✅ Job completed.")


def run(argv=None):
    main(argv)


if __name__ == "__main__":
    run()
