import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
import urllib.parse
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


def load_axesso_api_key(secret_name: str) -> str:
    """Load the Axesso API key from mounted secret file or Secret Manager."""
    file_path = Path("/etc/secrets") / secret_name
    if file_path.exists():
        logging.info("Loaded Axesso API key from mounted secret.")
        return file_path.read_text().strip()

    logging.info("Mounted secret not found. Trying Secret Manager.")
    project_id = gcp_config.get("project_id")
    if not project_id:
        raise ValueError("GCP 'project_id' missing in gcp_config.yaml.")

    client = secretmanager.SecretManagerServiceClient()
    secret_version_path = client.secret_version_path(project_id, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_version_path})
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


def read_input_from_bigquery(table: str, column: str, where: Optional[str], max_items: Optional[int]) -> List[str]:
    """Read values from a BigQuery table/column with optional WHERE and limit."""
    client = bigquery.Client()
    sql = f"SELECT {column} AS val FROM `{table}`"
    if where:
        sql += f" WHERE {where}"
    if max_items:
        sql += f" LIMIT {max_items}"
    logging.info("Running BigQuery query: %s", sql)
    rows = client.query(sql).result()
    return [row.val for row in rows if row.val]


def _encode_otto_url(url: str) -> str:
    """Encode Otto URL in the format Axesso expects: scheme intact, path encoded."""
    scheme, rest = url.split("://", 1)
    encoded_rest = urllib.parse.quote(rest, safe="=")
    return f"{scheme}:%2F%2F{encoded_rest}"


def call_axesso(endpoint: str, api_key: str, item: str) -> Dict:
    """Call Axesso Otto API for a single URL and return the parsed JSON."""
    encoded_url = _encode_otto_url(item)
    headers = {"axesso-api-key": api_key, "Cache-Control": "no-cache"}
    try:
        # Build the final URL manually to avoid double-encoding by requests
        request_url = f"{endpoint}?url={encoded_url}"
        resp = requests.get(request_url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error("Axesso call failed for %s: %s", item, e)
        status_code = None
        if hasattr(e, "response") and e.response is not None:
            status_code = e.response.status_code
        return {"error": str(e), "status_code": status_code}


def normalize_records(raw_records: Iterable[Dict], input_items: Iterable[str], category_label: str) -> pd.DataFrame:
    """Pair inputs with raw API responses and add metadata."""
    rows = []
    extracted_at = datetime.now(timezone.utc).isoformat()
    for item, payload in zip(input_items, raw_records):
        rows.append(
            {
                "input_value": item,
                "category_label": category_label,
                "extracted_at": extracted_at,
                "payload": payload,
            }
        )
    return pd.DataFrame(rows)


def save_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Save a DataFrame as JSONL to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    data = df.to_json(orient="records", lines=True, force_ascii=False)
    blob.upload_from_string(data, content_type="application/jsonl")
    logging.info("Data saved to gs://%s/%s", bucket_name, destination_blob_name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch marketplace2 product details via Axesso.")
    parser.add_argument("source_name", type=str, help="The source name in sources.yaml (e.g., marketplace2_product_details).")
    parser.add_argument("--input-file", type=str, help="Local file with one URL/ID per line. Use '-' for stdin.")
    parser.add_argument("--bq-table", type=str, help="BigQuery table in project.dataset.table format.")
    parser.add_argument("--bq-column", type=str, default="url", help="Column name to read from BigQuery.")
    parser.add_argument("--bq-where", type=str, help="Optional WHERE clause (without 'WHERE').")
    parser.add_argument("--max-items", type=int, help="Optional cap on items to process.")
    parser.add_argument("--no-upload", action="store_true", help="Save locally instead of GCS.")
    parser.add_argument("--local-dir", type=str, default="local_output", help="Local output directory when --no-upload is set.")
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
    endpoint = cfg.get("axesso_endpoint")
    if not endpoint:
        sys.exit("❌ axesso_endpoint missing in source parameters.")
    category_label = cfg.get("category_label", args.source_name)
    secret_name = cfg.get("axesso_secret_name", "marketplace2-details-axesso-key")
    api_key = load_axesso_api_key(secret_name)

    inputs: List[str] = []
    if args.input_file:
        if args.input_file == "-":
            inputs = [line.strip() for line in sys.stdin if line.strip()]
        else:
            inputs = read_input_from_file(args.input_file, args.max_items)
    elif args.bq_table:
        inputs = read_input_from_bigquery(args.bq_table, args.bq_column, args.bq_where, args.max_items)
    else:
        sys.exit("❌ Provide either --input-file or --bq-table.")

    if not inputs:
        logging.warning("No inputs to process. Exiting.")
        return

    logging.info("Processing %d items via Axesso.", len(inputs))
    payloads: List[Dict] = []
    for idx, item in enumerate(inputs, start=1):
        payloads.append(call_axesso(endpoint, api_key, item))
        if idx % 10 == 0:
            logging.info("Processed %d/%d items.", idx, len(inputs))
        time.sleep(random.uniform(0.5, 1.5))

    df = normalize_records(payloads, inputs, category_label)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{category_label}_{timestamp}.jsonl"

    if args.no_upload:
        Path(args.local_dir).mkdir(parents=True, exist_ok=True)
        local_path = Path(args.local_dir) / filename
        df.to_json(local_path, orient="records", lines=True, force_ascii=False)
        logging.info("Saved locally to %s", local_path)
    else:
        bucket_name = cfg.get("gcs_bucket_name")
        if not bucket_name:
            sys.exit("❌ gcs_bucket_name missing in source parameters.")
        destination_blob_name = f"{category_label}/{filename}"
        save_to_gcs(df, bucket_name, destination_blob_name)

    logging.info("✅ Job completed.")


def run(argv=None):
    main(argv)


if __name__ == "__main__":
    run()
