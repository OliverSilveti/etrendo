import argparse
import os
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    """Load a secret from env var, mounted file, or Secret Manager."""
    env_candidates = [secret_name.upper().replace("-", "_")]
    if "username" in secret_name:
        env_candidates.append("OXYLABS_USERNAME")
    if "password" in secret_name:
        env_candidates.append("OXYLABS_PASSWORD")
    for env_name in env_candidates:
        val = os.getenv(env_name)
        if val:
            logging.info("Loaded secret '%s' from environment (%s).", secret_name, env_name)
            return val

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


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    """Split gs://bucket/path into (bucket, path)."""
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI must start with gs://")
    without_scheme = uri[5:]
    if "/" not in without_scheme:
        raise ValueError("GCS URI must include bucket and object path")
    bucket_name, blob_name = without_scheme.split("/", 1)
    if not bucket_name or not blob_name:
        raise ValueError("Invalid GCS URI; missing bucket or object path")
    return bucket_name, blob_name


def read_input_from_gcs(uri: str, max_items: Optional[int] = None) -> List[str]:
    """Read non-empty lines from a text object in GCS."""
    bucket_name, blob_name = _parse_gcs_uri(uri)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    data = blob.download_as_text()
    lines: List[str] = []
    for line in data.splitlines():
        val = line.strip()
        if val:
            lines.append(val)
            if max_items and len(lines) >= max_items:
                break
    return lines


def call_oxylabs(username: str, password: str, domain: str, asin: str, source: str, endpoint: str) -> Dict:
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


def extract_content(api_response: Dict) -> Optional[Dict]:
    """Extract the parsed content block from the Oxylabs response."""
    try:
        results = api_response.get("results", [])
        if not results:
            return None
        return results[0].get("content")
    except Exception:
        return None


def flatten_pricing(content: Dict, category_label: str, extracted_at: str, node_label: Optional[str] = None) -> pd.DataFrame:
    """Flatten pricing offers (with delivery options) and add product fields."""
    if not content or "pricing" not in content:
        return pd.DataFrame()

    rows = []
    for offer in content.get("pricing", []):
        delivery_opts = offer.get("delivery_options", [None])
        for d in delivery_opts:
            row = {
                "asin": content.get("asin"),
                "title": content.get("title"),
                "url": content.get("url"),
                "review_count": content.get("review_count"),
                "seller": offer.get("seller"),
                "price": offer.get("price"),
                "currency": offer.get("currency"),
                "price_shipping": offer.get("price_shipping"),
                "condition": offer.get("condition"),
                "rating_count": offer.get("rating_count"),
                "seller_id": offer.get("seller_id"),
                "seller_link": offer.get("seller_link"),
                "delivery": offer.get("delivery"),
                "delivery_type": d.get("type") if d else None,
                "delivery_date": d.get("date", {}).get("by") if d else None,
                "category_label": category_label,
                "node_label": node_label,
                "extracted_at": extracted_at,
            }
            rows.append(row)

    if not rows:
        return pd.DataFrame()
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Amazon pricing (Oxylabs) for a list of ASINs.")
    parser.add_argument("source_name", type=str, help="The source name in sources.yaml (e.g., marketplace1_price_listing).")
    parser.add_argument("--category-label", type=str, help="Override category label for tagging output and filenames.")
    parser.add_argument("--node-label", type=str, help="Optional node/segment label to append to category (e.g., 13528250031).")
    parser.add_argument("--input-file-gcs", type=str, help="GCS URI to a text file with one ASIN per line (gs://bucket/path).")
    parser.add_argument("--input-file", type=str, help="Local file with one ASIN per line. Use '-' for stdin.")
    parser.add_argument("--bq-table", type=str, help="BigQuery table in project.dataset.table format.")
    parser.add_argument("--bq-column", type=str, default="asin", help="Column name to read from BigQuery.")
    parser.add_argument("--bq-where", type=str, help="Optional WHERE clause (without 'WHERE').")
    parser.add_argument("--bq-distinct", action="store_true", help="Deduplicate values from BigQuery using DISTINCT.")
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
    domain = cfg.get("domain")
    if not domain:
        sys.exit("❌ 'domain' missing in source parameters.")
    oxylabs_source = cfg.get("source", "amazon_pricing")
    endpoint = cfg.get("endpoint", "https://realtime.oxylabs.io/v1/queries")
    category_label = args.category_label or cfg.get("category_label", args.source_name)
    node_label = args.node_label or cfg.get("node_label") or cfg.get("node")
    label_for_output = f"{category_label}-{node_label}" if node_label else category_label
    throttle_min = cfg.get("throttle_min_seconds", 0.05)
    throttle_max = cfg.get("throttle_max_seconds", 0.2)

    username_secret = cfg.get("oxylabs_username_secret_name", "marketplace1-price-oxylabs-username")
    password_secret = cfg.get("oxylabs_password_secret_name", "marketplace1-price-oxylabs-password")
    username = load_secret(username_secret)
    password = load_secret(password_secret)

    # Exactly one input source must be provided
    provided_sources = [bool(args.input_file_gcs), bool(args.input_file), bool(args.bq_table)]
    if sum(provided_sources) != 1:
        sys.exit("❌ Provide exactly one input source: --input-file-gcs or --input-file or --bq-table.")

    inputs: List[str] = []
    if args.input_file_gcs:
        inputs = read_input_from_gcs(args.input_file_gcs, args.max_items)
    elif args.input_file:
        if args.input_file == "-":
            inputs = [line.strip() for line in sys.stdin if line.strip()]
        else:
            inputs = read_input_from_file(args.input_file, args.max_items)
    elif args.bq_table:
        inputs = read_input_from_bigquery(args.bq_table, args.bq_column, args.bq_where, args.max_items, distinct=args.bq_distinct)

    if not inputs:
        logging.warning("No inputs to process. Exiting.")
        return

    logging.info("Processing %d ASINs via Oxylabs.", len(inputs))
    all_dfs: List[pd.DataFrame] = []
    failures: List[str] = []
    extracted_at = datetime.now(timezone.utc).isoformat()

    for idx, asin in enumerate(inputs, start=1):
        resp = call_oxylabs(username, password, domain, asin, oxylabs_source, endpoint)
        if "error" in resp and "results" not in resp:
            failures.append(asin)
        else:
            content = extract_content(resp)
            df_offer = flatten_pricing(content, label_for_output, extracted_at, node_label=node_label)
            if df_offer.empty:
                logging.warning("No pricing content for %s", asin)
                failures.append(asin)
            else:
                all_dfs.append(df_offer)

        if idx % 20 == 0:
            logging.info("Processed %d/%d ASINs.", idx, len(inputs))
        time.sleep(random.uniform(throttle_min, throttle_max))

    if not all_dfs:
        sys.exit("❌ No data to save (all requests failed or returned empty).")

    df = pd.concat(all_dfs, ignore_index=True)
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
        destination_blob_name = f"{label_for_output}/{filename}"
        save_to_gcs(df, bucket_name, destination_blob_name)

    if failures:
        logging.warning("Completed with %d failures (see logs).", len(failures))
    logging.info("✅ Job completed.")


def run(argv=None):
    main(argv)


if __name__ == "__main__":
    run()
