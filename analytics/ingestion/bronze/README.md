# Bronze layer ingestion

Bronze pipelines are materialized via BigQuery external tables that point to JSONL files in GCS. Key notes:

- Format: newline-delimited JSON (JSONL). Schema is inferred or defined in BigQuery; no code runs here.
- Refresh: external tables reflect new data automatically when new JSONL files land in the linked GCS prefix (no manual job needed).
- Scope: this repo does not store bucket names; see your environment/secrets for the configured GCS locations.
- Usage: manage external table definitions in BigQuery (project/dataset configured per environment). Treat these as landing/raw sources for downstream silver/gold SQL in `analytics/sql`.
