#!/usr/bin/env bash
set -euo pipefail

# Ensure bucket exists under Terraform management.
# If the bucket already exists, import it into Terraform state.
# If it does not exist, create it with a targeted apply (safe for MVP).
# Run this from the repo root: `./scripts/ensure-bucket.sh`

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="$ROOT_DIR/infra/terraform"

echo "Using repo root: $ROOT_DIR"

# Read values from YAML config if not provided in env
if [ -z "${PROJECT_ID:-}" ] || [ -z "${GCS_BUCKET:-}" ]; then
  echo "Loading project and bucket from ingestion/config/gcp_config.yaml"
  PROJECT_ID=$(python3 - <<'PY'
import yaml,sys
cfg=yaml.safe_load(open('ingestion/config/gcp_config.yaml'))
print(cfg.get('project_id',''))
PY
  GCS_BUCKET=$(python3 - <<'PY'
import yaml,sys
cfg=yaml.safe_load(open('ingestion/config/gcp_config.yaml'))
print(cfg.get('ingestion',{}).get('gcs_bucket_name',''))
PY
fi

if [ -z "$PROJECT_ID" ] || [ -z "$GCS_BUCKET" ]; then
  echo "ERROR: PROJECT_ID or GCS_BUCKET not set or found in ingestion/config/gcp_config.yaml"
  exit 2
fi

echo "PROJECT_ID=$PROJECT_ID"
echo "GCS_BUCKET=$GCS_BUCKET"

command -v gsutil >/dev/null 2>&1 || { echo >&2 "gsutil required but not installed. Install gcloud SDK."; exit 3; }
command -v terraform >/dev/null 2>&1 || { echo >&2 "terraform required but not installed. See infra/terraform/README.md."; exit 4; }

cd "$TF_DIR"

echo "Initializing Terraform..."
terraform init -input=false

echo "Validating configuration..."
terraform validate || true

echo "Checking if bucket gs://$GCS_BUCKET exists..."
if gsutil ls -b "gs://$GCS_BUCKET" >/dev/null 2>&1; then
  echo "Bucket exists. Importing into Terraform state (if not already imported)."
  # Import may error if already imported; ignore non-zero but capture.
  if terraform state list | grep -q "google_storage_bucket.bucket"; then
    echo "Resource google_storage_bucket.bucket already in state. Skipping import."
  else
    echo "Importing..."
    terraform import -lock=false google_storage_bucket.bucket "projects/_/buckets/$GCS_BUCKET" || {
      echo "terraform import failed"; exit 5
    }
  fi
  echo "Done. Run 'terraform plan' to review any further changes."
else
  echo "Bucket does not exist. Creating it with targeted apply (only the bucket)."
  terraform apply -target=google_storage_bucket.bucket -var="project_id=$PROJECT_ID" -var="gcs_bucket_name=$GCS_BUCKET" -auto-approve
  echo "Bucket created. Run 'terraform plan' to review full configuration." 
fi

echo "Done."
