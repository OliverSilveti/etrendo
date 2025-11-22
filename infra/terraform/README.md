# from infra/terraform
ls -l ../../ingestion/config/gcp_config.yaml

PROJECT_ID=$(python3 -c "import yaml; print(__import__('yaml').safe_load(open('../../ingestion/config/gcp_config.yaml'))['project_id'])")
GCS_BUCKET=$(python3 -c "import yaml; print(__import__('yaml').safe_load(open('../../ingestion/config/gcp_config.yaml'))['ingestion']['gcs_bucket_name'])")
export PROJECT_ID GCS_BUCKET

# verify values are non-empty
echo "PROJECT_ID='${PROJECT_ID}'"
echo "GCS_BUCKET='${GCS_BUCKET}'"# Terraform Infrastructure for Etrendo

This directory contains the Terraform code to provision the infrastructure for the Etrendo project on Google Cloud Platform (GCP). It provisions a Cloud Run job (containerized ingestion), Cloud Scheduler trigger, a GCS bucket, Secret Manager secret, Artifact Registry repository, and related IAM.

## Important notes

- Terraform will create billable GCP resources. Use a test project if you want to avoid affecting production.
- The Terraform code includes a `null_resource` provisioner that runs a local Docker build and pushes the image to Artifact Registry during `apply`. That requires Docker, `gcloud` auth for Artifact Registry, and may be slow. See "Avoid Docker build on apply" below.

## Supported Terraform versions

- Terraform v1.0+ (use the latest stable v1.x). The provider constraint requires `hashicorp/google` >= 4.50.0.

## Install Terraform (macOS / zsh)

Recommended (Homebrew):

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
terraform version
```

Alternative — tfenv (version manager):

```bash
brew install tfenv
tfenv install latest
tfenv use latest
terraform version
```

Manual download:

1. Download the appropriate archive from https://www.terraform.io/downloads
2. Unzip and move the `terraform` binary to a directory on your `PATH` (e.g. `/usr/local/bin` or `/opt/homebrew/bin`).
3. Verify with `terraform version`.

## Quick safe test steps (no resources created)

Run these from the repo root or `infra/terraform` directory.

```bash
cd infra/terraform
terraform init
terraform validate

# Extract variables from repo config (examples):
PROJECT_ID=$(python3 -c "import yaml; print(__import__('yaml').safe_load(open('../ingestion/config/gcp_config.yaml'))['project_id'])")
GCS_BUCKET=$(python3 -c "import yaml; print(__import__('yaml').safe_load(open('../ingestion/config/gcp_config.yaml'))['ingestion']['gcs_bucket_name'])")

terraform plan -var="project_id=${PROJECT_ID}" -var="gcs_bucket_name=${GCS_BUCKET}" -out=tfplan
terraform show tfplan
```

## Deploying the Cloud Run job (detailed step-by-step)

Follow these steps to deploy the Cloud Run job and scheduler. Run commands one at a time and read the short notes after each step.

1) Set variables (from repo root or `infra/terraform` — adjust paths if needed)
```bash
# from repo root
export PROJECT_ID=$(awk -F': *' '/^project_id:/ {gsub(/"/,"",$2); print $2; exit}' ingestion/config/gcp_config.yaml)
export GCS_BUCKET=$(awk -F': *' '/gcs_bucket_name:/ {gsub(/"/,"",$2); print $2; exit}' ingestion/config/gcp_config.yaml)
echo "PROJECT_ID=${PROJECT_ID}" "GCS_BUCKET=${GCS_BUCKET}"
```

2) Authenticate to GCP (local dev — interactive)
```bash
gcloud auth application-default login
gcloud config set project "${PROJECT_ID}"
```

3) Ensure Docker is installed and configure Artifact Registry auth
```bash
# Install Docker Desktop on macOS (if needed)
brew install --cask docker
open -a Docker
while ! docker info >/dev/null 2>&1; do sleep 1; done
gcloud auth configure-docker europe-west1-docker.pkg.dev --quiet
```

4) Ensure secret value (SerpAPI) exists in Secret Manager
```bash
# use the service name defined in variables.tf (default: marketplace1-ingestion)
SECRET_ID="marketplace1-ingestion-serpapi-key"
printf '%s' "YOUR_SERPAPI_KEY" | gcloud secrets create "${SECRET_ID}" --data-file=- --project="${PROJECT_ID}" || \
	printf '%s' "YOUR_SERPAPI_KEY" | gcloud secrets versions add "${SECRET_ID}" --data-file=- --project="${PROJECT_ID}"
```

5) Ensure the GCS bucket is managed by Terraform (import if it already exists)
```bash
cd infra/terraform
terraform init
terraform validate
# If the bucket already exists and contains data, import it into TF state so TF will manage it
terraform import -input=false google_storage_bucket.bucket "${GCS_BUCKET}" || terraform import -input=false google_storage_bucket.bucket "projects/${PROJECT_ID}/buckets/${GCS_BUCKET}"
```

6) Review plan (safe — no changes yet)
```bash
terraform plan -var="project_id=${PROJECT_ID}" -var="gcs_bucket_name=${GCS_BUCKET}"
```

7) Apply (this will locally build & push the Docker image, then create the job and scheduler)
```bash
terraform apply -var="project_id=${PROJECT_ID}" -var="gcs_bucket_name=${GCS_BUCKET}"
```

Notes:
- The Terraform `null_resource` runs a local `docker build` and `docker push` during `apply`. Make sure Docker is running and `gcloud auth configure-docker` was run. If you cannot run Docker locally you can: (A) apply only the cloud resources that don't require the image, then push the image from another machine/CI; or (B) temporarily comment out the `null_resource` block before apply and push the image manually.
- If the bucket already contains data you care about, import it (step 5) — do not delete/recreate it.
- To protect the bucket from accidental deletion, consider adding `lifecycle { prevent_destroy = true }` to `google_storage_bucket.bucket` in `gcs.tf`.

8) Verify job and run a one-off execution
```bash
gcloud run jobs list --region=europe-west1 --project="${PROJECT_ID}"
gcloud run jobs execute marketplace1-ingestion --region=europe-west1 --project="${PROJECT_ID}"
# view logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=marketplace1-ingestion" --project="${PROJECT_ID}" --limit=50 --format="value(textPayload)"
```

## Helpful extras (optional)

- `scripts/ensure-bucket.sh`: helper which imports an existing bucket into TF state or creates it with a targeted apply. Use it from the repo root: `./scripts/ensure-bucket.sh`.
- `terraform.tfvars`: you can create a `terraform.tfvars` file to store `project_id` and `gcs_bucket_name` and avoid passing `-var` flags.
- For production/CI: use a dedicated Terraform service account, remote state (Terraform Cloud or a secure GCS backend with locking), and run `plan` in PRs and `apply` from an automated pipeline.

## Avoid Docker build on apply

The `null_resource.docker_build_push` runs a local `docker build` and `docker push` during `terraform apply`. To avoid that step when you just want to provision cloud resources:

- Option A: Apply only the bucket (or specific resources) using `-target`:

```bash
terraform apply -target=google_storage_bucket.bucket -var="project_id=${PROJECT_ID}" -var="gcs_bucket_name=${GCS_BUCKET}"
```

- Option B: Temporarily comment out or remove the `null_resource.docker_build_push` block in `iam.tf` before running `apply`.

## Creating a `terraform.tfvars` (optional)

Create `infra/terraform/terraform.tfvars` to avoid passing `-var` each time:

```hcl
project_id     = "etrendo-prd"
gcs_bucket_name = "amazon-product-listing-raw-etrendo-prd"
# service_name  = "marketplace1-ingestion"
# region        = "europe-west1"
```

Then run:

```bash
terraform plan
terraform apply
```

## Apply with caution

- `terraform plan` and `terraform validate` are safe checks. `terraform apply` will create resources and may perform local Docker builds/pushes.
- Ensure `gcloud` is authenticated and `gcloud config set project <PROJECT_ID>` is set to the target project.
- Ensure you have permissions to create the resources in the target project and that billing is enabled.

## Troubleshooting & tips

- If `terraform` command not found: install via Homebrew or set your `PATH` to include the directory containing the `terraform` binary.
- If Docker push fails: ensure `gcloud auth configure-docker` succeeds and Docker is running.
- If you want a scripted helper, consider adding `scripts/tf-test.sh` to run `init`, `validate`, `plan` and `show`.

If you'd like, I can add a `terraform.tfvars` generated from `ingestion/config/gcp_config.yaml` and a small helper script `scripts/tf-test.sh` — tell me which and I'll add them.
