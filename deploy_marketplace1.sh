#!/usr/bin/env bash
set -euo pipefail

###########################################
#  SETTINGS — customize ONLY if needed
###########################################
PROJECT_ID="etrendo-prd"
REGION="europe-west1"
REPO="etrendo-repo"
SERVICE_NAME="marketplace1-ingestion"   # from your Terraform var.service_name

# This is the FULL image name used by Cloud Run and Terraform
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}:latest"

###########################################
#  DEPLOY: Build & push AMD64 image
###########################################
echo "=============================================="
echo " Building and pushing image for Cloud Run Job "
echo "----------------------------------------------"
echo " Image: ${IMAGE}"
echo " Platform: linux/amd64 (required by Cloud Run)"
echo "=============================================="

# Build for amd64 and push directly to Artifact Registry
docker buildx build \
  --platform linux/amd64 \
  -t "${IMAGE}" \
  --push .

echo ""
echo ">>> ✅ Image pushed successfully!"
echo ""

###########################################
#  Next steps
###########################################
echo "To run the job in Cloud Run, execute:"
echo "  gcloud run jobs execute ${SERVICE_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "Or go to GCP Console → Cloud Run → Jobs → '${SERVICE_NAME}' → Run job."
