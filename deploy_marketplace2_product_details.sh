#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="etrendo-prd"
REGION="europe-west1"
REPO="etrendo-repo"
SERVICE_NAME="marketplace2-product-details-ingestion"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}:latest"

echo "=============================================="
echo " Building and pushing image for Cloud Run Job "
echo "----------------------------------------------"
echo " Image: ${IMAGE}"
echo " Dockerfile: ingestion/marketplace2_product_details/Dockerfile"
echo " Platform: linux/amd64 (required by Cloud Run)"
echo "=============================================="

docker buildx build \
  --platform linux/amd64 \
  -f ingestion/marketplace2_product_details/Dockerfile \
  -t "${IMAGE}" \
  --push .

echo ""
echo ">>> ✅ Image pushed successfully!"
echo ""
echo "To run the job in Cloud Run, execute:"
echo "  gcloud run jobs execute ${SERVICE_NAME} --region=${REGION} --project=${PROJECT_ID} --args=fetch_marketplace2_product_details --args=marketplace2_product_details"
