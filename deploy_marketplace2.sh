#!/usr/bin/env bash
set -euo pipefail

###########################################
#  SETTINGS — customize ONLY if needed
###########################################
PROJECT_ID="etrendo-prd"
REGION="europe-west1"
REPO="etrendo-repo"
SERVICE_NAME="marketplace2-ingestion"   # align with your Cloud Run/Terraform naming
DOCKERFILE="ingestion/marketplace2/Dockerfile"

# This is the FULL image name used by Cloud Run
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}:latest"

###########################################
#  DEPLOY: Build & push AMD64 image
###########################################
echo "=============================================="
echo " Building and pushing image for Cloud Run Job "
echo "----------------------------------------------"
echo " Image: ${IMAGE}"
echo " Dockerfile: ${DOCKERFILE}"
echo " Platform: linux/amd64 (required by Cloud Run)"
echo "=============================================="

docker buildx build \
  --platform linux/amd64 \
  -f "${DOCKERFILE}" \
  -t "${IMAGE}" \
  --push .

echo ""
echo ">>> ✅ Image pushed successfully!"
echo ""

###########################################
#  Next steps
###########################################
echo "Update your Cloud Run Job (or Terraform vars) to use:"
echo "  Image: ${IMAGE}"
echo "  Args:  marketplace2"
echo "Then run the job, for example:"
echo "  gcloud run jobs execute ${SERVICE_NAME} --region=${REGION} --project=${PROJECT_ID}"
