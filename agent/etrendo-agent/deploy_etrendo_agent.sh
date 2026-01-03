#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Configuration
PROJECT_ID="etrendo-prd"
REGION="europe-west1"
SERVICE_NAME="etrendo-agent"
REPO_NAME="etrendo-repo"
IMAGE_TAG="latest"

# 1. Build and Push the Docker image
echo "Building and Pushing Docker image..."
docker buildx build \
  --platform linux/amd64 \
  --push \
  -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:${IMAGE_TAG} \
  .

# 3. Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:${IMAGE_TAG} \
  --platform managed \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --allow-unauthenticated \
  --service-account etrendo-agent@etrendo-prd.iam.gserviceaccount.com

echo "Deployment complete!"
