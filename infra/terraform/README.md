# Terraform Infrastructure for Etrendo

This directory contains the Terraform code to provision the necessary infrastructure for the Etrendo project on Google Cloud Platform (GCP).

## Overview

This Terraform setup deploys a serverless data ingestion pipeline. It consists of a Cloud Run job that is triggered on a schedule by Cloud Scheduler. The job fetches data from a marketplace and stores it in a Google Cloud Storage (GCS) bucket.

## Prerequisites

- [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) (v1.0 or later)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- A GCP project with billing enabled
- Authenticated to GCP with `gcloud auth application-default login`

## Usage

1.  **Initialize Terraform:**
    ```bash
    terraform init
    ```

2.  **Review the plan:**
    ```bash
    terraform plan -var="project_id=your-gcp-project-id" -var="gcs_bucket_name=your-gcs-bucket-name"
    ```

3.  **Apply the changes:**
    ```bash
    terraform apply -var="project_id=your-gcp-project-id" -var="gcs_bucket_name=your-gcs-bucket-name"
    ```

## Inputs

| Name              | Description                                  | Type   | Default                    |
| ----------------- | -------------------------------------------- | ------ | -------------------------- |
| `project_id`      | The GCP project ID.                          | `string` | n/a                        |
| `region`          | The GCP region for resources.                | `string` | `europe-west1`             |
| `service_name`    | The name of the Cloud Run service.           | `string` | `marketplace1-ingestion`   |
| `gcs_bucket_name` | The name of the GCS bucket.                  | `string` | n/a                        |

## Resources

This Terraform code will create the following resources in your GCP project:

- **Cloud Run Job:** A containerized job to run the data ingestion script.
- **Cloud Scheduler Job:** A cron job to trigger the Cloud Run job on a weekly schedule.
- **Cloud Storage Bucket:** A bucket to store the ingested data.
- **Service Account:** A dedicated service account for the Cloud Run job with necessary permissions.
- **Secret Manager Secret:** A secret to store the SerpAPI API key.
- **Artifact Registry Repository:** A Docker image repository for the Cloud Run job.
- **IAM Policies:** Appropriate IAM policies to connect the resources.
