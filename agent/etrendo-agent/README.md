
# Etrendo Agent

This agent is designed to provide insights from the `etrendo-prd.amazon_gold.amazon_coffee_machines_snapshot_category_daily` BigQuery table. It uses Vertex AI to generate insights based on user queries.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment:**
    The agent's configuration is managed in the `config.yaml` file. This file contains the BigQuery and Vertex AI settings.

## Deployment

The agent is deployed as a serverless container on Cloud Run using Terraform.

1.  **Initialize Terraform:**
    ```bash
    terraform init
    ```

2.  **Apply Terraform:**
    ```bash
    terraform apply
    ```

This will build the Docker image, push it to the Artifact Registry, and deploy the agent to Cloud Run.

## Usage

Once deployed, the agent can be queried via its Cloud Run endpoint.

-   **GET /**: Returns a "hello world" message.
-   **POST /query**: Takes a JSON payload with a "query" field and returns a response from the agent.
