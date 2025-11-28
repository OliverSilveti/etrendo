# Ingestion

This folder contains the data ingestion jobs for the project.

## Getting Started

This guide will walk you through setting up and running the data ingestion jobs for local development.

### Prerequisites

Make sure you have the following tools installed on your local machine:

- [Python](https://www.python.org/downloads/) (3.9 or later)
- [Pip](https://pip.pypa.io/en/stable/installation/)

### Installation

1.  **Install dependencies:**
    From the root of the project, run:
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

The ingestion jobs use configuration files located in the `ingestion/config` directory.

-   `gcp_config.yaml`: Contains GCP-specific configurations.
-   `sources.yaml`: Defines the data sources for ingestion.

You may need to update these files according to your environment and data sources.

### Running the Jobs

To run a specific ingestion job, use the `main.py` script with the `--job` argument. For example, to run the `fetch_marketplace1_listing` job:

```bash
python ingestion/main.py --job fetch_marketplace1_listing
```

Available jobs can be found in the `ingestion/jobs` directory.

### Marketplace2 container

- Marketplace2 code lives in `ingestion/marketplace2/fetch_marketplace2_listing.py` (no shim in `ingestion/jobs`).
- Dockerfile for the marketplace2 job: `ingestion/marketplace2/Dockerfile` (entrypoint `python -m ingestion.marketplace2.fetch_marketplace2_listing`).
- Build/push like marketplace1 via `./deploy_marketplace2.sh` (adjust PROJECT_ID/REGION/REPO/SERVICE_NAME as needed); the Cloud Run job should pass `marketplace2` as the argument.
- Infra/Terraform wiring is set to use the new image (`${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}:latest`) when you're ready.
