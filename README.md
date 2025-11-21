# E-commerce Insights & AI Agent

This repository contains the code for an MVP to provide e-commerce insights and power an AI agent using GCP, BigQuery, and Gemini.

## Getting Started

This guide will walk you through setting up the project for local development.

### Prerequisites

Make sure you have the following tools installed on your local machine:

- [Python](https://www.python.org/downloads/) (3.9 or later)
- [Pip](https://pip.pypa.io/en/stable/installation/)
- [Node.js](https://nodejs.org/en/download/) (for the frontend, optional for now)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone git@github.com:OliverSilveti/etrendo.git
    cd etrendo
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

The project uses a combination of environment variables and configuration files.

1.  **Google Cloud Authentication:**
    Make sure you are authenticated to your GCP account:
    ```bash
    gcloud auth application-default login
    ```

2.  **Secrets:**
    The project uses [Google Secret Manager](https://cloud.google.com/secret-manager) to handle secrets like API keys. The Terraform configuration will create the necessary secrets, but you will need to provide the values.

    For local development, you can use a `.env` file. Create a `.env` file in the root of the project and add the following environment variables:

    ```
    SERPAPI_API_KEY="your_serpapi_api_key"
    ```

    **Note:** Do not commit the `.env` file to version control.

## Usage

Each service can be run individually. Refer to the `README.md` file in each service's directory for specific instructions.

-   `agent`: The AI agent API.
-   `context_api`: The API for fetching context from BigQuery.
-   `ingestion`: The data ingestion jobs.
-   `infra/terraform`: The infrastructure as code.

## Project Structure

- **/ingestion**: Data ingestion pipelines (Cloud Functions/Run).
- **/analytics**: SQL transformation logic (dbt or Dataform) for the medallion architecture.
- **/agent**: The core AI agent logic and serving API (FastAPI/Gemini).
- **/context_api**: An API to fetch real-time context from BigQuery for the agent.
- **/notebooks**: Jupyter notebooks for exploration and prototyping.
- **/orchestration**: Infrastructure-as-code for orchestration (Cloud Scheduler, Workflows).
- **/frontend**: Placeholder for a user-facing application.
- **/infra**: General infrastructure-as-code (Terraform/Pulumi).

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
