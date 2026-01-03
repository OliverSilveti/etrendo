
# Etrendo Agent

This agent is designed to provide insights from the `etrendo-prd.amazon_gold.amazon_coffee_machines_snapshot_category_daily` BigQuery table. It uses Vertex AI to generate insights based on user queries.

## Project Structure

*   `agent/`: Main application package.
    *   `agent_app.py`: FastAPI application entry point.
    *   `config.py`: Configuration loading.
    *   `tools/`: Helper modules (e.g., BigQuery integration).
*   `config.yaml`: Configuration settings (Project ID, Model Name, etc.).
*   `prompts.yaml`: System prompts and templates for the LLM.
*   `requirements.txt`: Python dependencies.
*   `Dockerfile`: Container definition.
*   `deploy_etrendo_agent.sh`: Deployment script.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment:**
    The agent's configuration is managed in the `config.yaml` file.
    Prompts are managed in `prompts.yaml`.

## Deployment

The agent is deployed as a serverless container on Cloud Run using a shell script.

1.  **Run Deployment Script:**
    ```bash
    cd agent/etrendo-agent
    ./deploy_etrendo_agent.sh
    ```

This will build the Docker image, push it to the Artifact Registry, and deploy the agent to Cloud Run.

## Usage

Once deployed, the agent can be queried via its Cloud Run endpoint.

-   **GET /**: Returns a health check message.
-   **POST /query**: Takes a JSON payload with a "query" field (and optional "asins" list) and returns a response from the agent.
