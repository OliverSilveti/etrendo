
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

## Core Questions & Capabilities

This agent is optimized to answer the following types of questions for E-commerce Sellers:

### 1. The "Daily Pulse" (Status Check)
*   "How am I doing today vs yesterday?"
*   "Which ASINs need my immediate attention today?"
*   "Did I lose the Buy Box for any top products overnight?"

### 2. Pricing Strategy
*   "Is my price competitive for ASIN X?"
*   "Are competitors undercutting me on my top selling items?"
*   "Has the market price for this category dropped recently?"

### 3. Stock & Availability
*   "Which products are running low on stock?"
*   "Did I lose the Buy Box due to stock issues?"

### 4. Competition
*   "Who is my main competitor for ASIN Y?"
*   "Are there new sellers appearing on my listings?"
