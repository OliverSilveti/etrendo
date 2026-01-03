
# Etrendo Agent

This agent is designed to provide insights from the `etrendo-prd.amazon_gold.amazon_coffee_machines_snapshot_category_daily` BigQuery table. It uses Vertex AI to generate insights based on user queries.

## Project Structure

* `agent/`: Application package (ADK agent + optional FastAPI)
  * `agent.py`: Defines the root `LlmAgent`, ADK `App`, and a simple runner helper.
  * `app.py`: Optional FastAPI wrapper for HTTP access (uses the ADK runner).
  * `config.py`: Configuration loading.
  * `tools/`: Helper modules (BigQuery integration).
* `config.yaml`: Configuration settings (Project ID, Model Name, etc.).
* `prompts.yaml`: System prompts and templates for the LLM.
* `requirements.txt`: Python dependencies.
* `Dockerfile`: Container definition.
* `deploy_etrendo_agent.sh`: Deployment script.

## Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**
   Update `config.yaml` for project/dataset/table/model. Ensure Google auth is available (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`).

## Running Locally (ADK CLI)

From `agent/etrendo-agent`:
- Interactive CLI: `uv run adk run agent`
- ADK Web UI: `uv run adk web` (select `agent`)

## Running Locally (FastAPI, optional)

Still from `agent/etrendo-agent`:
```bash
uvicorn agent.app:app --host 0.0.0.0 --port 8080
```
Then:
- `GET /` health check
- `POST /query` with JSON `{"query": "...", "asins": ["ASIN1", ...]}` (the `asins` list is optional)

## Example Queries
- Daily pulse: "How am I doing today vs yesterday?" (uses `get_daily_pulse`)
- Buy Box changes: "Which ASINs did seller 'Yoer' lose the Buy Box on in the last 2 days?" (uses `get_buy_box_changes`)
- Price competitiveness: "Is my price competitive for ASIN B0FP2HTVYR?" (uses `get_price_competitiveness`)
- Stock status: "Which products for seller 'MyStore' are low on stock in the last 2 days?" (uses `get_stock_status`)
- General overview (fallback): "Give me a quick overview of recent activity." (uses `get_general_data`)

## Deployment

Cloud Run deployment via the provided script:
```bash
cd agent/etrendo-agent
./deploy_etrendo_agent.sh
```
This builds the image, pushes to Artifact Registry, and deploys to Cloud Run.

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
