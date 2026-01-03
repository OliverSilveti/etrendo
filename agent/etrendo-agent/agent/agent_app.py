
import logging
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import vertexai
from vertexai.generative_models import GenerativeModel

from agent.config import config, prompts
from agent.tools.bq_tool import fetch_context

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Vertex AI
try:
    PROJECT_ID = config["vertex_ai"]["project_id"]
    LOCATION = config["vertex_ai"]["location"]
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    
    model = GenerativeModel(
        model_name=config["vertex_ai"]["model_name"],
        system_instruction=prompts["system_instruction"]
    )
    logger.info("Vertex AI client initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Vertex AI client: {e}")
    raise

class Query(BaseModel):
    query: str
    asins: Optional[List[str]] = None

@app.get("/")
def read_root():
    return {"message": "Etrendo Agent is running and ready for queries."}

@app.post("/query")
def query_agent(query: Query):
    logger.info(f"Received query: {query.query}")
    
    # 1. Fetch Context from BigQuery using the tool
    data_context = fetch_context(query.asins)
    
    # 2. Construct Prompt using Template
    prompt = prompts["context_template"].format(
        query=query.query,
        data_context=data_context
    )

    # 3. Generate Insight with Vertex AI
    try:
        response = model.generate_content(prompt)
        return {"response": response.text}
    except Exception as e:
        logger.error(f"Vertex AI generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
