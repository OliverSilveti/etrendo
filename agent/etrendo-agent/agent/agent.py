
import vertexai
from google.adk.agents import LlmAgent
from agent.config import config, prompts
from agent.tools.bq_tool import BigQueryTool

# Initialize Vertex AI
try:
    PROJECT_ID = config["vertex_ai"]["project_id"]
    LOCATION = config["vertex_ai"]["location"]
    vertexai.init(project=PROJECT_ID, location=LOCATION)
except Exception as e:
    raise Exception(f"Failed to initialize Vertex AI client: {e}")

# Create the agent
root_agent = LlmAgent(
    name="etrendo_agent",
    description="An agent that provides e-commerce insights from BigQuery.",
    model=config["vertex_ai"]["model_name"],
    tools=[BigQueryTool()],
    instruction=prompts["system_instruction"],
)
