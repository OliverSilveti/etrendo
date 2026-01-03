
import uuid

import vertexai
from google.adk.agents import LlmAgent
from google.adk.apps import App
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import (
    InMemorySessionService,
)
from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from agent.config import config, prompts
from agent.tools.bq_tool import BigQueryTool

# Initialize Vertex AI
try:
    PROJECT_ID = config["vertex_ai"]["project_id"]
    LOCATION = config["vertex_ai"]["location"]
    vertexai.init(project=PROJECT_ID, location=LOCATION)
except Exception as e:
    raise Exception(f"Failed to initialize Vertex AI client: {e}")

# Build tools once and wrap BigQuery methods so ADK can call them.
_bq_tool = BigQueryTool()
TOOLS = [
    FunctionTool(_bq_tool.get_daily_pulse),
    FunctionTool(_bq_tool.get_price_competitiveness),
    FunctionTool(_bq_tool.get_stock_status),
    FunctionTool(_bq_tool.get_buy_box_changes),
    FunctionTool(_bq_tool.get_general_data),
]

# Create the agent
root_agent = LlmAgent(
    name="etrendo_agent",
    description="An agent that provides e-commerce insights from BigQuery.",
    model=config["vertex_ai"]["model_name"],
    tools=TOOLS,
    instruction=prompts["system_instruction"],
)

# Expose an ADK App so the CLI (`adk run` / `adk web`) can load this agent.
app = App(name="agent", root_agent=root_agent)

# Simple runner + session service for programmatic (or FastAPI) use.
_session_service = InMemorySessionService()
_runner = Runner(app=app, session_service=_session_service)
_USER_ID = "local-user"


def run_agent_query(query: str) -> str:
    """Runs a single-turn query through the ADK runner and returns text."""
    session_id = str(uuid.uuid4())
    _session_service.create_session_sync(
        app_name=app.name,
        user_id=_USER_ID,
        session_id=session_id,
    )
    events = _runner.run(
        user_id=_USER_ID,
        session_id=session_id,
        new_message=types.Content(
            role="user", parts=[types.Part.from_text(query)]
        ),
    )

    final_text: str | None = None
    for event in events:
        if event.author == "user":
            continue
        if event.is_final_response() and event.content and event.content.parts:
            text_parts = [part.text for part in event.content.parts if part.text]
            if text_parts:
                final_text = "".join(text_parts)
    return final_text or "No response generated."
