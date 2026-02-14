# agent/app.py (or whatever module ADK loads)
# IMPORTANT: set env vars BEFORE importing ADK / google.genai

import os
import uuid
import vertexai

from agent.config import config, prompts
from agent.tools.bq_tool import BigQueryTool

# ---- Vertex / Gemini-on-Vertex configuration (from config.yaml) ----
PROJECT_ID = config["vertex_ai"]["project_id"]
LOCATION = config["vertex_ai"]["location"]
MODEL_NAME = config["vertex_ai"]["model_name"]  # e.g. "gemini-2.0-flash"

# Force google-genai (used under the hood) to use Vertex AI, not API-key Gemini
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", PROJECT_ID)
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", LOCATION)

# Initialize Vertex AI SDK (separate from google-genai, but good practice)
vertexai.init(project=PROJECT_ID, location=LOCATION)

# ---- Now it’s safe to import ADK / google.genai ----
from google.adk.agents import LlmAgent
from google.adk.apps import App
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.adk.tools.function_tool import FunctionTool
from google.adk.models.google_llm import Gemini
from google.genai import types


# ---- Tools ----
_bq_tool = BigQueryTool()
TOOLS = [
    FunctionTool(_bq_tool.get_daily_pulse),
    FunctionTool(_bq_tool.get_price_competitiveness),
    FunctionTool(_bq_tool.get_stock_status),
    FunctionTool(_bq_tool.get_buy_box_changes),
    FunctionTool(_bq_tool.get_general_data),
    FunctionTool(_bq_tool.analyze_product_performance),
    FunctionTool(_bq_tool.get_portfolio_health_check),
    FunctionTool(_bq_tool.get_asin_raw_history),
    FunctionTool(_bq_tool.get_competitor_landscape),
]

# ---- Global Session Service (Maintains Chat History) ----
_session_service = InMemorySessionService()
_USER_ID = "local-user"

# Note: App and Runner are now created per-request to avoid Streamlit/Async loop issues.

def run_agent_query(query: str, session_id: str | None = None) -> tuple[str, str, list[str]]:
    """Runs a single-turn query through the ADK runner and returns text + session_id + logs."""
    logs: list[str] = []

    if not session_id:
        session_id = str(uuid.uuid4())

    # ---- Fresh Connection Setup (Stability Fix) ----
    # Recreate the connection stack for every request to ensure it uses the active event loop.
    
    # 1. Model Client
    model = Gemini(
        model=MODEL_NAME,
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION,
    )

    # 2. Agent
    root_agent = LlmAgent(
        name="etrendo_agent",
        description="An agent that provides e-commerce insights from BigQuery.",
        model=model,
        tools=TOOLS,
        instruction=prompts["system_instruction"],
    )

    # 3. App
    app = App(name="agent", root_agent=root_agent)
    
    # 4. Runner (Injecting the GLOBAL session service to preserve memory)
    runner = Runner(app=app, session_service=_session_service)
    # ------------------------------------------------

    # Ensure session exists in the service
    try:
        _session_service.create_session_sync(
            app_name=app.name,
            user_id=_USER_ID,
            session_id=session_id,
        )
    except Exception:
        pass

    try:
        # Run using the fresh local runner
        events = runner.run(
            user_id=_USER_ID,
            session_id=session_id,
            new_message=types.Content(
                role="user",
                parts=[types.Part.from_text(text=query)],
            ),
        )

        final_text: str | None = None
        for event in events:
            logs.append(f"DEBUG Event: {event}")
            if getattr(event, "author", None) == "user":
                continue
            if event.is_final_response() and event.content and event.content.parts:
                text_parts = [p.text for p in event.content.parts if getattr(p, "text", None)]
                if text_parts:
                    final_text = "".join(text_parts)

        return final_text or "No response generated.", session_id, logs

    except Exception as e:
        logs.append(f"ERROR: {e!r}")
        return "An error occurred.", session_id, logs
