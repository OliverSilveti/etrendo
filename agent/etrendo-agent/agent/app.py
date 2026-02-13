
import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from agent.agent import run_agent_query

app = FastAPI(
    title="Etrendo Agent",
    description="An agent that provides e-commerce insights from BigQuery.",
)

class QueryRequest(BaseModel):
    query: str
    asins: list[str] | None = None
    session_id: str | None = None

@app.get("/")
def health_check():
    """Returns a health check message."""
    return {"status": "ok"}

@app.post("/query")
def query_agent(request: QueryRequest) -> dict:
    """Queries the agent and returns the response."""
    query_text = request.query
    if request.asins:
        query_text += f" (ASIN filter: {', '.join(request.asins)})"

    try:
        response, session_id, logs = run_agent_query(query_text, request.session_id)
        return {"response": response, "session_id": session_id}
    except Exception as exc:  # pragma: no cover - pass through as HTTP 500
        logging.exception("Agent query failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
