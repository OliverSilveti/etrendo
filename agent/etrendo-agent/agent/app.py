
from fastapi import FastAPI
from pydantic import BaseModel
from agent.agent import root_agent

app = FastAPI(
    title="Etrendo Agent",
    description="An agent that provides e-commerce insights from BigQuery.",
)

class QueryRequest(BaseModel):
    query: str
    asins: list[str] | None = None

@app.get("/")
def health_check():
    """Returns a health check message."""
    return {"status": "ok"}

@app.post("/query")
def query_agent(request: QueryRequest) -> dict:
    """Queries the agent and returns the response."""
    # The ADK agent's chat method is the standard way to interact.
    # We pass the query and can use the 'files' parameter for context if needed,
    # though here we'll just pass the query.
    response = root_agent.chat(request.query)
    return {"response": response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
