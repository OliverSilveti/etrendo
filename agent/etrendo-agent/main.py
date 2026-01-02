
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Query(BaseModel):
    query: str

@app.get("/")
def read_root():
    return {"message": "Etrendo Agent is running"}

@app.post("/query")
def query_agent(query: Query):
    # In the next steps, we will implement the logic to query BigQuery
    # and use Vertex AI to generate insights.
    return {"response": f"Processing your query: {query.query}"}
