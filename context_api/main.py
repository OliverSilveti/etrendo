(from fastapi import FastAPI

app = FastAPI(title="context_api")


@app.get("/health")
def health():
	return {"status": "ok"}


@app.get("/products/{product_id}")
def get_product_context(product_id: str):
	"""Example endpoint that returns a minimal product context payload.

	Replace this with real BigQuery lookups or context assembly logic.
	"""
	return {"product_id": product_id, "context": "example product context"}
)

