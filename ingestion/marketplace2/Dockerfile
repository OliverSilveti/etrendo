FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion package (includes configs and marketplace2 job).
COPY ingestion ./ingestion

# Default entrypoint mirrors marketplace1 pattern; Cloud Run args supply the source name.
ENTRYPOINT [ "python", "-m", "ingestion.jobs.fetch_marketplace2_listing" ]
