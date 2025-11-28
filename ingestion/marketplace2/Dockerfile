FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion package (includes configs and marketplace2 job).
COPY ingestion ./ingestion

# Default entrypoint now targets the isolated marketplace2 package directly.
ENTRYPOINT [ "python", "-m", "ingestion.marketplace2.fetch_marketplace2_listing" ]
