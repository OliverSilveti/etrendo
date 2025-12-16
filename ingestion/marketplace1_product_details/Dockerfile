FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion ./ingestion

ENTRYPOINT [ "python", "-m", "ingestion.marketplace1_product_details.fetch_marketplace1_product_details" ]
