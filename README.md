# E-commerce Insights & AI Agent

This repository contains the code for an MVP to provide e-commerce insights and power an AI agent using GCP, BigQuery, and Gemini.

## Project Structure

- **/ingestion**: Data ingestion pipelines (Cloud Functions/Run).
- **/analytics**: SQL transformation logic (dbt or Dataform) for the medallion architecture.
- **/agent**: The core AI agent logic and serving API (FastAPI/Gemini).
- **/context_api**: An API to fetch real-time context from BigQuery for the agent.
- **/notebooks**: Jupyter notebooks for exploration and prototyping.
- **/orchestration**: Infrastructure-as-code for orchestration (Cloud Scheduler, Workflows).
- **/frontend**: Placeholder for a user-facing application.
- **/infra**: General infrastructure-as-code (Terraform/Pulumi).

