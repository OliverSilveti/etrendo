# Agent

This folder contains the agent code for the project.

## Getting Started

This guide will walk you through setting up the agent service for local development.

### Prerequisites

Make sure you have the following tools installed on your local machine:

- [Python](https://www.python.org/downloads/) (3.9 or later)
- [Pip](https://pip.pypa.io/en/stable/installation/)

### Installation

1.  **Install dependencies:**
    From the root of the project, run:
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

The agent service uses environment variables for configuration. Create a `.env` file in the root of the project and add the following:

```
SERPAPI_API_KEY="your_serpapi_api_key"
```

### Running the Service

To run the agent service, run the following command from the root of the project:

```bash
uvicorn agent.agent_api.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.
