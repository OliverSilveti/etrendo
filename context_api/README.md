# Context API

This folder contains the context API code for the project.

## Getting Started

This guide will walk you through setting up the context API service for local development.

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

The context API service uses environment variables for configuration. Create a `.env` file in the root of the project and add any necessary environment variables here.

### Running the Service

To run the context API service, run the following command from the root of the project:

```bash
uvicorn context_api.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.
