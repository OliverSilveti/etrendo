
# Use a Python runtime that supports the code (requires 3.10+ for | unions)
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy configuration files
COPY config.yaml .
COPY prompts.yaml .

# Copy the rest of the application's code into the container
COPY . .

# Configure Environment Variables
ENV GOOGLE_CLOUD_PROJECT=etrendo-prd
ENV GOOGLE_CLOUD_LOCATION=us-central1
ENV GOOGLE_GENAI_USE_VERTEXAI=True

# Expose the port the app runs on
EXPOSE 8080

# Run the application
CMD ["streamlit", "run", "ui.py", "--server.port", "8080", "--server.address", "0.0.0.0"]
