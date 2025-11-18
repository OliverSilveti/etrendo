# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire ingestion directory from the project root into the container.
# This makes the 'ingestion' module available for Python to import.

COPY ingestion ./ingestion

# Define the command to run your script.
# We use '-m' to run the script as a module, which is good practice.
# The arguments for the script will be passed by the Cloud Run Job.
ENTRYPOINT [ "python", "-m", "ingestion.jobs.fetch_marketplace1_listing" ]