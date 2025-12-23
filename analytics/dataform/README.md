# Etrendo Dataform Project

This project contains the data transformation logic for the Etrendo project, managed by Dataform. It follows a medallion architecture to transform raw data (Bronze) into cleaned, reliable datasets (Silver) and finally into business-ready aggregates (Gold).

## Project Structure

- `definitions/`: This directory contains the SQLX files that define all the tables, views, and operations in the pipeline.
  - `sources/`: Contains the declarations of raw source tables from BigQuery.
  - `bronze/`: Models for the Bronze layer (raw data).
  - `silver/`: Models for the Silver layer (cleaned, conformed data).
  - `gold/`: Models for the Gold layer (business-level aggregates).
- `dataform.json`: The main configuration file for this Dataform project.
- `package.json`: Manages project dependencies.

## MVP Setup in Google Cloud (Manual Steps)

To get this project running in your GCP environment for the MVP, you need to perform a one-time manual setup in the Google Cloud Console.

### 1. Connect Your Git Repository

1.  Go to the **Dataform** page in the Google Cloud Console.
2.  Click **"Create Repository"**.
3.  Give it a name (e.g., `etrendo-dataform`).
4.  Connect it to the Git repository this code lives in.
5.  In the repository connection settings, you **must** specify the folder path. Enter exactly:
    `analytics/dataform`
6.  Finish creating the repository. Dataform is now linked to this folder.

### 2. Set Up a Daily Schedule

1.  Go to the **Cloud Scheduler** page in the Google Cloud Console.
2.  Click **"Create Job"**.
3.  **Define the schedule:** Give it a name, and for the frequency, use the cron expression `0 5 * * *` to run it every day at 5 AM.
4.  **Configure the target:**
    *   **Target type**: Select **Pub/Sub**.
    *   **Topic**: Create a new topic and name it `dataform-run`.
    *   **Message**: Paste the following JSON into the message body. This tells Dataform to run all the models in your project.
        ```json
        {
          "compilationResultName": "projects/etrendo-prd/locations/europe-west1/repositories/etrendo-dataform/compilationResults/main",
          "runConfig": {
            "tags": []
          }
        }
        ```
        *(**Important**: You may need to adjust the `compilationResultName` to match your exact GCP project ID, location, and the Dataform repository name you chose in step 1).*
5.  Click **"Create"**. Your Dataform project will now run automatically every day.

## Testing

You can manually run and test parts of the project directly from the Dataform UI in the Cloud Console. Use the "tags" we've added to the files (e.g., `silver`, `marketplace1`) to select and run only the specific models you want to test.
