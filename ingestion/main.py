import argparse
import importlib
import sys
from pathlib import Path

def main():
    """
    Dynamically imports and runs an ingestion job module.
    """
    # Use a generic parser to grab the job name, and pass the rest on
    parser = argparse.ArgumentParser()
    parser.add_argument('job_name', help='The name of the job file to run in the jobs/ directory.')
    args, remaining_argv = parser.parse_known_args()

    job_name = args.job_name

    # Add project root to path for absolute imports
    sys.path.append(str(Path.cwd()))

    module_candidates = [f"ingestion.jobs.{job_name}"]
    # Fallback for jobs that have been relocated to dedicated packages.
    if job_name == "fetch_marketplace2_listing":
        module_candidates.append("ingestion.marketplace2.fetch_marketplace2_listing")
    if job_name == "fetch_marketplace1_listing":
        module_candidates.append("ingestion.marketplace1.fetch_marketplace1_listing")

    job_module = None
    for module_path in module_candidates:
        try:
            job_module = importlib.import_module(module_path)
            break
        except ImportError:
            continue

    if not job_module:
        print(f"Error: Could not find job '{job_name}'. Make sure there is a '{job_name}.py' in 'ingestion/jobs/' or an alias configured.")
        sys.exit(1)

    try:
        # The job module's main function is responsible for its own argument parsing
        # We pass the remaining arguments to it
        job_module.main(remaining_argv)
    except Exception as e:
        print(f"An error occurred while running job '{job_name}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
