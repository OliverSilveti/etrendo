resource "google_cloud_run_v2_job" "default" {
  name     = var.service_name
  project  = local.project_id
  location = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.job_runner.email

      containers {
        image = "${var.region}-docker.pkg.dev/${local.project_id}/etrendo-repo/${var.service_name}:latest"

        # Pass the required argument to the Python module
        args = ["marketplace1"]

        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }

        volume_mounts {
          name       = "serpapi-secret"
          mount_path = "/etc/secrets"
        }
      }

      volumes {
        name = "serpapi-secret"
        secret {
          secret = google_secret_manager_secret.serpapi_key.secret_id

          # 👇 This tells Cloud Run to mount the *existing* secret version
          items {
            version = "latest"
            path    = "${var.service_name}-serpapi-key"
          }
        }
      }
    }
  }
  depends_on = [
    null_resource.docker_build_push,
    google_secret_manager_secret.serpapi_key
  ]
}

resource "google_cloud_run_v2_job" "marketplace2" {
  name     = var.service_name_marketplace2
  project  = local.project_id
  location = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.job_runner_marketplace2.email

      containers {
        image = "${var.region}-docker.pkg.dev/${local.project_id}/etrendo-repo/${var.service_name_marketplace2}:latest"
        args  = ["marketplace2"]
      }
    }
  }

  depends_on = [
    google_artifact_registry_repository.repo
  ]
}
