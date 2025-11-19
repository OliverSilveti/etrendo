
resource "google_cloud_run_v2_job" "default" {
  name     = var.service_name
  project  = local.project_id
  location = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.job_runner.email
      containers {
        image = "us-central1-docker.pkg.dev/${local.project_id}/etrendo-repo/${var.service_name}:latest"
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        volume_mounts {
          name = "serpapi-secret"
          mount_path = "/etc/secrets"
        }
      }
      volumes {
        name = "serpapi-secret"
        secret {
          secret = google_secret_manager_secret.serpapi_key.secret_id
        }
      }
    }
  }
  depends_on = [
    null_resource.docker_build_push,
    google_secret_manager_secret.serpapi_key
  ]
}