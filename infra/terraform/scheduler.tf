resource "google_cloud_scheduler_job" "job" {
  project          = var.project_id
  region           = var.region
  name             = "${var.service_name}-weekly-trigger"
  description      = "Triggers the ${var.service_name} Cloud Run job every Thursday at 07:00 Europe/Berlin."
  
  # Every Thursday at 07:00 (German time)
  schedule  = "0 7 * * 4"
  time_zone = "Europe/Berlin"
  attempt_deadline = "320s"

  http_target {
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.default.id}:run"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.job_runner.email
    }
  }
}
