resource "google_project_service" "services" {
  for_each = toset([
    "run.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "cloudbuild.googleapis.com"
  ])
  service = each.key
  disable_on_destroy = false
}

resource "google_service_account" "job_runner" {
  account_id   = "${var.service_name}-sa"
  project      = local.project_id
  display_name = "Service Account for Marketplace1 Ingestion Job"
}

resource "google_secret_manager_secret" "serpapi_key" {
  secret_id = "${var.service_name}-serpapi-key"
  project   = local.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "serpapi_key_version" {
  secret = google_secret_manager_secret.serpapi_key.id
  secret_data = var.serpapi_api_key
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.serpapi_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.job_runner.email}"
}

data "google_storage_bucket" "default" {
  name = var.gcs_bucket_name
}

resource "google_storage_bucket_iam_member" "gcs_writer" {
  bucket = data.google_storage_bucket.default.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job_runner.email}"
}

resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  project       = local.project_id
  repository_id = "etrendo-repo"
  format        = "DOCKER"
  depends_on    = [google_project_service.services]
}

resource "null_resource" "docker_build_push" {
  triggers = {
    job_script_hash = filemd5("${path.module}/ingestion/jobs/fetch_marketplace1_listing.py")
    dockerfile_hash = filemd5("${path.module}/Dockerfile")
    requirements_hash = filemd5("${path.module}/requirements.txt")
  }

  provisioner "local-exec" {
    command = <<-EOT
      gcloud auth configure-docker ${var.region}-docker.pkg.dev
      docker build -f ${path.module}/Dockerfile -t ${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.repo.name}/${var.service_name}:latest .
      docker push ${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.repo.name}/${var.service_name}:latest
    EOT
    working_dir = "${path.module}"
  }

  depends_on = [google_artifact_registry_repository.repo]
}