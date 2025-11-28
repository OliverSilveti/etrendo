resource "google_project_service" "services" {
  for_each = toset([
    "run.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "cloudbuild.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

resource "google_service_account" "job_runner" {
  account_id   = "${var.service_name}-sa"
  project      = local.project_id
  display_name = "Service Account for Marketplace1 Ingestion Job"
}

resource "google_service_account" "job_runner_marketplace2" {
  account_id   = "${var.service_name_marketplace2}-sa"
  project      = local.project_id
  display_name = "Service Account for Marketplace2 Ingestion Job"
}

resource "google_secret_manager_secret" "serpapi_key" {
  secret_id = "${var.service_name}-serpapi-key"
  project   = local.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.serpapi_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.job_runner.email}"
}

# We no longer use a data source for the bucket; Terraform creates it via
# resource "google_storage_bucket.bucket" in gcs.tf.
# data "google_storage_bucket" "default" {
#   name = var.gcs_bucket_name
# }

resource "google_storage_bucket_iam_member" "gcs_writer" {
  bucket = google_storage_bucket.bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job_runner.email}"
}

resource "google_storage_bucket_iam_member" "gcs_writer_marketplace2" {
  bucket = var.gcs_bucket_marketplace2
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job_runner_marketplace2.email}"
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
    job_script_hash   = filemd5("../../ingestion/marketplace1/fetch_marketplace1_listing.py")
    dockerfile_hash   = filemd5("../../ingestion/marketplace1/Dockerfile")
    requirements_hash = filemd5("../../requirements.txt")
    sources_hash      = filemd5("../../ingestion/config/sources.yaml")
  }

  provisioner "local-exec" {
    command = <<-EOT
      gcloud auth configure-docker ${var.region}-docker.pkg.dev
      docker buildx build \
        --platform linux/amd64 \
        -f ../../ingestion/marketplace1/Dockerfile \
        -t ${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.repo.repository_id}/${var.service_name}:latest \
        --push ../..
    EOT
    working_dir = "${path.module}"
  }

  depends_on = [google_artifact_registry_repository.repo]
}
