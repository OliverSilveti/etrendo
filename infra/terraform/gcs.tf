resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }
}
