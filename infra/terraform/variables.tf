variable "region" {
  description = "The GCP region for resources."
  type        = string
  default     = "europe-west1"
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "service_name" {
  description = "The name of the Cloud Run service."
  type        = string
  default     = "marketplace1-ingestion"
}

variable "service_name_marketplace2" {
  description = "The name of the Cloud Run service for marketplace2."
  type        = string
  default     = "marketplace2-ingestion"
}

#variable "serpapi_api_key" {
#  description = "The SerpAPI API key."
#  type        = string
#  sensitive   = true
#}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket."
  type        = string
}

variable "gcs_bucket_marketplace2" {
  description = "The name of the GCS bucket for marketplace2."
  type        = string
  default     = "otto-product-listing-raw-etrendo-prd"
}
