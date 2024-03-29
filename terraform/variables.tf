locals {
  data_lake_bucket = "ucl_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "premier-predict-417919"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west2"
  type        = string
}

variable "location" {
  description = "Project Location"
  default = "US"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "ucl-premier-417919-bucket"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "ucl_data"
}

variable "credentials" {
  type    = string
  default = "~/.google/credentials/google_credentials.json"
}