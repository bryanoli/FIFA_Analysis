terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.21.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  credentials = file(var.credentials)
  project = var.project
  region  = var.region
}



resource "google_storage_bucket" "ucl-premier-417919-lake" {
  name = var.gcs_bucket_name
  location = var.location

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    condition{
        age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "ucl-dataset" {
  dataset_id = var.BQ_DATASET
  location   = var.location
}