terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.2.0"
    }
  }
}

provider "google" {
  credentials = "./keys/_.json"
  project     = var.project_id
  region      = var.region
}