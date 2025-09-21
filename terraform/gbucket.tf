resource "google_storage_bucket" "taxidata" {
  name          = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}