resource "google_bigquery_dataset" "taxidata" {
  dataset_id = var.bq_dataset_id
  location   = "US"
}