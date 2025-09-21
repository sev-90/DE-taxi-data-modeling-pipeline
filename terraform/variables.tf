

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "my_project_id"
}

variable "region" {
  description = "The region to deploy resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default     = "my-bucket_name"
}

variable "bq_dataset_id" {
  description = "The dataset ID for BigQuery"
  type        = string
  default     = "my_dataset_name"
}

variable "vm_name" {
  description = "The name of the VM"
  type        = string
  default     = "my_vm_name"
}

variable "vm_machine_type" {
  description = "The machine type for the GCP VM"
  type        = string
  default     = "n2-standard-4" #"e2-medium"
}
