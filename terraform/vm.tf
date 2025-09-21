resource "google_service_account" "taxiproject" {
  account_id   = var.vm_name
  display_name = "Custom SA for VM Instance"
}

data "google_compute_image" "debian" {
  family  = "debian-11"
  project = "debian-cloud"
}

resource "google_compute_instance" "taxiproject" {
  name         = var.vm_name
  machine_type = var.vm_machine_type
  zone         = "us-central1-a"

  tags = ["ssh", "airflow-ui"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.debian.self_link
      size  = 200
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    ssh-keys = "taxiproject:${file("~/.ssh/gcp.pub")} taxiproject"
  }


  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = google_service_account.taxiproject.email
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_firewall" "ssh" {
  name    = "ssh-access"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_tags   = ["ssh"]
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["ssh"]
}
resource "google_compute_firewall" "airflow_ui" {
  name    = "allow-airflow-ui"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"] 
  target_tags   = ["airflow-ui"]
}
