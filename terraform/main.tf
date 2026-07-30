# ==============================================================================
# BRK Ambiental Data Lakehouse - Core Infrastructure
# ==============================================================================

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# ------------------------------------------------------------------------------
# 1. GCS Buckets (Raw, Standardized, Trusted)
# ------------------------------------------------------------------------------

resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw-${random_id.bucket_suffix.hex}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}

resource "google_storage_bucket" "standardized" {
  name                        = "${var.project_id}-standardized-${random_id.bucket_suffix.hex}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}

resource "google_storage_bucket" "trusted" {
  name                        = "${var.project_id}-trusted-${random_id.bucket_suffix.hex}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}

# ------------------------------------------------------------------------------
# 2. Pub/Sub Topic & Subscription (Ingestão de Telemetria / IoT)
# ------------------------------------------------------------------------------

resource "google_pubsub_topic" "telemetry_events" {
  name = "brk-telemetry-events"
}

resource "google_pubsub_subscription" "telemetry_subscription" {
  name  = "brk-telemetry-events-sub"
  topic = google_pubsub_topic.telemetry_events.name

  ack_deadline_seconds = 20
}

# ------------------------------------------------------------------------------
# 3. BigQuery Dataset
# ------------------------------------------------------------------------------

resource "google_bigquery_dataset" "lakehouse_ds" {
  dataset_id                  = "brk_lakehouse"
  friendly_name               = "BRK Lakehouse Dataset"
  description                 = "Dataset para armazenamento de dados das camadas Standardized e Trusted do Lakehouse BRK"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# ------------------------------------------------------------------------------
# 4. Service Account & IAM para Cloud Composer 3
# ------------------------------------------------------------------------------

resource "google_service_account" "composer_sa" {
  account_id   = "brk-composer-sa"
  display_name = "Service Account para Cloud Composer 3 BRK"
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Grant Cloud Composer v2/v3 Service Agent Extension Role to Default Service Agent
data "google_project" "project" {}

resource "google_project_iam_member" "composer_agent_service_agent_v2" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# ------------------------------------------------------------------------------
# 5. Cloud Composer 3 (Tamanho Medium em us-east4)
# ------------------------------------------------------------------------------

resource "google_composer_environment" "composer_env" {
  name   = "brk-composer-environment"
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2"
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }

    environment_size = "ENVIRONMENT_SIZE_MEDIUM"
  }

  depends_on = [
    google_project_iam_member.composer_worker,
    google_project_iam_member.composer_agent_service_agent_v2
  ]
}
