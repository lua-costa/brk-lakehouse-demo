output "project_id" {
  value       = var.project_id
  description = "ID do Projeto GCP"
}

output "raw_bucket_name" {
  value       = google_storage_bucket.raw.name
  description = "Bucket GCS - Camada Raw"
}

output "standardized_bucket_name" {
  value       = google_storage_bucket.standardized.name
  description = "Bucket GCS - Camada Standardized"
}

output "trusted_bucket_name" {
  value       = google_storage_bucket.trusted.name
  description = "Bucket GCS - Camada Trusted"
}

output "pubsub_topic" {
  value       = google_pubsub_topic.telemetry_events.name
  description = "Tópico Pub/Sub para Telemetria"
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.lakehouse_ds.dataset_id
  description = "Dataset ID do BigQuery"
}

output "composer_environment_name" {
  value       = google_composer_environment.composer_env.name
  description = "Nome do Ambiente Cloud Composer 3"
}

output "composer_dag_gcs_prefix" {
  value       = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  description = "Bucket GCS para deploy das DAGs do Composer"
}
