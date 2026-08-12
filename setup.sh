#!/usr/bin/env bash
set -e

# ==============================================================================
# BRK Ambiental Data Lakehouse - Setup & Provisioning Script (gcloud CLI)
# ==============================================================================

echo "============================================================"
echo "🚀 Iniciando Setup da Demo Data Lakehouse - BRK Ambiental (gcloud CLI)"
echo "============================================================"

# Check Billing Account variable
if [ -z "$GCP_BILLING_ACCOUNT_ID" ]; then
    echo "❌ ERRO: A variável de ambiente GCP_BILLING_ACCOUNT_ID não foi definida."
    echo "   Por favor, defina antes de executar: export GCP_BILLING_ACCOUNT_ID=\"SEU_BILLING_ID\""
    exit 1
fi

REGION="us-east4"

# 1. Definir ou criar projeto GCP
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
    RANDOM_SUFFIX=$(openssl rand -hex 3)
    PROJECT_ID="brk-demo-gcp-${RANDOM_SUFFIX}"
    echo "📌 Criando novo projeto GCP isolado: ${PROJECT_ID}..."
    gcloud projects create "${PROJECT_ID}" --name="BRK Lakehouse Demo"
    gcloud config set project "${PROJECT_ID}"
    echo "💳 Vinculando o projeto ${PROJECT_ID} à Conta de Faturamento (${GCP_BILLING_ACCOUNT_ID})..."
    gcloud billing projects link "${PROJECT_ID}" --billing-account="${GCP_BILLING_ACCOUNT_ID}"
else
    echo "📌 Usando projeto GCP atual: ${PROJECT_ID}"
fi

# 2. Ativar APIs Necessárias
echo "🔌 [1/6] Ativando APIs necessárias na GCP..."
gcloud services enable \
    compute.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    run.googleapis.com \
    bigquery.googleapis.com \
    dataproc.googleapis.com \
    composer.googleapis.com \
    cloudresourcemanager.googleapis.com \
    iam.googleapis.com

echo "⏳ Aguardando propagação das APIs..."
sleep 10

# 3. Criar Buckets GCS (Raw, Standardized, Trusted)
RANDOM_BUCKET_SUFFIX=$(openssl rand -hex 4)
RAW_BUCKET="${PROJECT_ID}-raw-${RANDOM_BUCKET_SUFFIX}"
STD_BUCKET="${PROJECT_ID}-standardized-${RANDOM_BUCKET_SUFFIX}"
TRUSTED_BUCKET="${PROJECT_ID}-trusted-${RANDOM_BUCKET_SUFFIX}"

echo "🪣 [2/6] Criando Buckets GCS (Raw, Standardized, Trusted)..."
gcloud storage buckets create "gs://${RAW_BUCKET}" --location="${REGION}" || true
gcloud storage buckets create "gs://${STD_BUCKET}" --location="${REGION}" || true
gcloud storage buckets create "gs://${TRUSTED_BUCKET}" --location="${REGION}" || true

# Upload dos scripts PySpark para o bucket Raw
echo "📤 Upload dos scripts PySpark para gs://${RAW_BUCKET}/scripts/..."
gcloud storage cp src/spark/transform_telemetry.py "gs://${RAW_BUCKET}/scripts/transform_telemetry.py"
gcloud storage cp src/spark/transform_gold.py "gs://${RAW_BUCKET}/scripts/transform_gold.py"

# 4. Criar Tópico e Subscription Pub/Sub
echo "📡 [3/6] Criando Tópico e Subscription Pub/Sub..."
gcloud pubsub topics create brk-telemetry-events || true
gcloud pubsub subscriptions create brk-telemetry-events-sub --topic=brk-telemetry-events || true

# 5. Criar Dataset no BigQuery
echo "📊 [4/6] Criando Dataset brk_lakehouse no BigQuery..."
bq --location="${REGION}" mk --dataset --default_table_expiration 0 "${PROJECT_ID}:brk_lakehouse" || true

# 6. Criar Service Account e permissões IAM para o Composer
echo "🔐 [5/6] Criando Service Account e ajustando permissões IAM..."
SA_NAME="brk-composer-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create "${SA_NAME}" --display-name="Service Account para Cloud Composer 3 BRK" || true

for ROLE in "roles/composer.worker" "roles/storage.admin" "roles/bigquery.admin" "roles/dataproc.editor"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${ROLE}" --quiet > /dev/null
done

# Permissão para o Service Agent do Composer
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
COMPOSER_AGENT_SA="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${COMPOSER_AGENT_SA}" \
    --role="roles/composer.ServiceAgentV2Ext" --quiet > /dev/null || true

# 7. Criar ambiente do Cloud Composer 3
echo "🎼 [6/6] Criando ambiente Cloud Composer 3 (brk-composer-environment)..."
echo "⏳ Nota: A criação do Composer leva de 10 a 15 minutos."
gcloud composer environments create brk-composer-environment \
    --location="${REGION}" \
    --image-version="composer-3-airflow-2" \
    --environment-size="medium" \
    --service-account="${SA_EMAIL}" --async || true

echo "============================================================"
echo "🎉 Setup concluído via gcloud CLI!"
echo "📌 GCP Project ID: ${PROJECT_ID}"
echo "📌 Raw Bucket: gs://${RAW_BUCKET}"
echo "📌 Standardized Bucket: gs://${STD_BUCKET}"
echo "📌 Trusted Bucket: gs://${TRUSTED_BUCKET}"
echo "============================================================"
