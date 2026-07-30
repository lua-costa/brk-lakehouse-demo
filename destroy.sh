#!/usr/bin/env bash
set -e

# ==============================================================================
# BRK Ambiental Data Lakehouse - Teardown & Cleanup Script (gcloud CLI)
# ==============================================================================

echo "============================================================"
echo "🧹 Iniciando Teardown da Demo Data Lakehouse - BRK Ambiental"
echo "============================================================"

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
    echo "❌ ERRO: Nenhum projeto padrão definido no gcloud CLI."
    echo "   Por favor, defina: gcloud config set project SEU_PROJECT_ID"
    exit 1
fi

echo "⚠️ PROJETO ALVO PARA DESTRUIÇÃO: ${PROJECT_ID}"

# Deletar Projeto GCP via gcloud CLI
echo "🗑️ Deletando permanentemente o Projeto GCP (${PROJECT_ID})..."
gcloud projects delete "${PROJECT_ID}" --quiet

echo "============================================================"
echo "✅ Teardown concluído com sucesso!"
echo "💰 O projeto GCP ${PROJECT_ID} foi desativado. Cobranças zeradas!"
echo "============================================================"
