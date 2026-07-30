# 🌊 BRK Ambiental - GCP Data Lakehouse Demo

Blueprint completo de um **Data Lakehouse automatizado, isolado e efêmero** no Google Cloud Platform para a **BRK Ambiental**, simulando a ingestão, processamento de telemetria de água/esgoto e orquestração.

---

## 🏛️ Arquitetura e Componentes

- **Ingestão IoT / Telemetria**: API Flask no **Cloud Run** que simula leituras de hidrômetros e estações da BRK usando `Faker` (pressão, vazão, pH, turbidez, cloro).
- **Barramento de Mensagens**: **Pub/Sub** (`brk-telemetry-events`).
- **Armazenamento em Camadas (GCS)**:
  - `Raw`: JSONs brutos de telemetria.
  - `Standardized`: Dados tratados, deduplicados e salvos em Parquet particionado.
  - `Trusted`: Dados agregados para BI no **BigQuery**.
- **Processamento de Dados**: Job **PySpark** executado via **Dataproc Serverless**.
- **Orquestração**: **Cloud Composer 3 (Apache Airflow)** em tamanho Medium.

---

## 🚀 Como Executar

### 1. Definir a variável do Billing e executar o Setup
```bash
export GCP_BILLING_ACCOUNT_ID="SEU_BILLING_ACCOUNT_ID"
./setup.sh
```

### 2. Teardown (Destruição para Custo Zero)
Ao terminar o laboratório, execute o script de teardown para destruir toda a infraestrutura no Terraform e apagar o projeto GCP por completo:
```bash
./destroy.sh
```

---

## ⚠️ Avisos de Segurança e Custos (Watch Out)

1. **Segurança & Dados Sensíveis**:
   - **Nunca inclua chaves, tokens ou o ID de faturamento (Billing Account ID) hardcoded nos arquivos do repositório**.
   - O [setup.sh](file:///Users/luanacosta/brk-lakehouse-demo/setup.sh) depende exclusivamente da variável de ambiente exportada `GCP_BILLING_ACCOUNT_ID`.
   - O arquivo [terraform.tfvars](file:///Users/luanacosta/brk-lakehouse-demo/terraform/terraform.tfvars) é gerado dinamicamente em tempo de execução e está incluído no [.gitignore](file:///Users/luanacosta/brk-lakehouse-demo/.gitignore) para que não seja enviado ao GitHub.

2. **Ciclo de Cobrança e Encerramento de Projeto GCP**:
   - Ao executar `./destroy.sh`, o comando `gcloud projects delete` coloca o projeto em estado de encerramento (*shut down*).
   - A exclusão física e definitiva nos servidores do Google leva cerca de **30 dias**, porém **a cobrança de todos os recursos é imediatamente interrompida e congelada assim que o projeto é desativado**.

