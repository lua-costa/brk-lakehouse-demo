# 🌊 BRK Ambiental - GCP Data Lakehouse Demo

Blueprint completo e automatizado de um **Data Lakehouse efêmero** no Google Cloud Platform para a **BRK Ambiental**, simulando a ingestão de telemetria IoT (água e esgoto), processamento em camadas no Data Lake, orquestração e consultas de BI/AI no BigQuery.

> 📚 **Documentação Técnica Detalhada**:  
> Para detalhes aprofundados sobre **dimensionamento de máquinas, Dataproc Serverless (Spark), Cloud Composer 3, Cloud Run e Machine Learning no BigQuery (BQML)**, consulte o guia dedicado:  
> 👉 [**ARCHITECTURE.md**](file:///Users/luanacosta/brk-lakehouse-demo/ARCHITECTURE.md)

---

## 🏛️ Arquitetura e Componentes

- **Ingestão IoT / Telemetria**: API Flask no **Cloud Run** que simula leituras de hidrômetros e estações da BRK usando `Faker` (pressão, vazão, pH, turbidez, cloro).
- **Barramento de Mensagens**: **Pub/Sub** (`brk-telemetry-events`).
- **Armazenamento em Camadas (GCS)**:
  - `Raw`: JSONs brutos de telemetria recebidos via Pub/Sub.
  - `Standardized`: Dados tratados, deduplicados e salvos em Parquet particionado.
  - `Trusted`: Dados agregados para BI no **BigQuery**.
- **Processamento de Dados**: Job **PySpark** executado via **Dataproc Serverless** (alocação dinâmica por segundo).
- **Orquestração**: **Cloud Composer 3 (Apache Airflow)** em tamanho Medium.
- **Analytics & IA**: Views analíticas e modelo de detecção de anomalias K-Means no **BigQuery** (`brk_lakehouse`).

---

## 🛠️ Pré-requisitos

Para executar este laboratório, você precisa apenas de:
1. **Google Cloud SDK (`gcloud CLI`)** instalado e autenticado (`gcloud auth login`).
2. Permissão no GCP para criar projetos ou um projeto ativo com **Conta de Faturamento (Billing Account)** associada.

---

## 🚀 Como Executar (Passo a Passo)

### 1. Definir a variável da Conta de Faturamento e rodar o Setup

No seu terminal, defina o ID da sua Conta de Faturamento GCP e execute o script de automação:

```bash
export GCP_BILLING_ACCOUNT_ID="SEU_BILLING_ACCOUNT_ID"
./setup.sh
```

> **O que o `setup.sh` faz automaticamente?**
> - Cria um novo projeto GCP isolado (`brk-demo-gcp-XXXXXX`).
> - Ativa todas as APIs do GCP necessárias.
> - Cria os 3 Buckets GCS (`Raw`, `Standardized`, `Trusted`).
> - Configura o Tópico e Subscription do Pub/Sub.
> - Faz deploy do gerador de telemetria no Cloud Run.
> - Configura o Service Account e o ambiente do **Cloud Composer 3** (Airflow).
> - Cria as Views analíticas e o modelo preditivo BQML no **BigQuery**.

---

### 2. Validar os Dados no BigQuery

Após a execução da DAG no Airflow/Composer, você pode consultar os dados processados direto no BigQuery:

```sql
SELECT cidade, unidade_operacional, total_leituras, avg_vazao_lps, pct_anomalias 
FROM `brk_lakehouse.vw_kpis_qualidade_operacao`;
```

---

### 3. Teardown (Destruição para Custo Zero)

Ao finalizar a demonstração ou testes, execute o script de destruição para desativar o projeto GCP e interromper imediatamente qualquer cobrança:

```bash
./destroy.sh
```

---

## ⚠️ Avisos de Segurança e Custos

1. **Segurança & Credenciais**:
   - Nunca inclua chaves, tokens ou o ID de faturamento hardcoded no código.
   - O `setup.sh` lê o Billing ID dinamicamente da variável de ambiente `GCP_BILLING_ACCOUNT_ID`.

2. **Ciclo de Cobrança e Encerramento**:
   - Ao executar `./destroy.sh`, o projeto GCP é colocado em estado de desativação (`shut down`).
   - A cobrança de todos os recursos (Composer, Cloud Run, BigQuery, GCS) é **imediatamente interrompida e zerada**.
