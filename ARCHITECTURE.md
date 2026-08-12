# 📖 Guia de Arquitetura, Configurações e Infraestrutura - BRK Lakehouse

Este documento traz o detalhamento técnico completo de todas as configurações, dimensionamentos de máquinas, arquitetura serverless, orquestração e modelos de Machine Learning (BQML) implementados para o projeto **BRK Ambiental Data Lakehouse**.

---

## 🏛️ 1. Arquitetura do Projeto & Fluxo de Dados

A solução adota a arquitetura **Medalhão (Bronze ➔ Silver ➔ Gold)** integrada com capacidade preditiva e serverless no Google Cloud Platform (GCP).

```
 [IoT / Estações BRK] ➔ (Cloud Run - Flask/Faker) 
        │
        ▼
 [Pub/Sub Topic] ➔ (Streaming Ingestion) ➔ [GCS Raw Bucket] (Camada Bronze)
                                                   │
                                                   ▼
                                  [Dataproc Serverless - PySpark Batch]
                                                   │
                                  ┌────────────────┴────────────────┐
                                  ▼                                 ▼
                     [GCS Standardized Bucket]            [GCS Trusted Bucket]
                         (Camada Silver)                   (Camada Gold)
                                  │                                 │
                                  └────────────────┬────────────────┘
                                                   ▼
                                      [BigQuery External Tables]
                                                   │
                                                   ▼
                                      [BQML - K-Means Clustering]
                                                   │
                                                   ▼
                                [Views Analíticas & Preditivas (BI/AI)]
```

---

## 💻 2. Dimensionamento de Infraestrutura & Configurações de Máquinas

### ☁️ **Cloud Run — Gerador de Telemetria IoT (`brk-telemetry-generator`)**
- **Serviço**: API Python/Flask rodando em container Docker no Cloud Run em `us-east4`.
- **Configuração de Recursos**:
  - **CPU**: 1 vCPU
  - **Memória**: 512 MiB RAM
  - **Auto-escalamento**: 0 a 10 instâncias (Escalamento automático para zero quando ocioso, otimizando custos).
  - **Segurança**: Autenticação via IAM (Identity Token do GCP).

---

### 🎼 **Cloud Composer 3 — Orquestração Apache Airflow (`brk-composer-environment`)**
- **Versão**: Composer 3 (Airflow 2.x) gerenciado e sem servidores fixos GKE.
- **Tamanho do Ambiente**: `ENVIRONMENT_SIZE_MEDIUM`
- **Componentes e Recursos Alocados**:
  - **Airflow Scheduler**: 2 vCPUs, 7.5 GB RAM, 5 GB Storage.
  - **Airflow Worker**: Escala automaticamente de 2 a 6 workers (2 vCPUs, 7.5 GB RAM por worker).
  - **Airflow Webserver**: 2 vCPUs, 7.5 GB RAM, 5 GB Storage.
  - **Database (PostgreSQL Metadata)**: Gerenciado em High Availability.
- **Service Account do Worker**: `brk-composer-sa@{PROJECT_ID}.iam.gserviceaccount.com` com papéis `roles/composer.worker`, `roles/storage.admin`, `roles/bigquery.admin` e `roles/dataproc.editor`.

---

### ⚡ **Dataproc Serverless — Processamento Spark (`PySpark Batch`)**
Diferente do Dataproc tradicional (que exige provisionamento de clusters fixos de máquinas virtuais GCE), o **Dataproc Serverless** aloca recursos dinamicamente por segundo para cada job Spark (Spark Batch).

- **Como funciona no projeto**:
  - O Airflow aciona a API do Dataproc Serverless via `DataprocCreateBatchOperator`.
  - **Versão do Runtime**: Spark 3.3 / PySpark (Runtime `2.1`).
- **Unidades de Processamento (DCUs - Dataproc Compute Units)**:
  - **Configuração Padrão Autogerenciada**: 
    - **Driver**: 4 DCUs (16 GB RAM, 4 vCPUs)
    - **Executores**: Auto-scaling inicial de 2 executores (cada um com 4 DCUs - 16 GB RAM, 4 vCPUs).
  - **Alocação Dinâmica**: O Spark aloca mais executores conforme o volume de dados da camada Bronze aumenta, e encerra todas as máquinas assim que a transformação finaliza.
- **Rede & VPC**:
  - Executa na sub-rede `projects/{PROJECT_ID}/regions/us-east4/subnetworks/default` com acesso privado aos buckets do Cloud Storage via `Private Google Access`.

---

### 📦 **Google Cloud Storage (GCS) — Camadas do Data Lake**
Os dados são armazenados em 3 buckets isolados na região `us-east4`:
1. **Camada Bronze (Raw)**: `gs://{PROJECT_ID}-raw-{SUFFIX}/`
   - Armazena os eventos brutos de telemetria em formato JSON recebidos via streaming do Pub/Sub.
2. **Camada Silver (Standardized)**: `gs://{PROJECT_ID}-standardized-{SUFFIX}/`
   - Armazena dados limpos, deduplicados e particionados por data (`dt=YYYY-MM-DD`) em formato colunar **Parquet** de alta performance.
3. **Camada Gold (Trusted)**: `gs://{PROJECT_ID}-trusted-{SUFFIX}/`
   - Armazena agregações de negócios e indicadores calculados por unidade operacional e cidade em formato **Parquet**.

---

### 🧠 3. Modelagem de Dados, BigQuery & BQML (Machine Learning)

No BigQuery, criamos tabelas externas sobre o GCS para permitir consultas SQL de altíssima performance sem custo de duplicação de dados:

1. **`telemetry_silver`**: Tabela Externa conectada aos arquivos Parquet da camada Silver.
2. **`telemetry_gold_summary`**: Tabela Externa conectada aos agregados da camada Gold.

#### 🤖 **Modelo de Inteligência Artificial — BQML (`mdl_deteccao_anomalias_pressao`)**
Para detectar riscos operacionais e anomalias nas estações de tratamento e rede de distribuição de água/esgoto da BRK, o pipeline do Airflow treina um modelo de Machine Learning nativo no BigQuery:

- **Algoritmo**: `K-MEANS` (Agrupamento não supervisionado).
- **Parâmetros**:
  - `num_clusters = 3` (Perfis operacionais: Normal, Flutuação Aceitável e Anomalia Crítica).
  - `standardize_features = TRUE` (Normalização z-score das variáveis numéricas).
- **Features Analisadas**:
  - `vazao_lps` (Vazão em litros por segundo)
  - `pressao_psi` (Pressão da rede em PSI)
  - `ph_agua` (pH da água/esgoto)
  - `turbidez_unt` (Nível de turbidez)
  - `cloro_residual_mg_l` (Nível de cloro residual)

#### 🔮 **View Preditiva — `vw_predicao_risco_vazamento`**
Utiliza a função `ML.PREDICT` sobre a tabela Silver calculando a distância euclidiana para o centroide do cluster mais próximo (`NEAREST_CENTROIDS_DISTANCE`):
- Distância > 2.5 ➔ **`RISCO_CRITICO_ANOMALIA`**
- Distância entre 1.5 e 2.5 ➔ **`RISCO_MODERADO`**
- Distância < 1.5 ➔ **`OPERACAO_NORMAL`**

---

## 🛠️ 4. Fluxo de Criação do Projeto e Recursos (`setup.sh`)

O provisionamento é 100% automatizado via `gcloud CLI` através das seguintes etapas:

1. **Criação do Projeto GCP**: Cria o projeto `brk-demo-gcp-XXXXXX` e faz o vínculo com o `GCP_BILLING_ACCOUNT_ID`.
2. **Habilitação de APIs**: Ativa `compute`, `storage`, `pubsub`, `run`, `bigquery`, `dataproc`, `composer`, `iam` e `cloudresourcemanager`.
3. **Provisionamento do Storage**: Cria os 3 buckets GCS e faz o upload dos scripts PySpark.
4. **Setup do Pub/Sub**: Cria o tópico `brk-telemetry-events` e a subscrição `brk-telemetry-events-sub`.
5. **Ajuste de IAM**: Cria a Service Account `brk-composer-sa` e concede as roles necessárias para execução do Composer, Dataproc Serverless e BigQuery.
6. **Deploy do Cloud Run**: Compila o container do gerador de telemetria e publica a API.
7. **Deploy do Cloud Composer 3**: Cria o ambiente de orquestração do Airflow e faz a sincronização da DAG `brk_lakehouse_pipeline.py`.
8. **Criação das Views no BigQuery**: Executa o script SQL para expor a camada analítica e os modelos preditivos BQML.
