from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "brk-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Leitura dinâmica das variáveis do Airflow (sem valores hardcoded de projetos específicos)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="brk-demo-gcp")
GCP_REGION = Variable.get("GCP_REGION", default_var="us-east4")
RAW_BUCKET = Variable.get("RAW_BUCKET_NAME", default_var=f"{GCP_PROJECT_ID}-raw")
STANDARDIZED_BUCKET = Variable.get("STANDARDIZED_BUCKET_NAME", default_var=f"{GCP_PROJECT_ID}-standardized")
TRUSTED_BUCKET = Variable.get("TRUSTED_BUCKET_NAME", default_var=f"{GCP_PROJECT_ID}-trusted")

SILVER_SPARK_SCRIPT = f"gs://{RAW_BUCKET}/scripts/transform_telemetry.py"
GOLD_SPARK_SCRIPT = f"gs://{RAW_BUCKET}/scripts/transform_gold.py"

with DAG(
    dag_id="brk_lakehouse_telemetry_pipeline",
    default_args=default_args,
    description="Pipeline de Telemetria BRK Ambiental - Medalhão (Bronze -> Silver -> Gold no BigQuery + BQML)",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["brk", "lakehouse", "pyspark", "silver", "gold", "bigquery", "bqml"],
) as dag:

    # --------------------------------------------------------------------------
    # 1. CAMADA SILVER (Standardized) - Limpeza e Deduplicação via PySpark
    # --------------------------------------------------------------------------
    silver_batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": SILVER_SPARK_SCRIPT,
            "args": [
                f"--raw_bucket={RAW_BUCKET}",
                f"--standardized_bucket={STANDARDIZED_BUCKET}",
                "--execution_date={{ ds }}"
            ]
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"brk-composer-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com",
                "subnetwork_uri": f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/default"
            }
        },
        "runtime_config": {
            "version": "2.1"
        }
    }

    run_silver_pyspark_batch = DataprocCreateBatchOperator(
        task_id="run_pyspark_silver_raw_to_standardized",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch=silver_batch_details,
        batch_id="brk-silver-{{ ts_nodash.lower() }}",
    )

    create_bq_silver_table = BigQueryInsertJobOperator(
        task_id="create_bigquery_silver_table",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.brk_lakehouse.telemetry_silver`
                    OPTIONS (
                        format = 'PARQUET',
                        uris = ['gs://{STANDARDIZED_BUCKET}/telemetry_parquet/*']
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # --------------------------------------------------------------------------
    # 2. CAMADA GOLD (Trusted) - Agregações de Negócio via PySpark
    # --------------------------------------------------------------------------
    gold_batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": GOLD_SPARK_SCRIPT,
            "args": [
                f"--standardized_bucket={STANDARDIZED_BUCKET}",
                f"--trusted_bucket={TRUSTED_BUCKET}",
                "--execution_date={{ ds }}"
            ]
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"brk-composer-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com",
                "subnetwork_uri": f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/default"
            }
        },
        "runtime_config": {
            "version": "2.1"
        }
    }

    run_gold_pyspark_batch = DataprocCreateBatchOperator(
        task_id="run_pyspark_gold_standardized_to_trusted",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch=gold_batch_details,
        batch_id="brk-gold-{{ ts_nodash.lower() }}",
    )

    create_bq_gold_table = BigQueryInsertJobOperator(
        task_id="create_bigquery_gold_table",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.brk_lakehouse.telemetry_gold_summary`
                    OPTIONS (
                        format = 'PARQUET',
                        uris = ['gs://{TRUSTED_BUCKET}/telemetry_gold_parquet/*']
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # --------------------------------------------------------------------------
    # 3. CAMADA BQML - Treinamento e Atualização das Views Preditivas de IA
    # --------------------------------------------------------------------------
    train_bqml_model_and_views = BigQueryInsertJobOperator(
        task_id="train_bqml_anomaly_model_and_views",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.brk_lakehouse.mdl_deteccao_anomalias_pressao`
                    OPTIONS(
                      model_type='KMEANS',
                      num_clusters=3,
                      standardize_features=TRUE
                    ) AS
                    SELECT vazao_lps, pressao_psi, ph_agua, turbidez_unt, cloro_residual_mg_l
                    FROM `{GCP_PROJECT_ID}.brk_lakehouse.telemetry_silver`;

                    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.brk_lakehouse.vw_predicao_risco_vazamento` AS
                    SELECT
                      unidade_operacional, cidade, vazao_lps, pressao_psi, ph_agua, status_sistema,
                      CENTROID_ID AS cluster_perfil_operacao,
                      ROUND(NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance, 2) AS score_anomalia_distancia,
                      CASE
                        WHEN NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance > 2.5 THEN 'RISCO_CRITICO_ANOMALIA'
                        WHEN NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance > 1.5 THEN 'RISCO_MODERADO'
                        ELSE 'OPERACAO_NORMAL'
                      END AS classificacao_risco
                    FROM
                      ML.PREDICT(MODEL `{GCP_PROJECT_ID}.brk_lakehouse.mdl_deteccao_anomalias_pressao`,
                        (SELECT * FROM `{GCP_PROJECT_ID}.brk_lakehouse.telemetry_silver`)
                      );
                """,
                "useLegacySql": False,
            }
        },
    )

    run_silver_pyspark_batch >> create_bq_silver_table >> run_gold_pyspark_batch >> create_bq_gold_table >> train_bqml_model_and_views
