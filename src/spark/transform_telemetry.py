#!/usr/bin/env python3
"""
BRK Ambiental - Data Lakehouse Pipeline (PySpark / Dataproc Serverless)
Job: Processamento e tratamento de dados de Telemetria (Raw -> Standardized)
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Job - BRK Telemetry Raw to Standardized")
    parser.add_argument("--raw_bucket", required=True, help="Nome do bucket GCS Raw")
    parser.add_argument("--standardized_bucket", required=True, help="Nome do bucket GCS Standardized")
    parser.add_argument("--execution_date", required=False, default=None, help="Data de execução (YYYY-MM-DD)")
    return parser.parse_args()

def main():
    args = parse_args()
    raw_bucket = args.raw_bucket
    standardized_bucket = args.standardized_bucket

    print(f"🚀 Iniciando Spark Job BRK Telemetry (Raw -> Standardized)")
    print(f"📥 Input Bucket: gs://{raw_bucket}/telemetry/")
    print(f"📤 Output: gs://{standardized_bucket}/telemetry_parquet/")

    spark = SparkSession.builder \
        .appName("BRK-Telemetry-RawToStandardized") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Busca recursiva de arquivos JSON dentro da pasta telemetry/
    raw_path = f"gs://{raw_bucket}/telemetry/**/*.json"

    # 1. Leitura dos arquivos JSON brutos da camada Raw
    try:
        df_raw = spark.read.option("recursiveFileLookup", "true").json(f"gs://{raw_bucket}/telemetry/")
    except Exception as e:
        print(f"⚠️ Aviso na leitura recursiva de gs://{raw_bucket}/telemetry/: {e}")
        df_raw = spark.read.json(f"gs://{raw_bucket}/telemetry/*.json")

    print(f"📊 Total de registros lidos brutos: {df_raw.count()}")

    # 2. Achatar estrutura aninhada do JSON e fazer parse correto dos tipos
    df_flattened = df_raw.select(
        F.col("event_id").cast(StringType()).alias("event_id"),
        F.col("hidrometro_id").cast(StringType()).alias("hidrometro_id"),
        F.col("unidade_operacional").cast(StringType()).alias("unidade_operacional"),
        F.col("cidade").cast(StringType()).alias("cidade"),
        F.col("estado").cast(StringType()).alias("estado"),
        F.to_timestamp(F.col("timestamp")).alias("timestamp_evento"),
        F.col("leitura.vazao_lps").cast(DoubleType()).alias("vazao_lps"),
        F.col("leitura.pressao_psi").cast(DoubleType()).alias("pressao_psi"),
        F.col("leitura.ph_agua").cast(DoubleType()).alias("ph_agua"),
        F.col("leitura.turbidez_unt").cast(DoubleType()).alias("turbidez_unt"),
        F.col("leitura.cloro_residual_mg_l").cast(DoubleType()).alias("cloro_residual_mg_l"),
        F.col("status_sistema").cast(StringType()).alias("status_sistema"),
        F.col("operador_responsavel").cast(StringType()).alias("operador_responsavel")
    )

    # 3. Tratamento de duplicados baseados no event_id
    df_deduplicated = df_flattened.dropDuplicates(["event_id"])

    # 4. Adicionar colunas de partição de data baseadas no timestamp do evento
    df_transformed = df_deduplicated \
        .withColumn("data_processamento", F.current_date()) \
        .withColumn("ano", F.year(F.col("timestamp_evento"))) \
        .withColumn("mes", F.lpad(F.month(F.col("timestamp_evento")), 2, "0")) \
        .withColumn("dia", F.lpad(F.dayofmonth(F.col("timestamp_evento")), 2, "0"))

    print(f"✅ Registros únicos pós-deduplicação: {df_transformed.count()}")

    # 5. Escrita na camada Standardized em formato Parquet particionado
    output_path = f"gs://{standardized_bucket}/telemetry_parquet/"

    df_transformed.write \
        .mode("append") \
        .partitionBy("estado", "ano", "mes") \
        .parquet(output_path)

    print(f"🎉 Processamento concluído e salvo com sucesso em: {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()
