#!/usr/bin/env python3
"""
BRK Ambiental - Data Lakehouse Pipeline (PySpark / Dataproc Serverless)
Job Gold: Agregação e Métricas de Negócio (Standardized -> Trusted / Gold)
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Job - BRK Telemetry Standardized to Trusted (Gold)")
    parser.add_argument("--standardized_bucket", required=True, help="Nome do bucket GCS Standardized")
    parser.add_argument("--trusted_bucket", required=True, help="Nome do bucket GCS Trusted")
    parser.add_argument("--execution_date", required=False, default=None, help="Data de execução (YYYY-MM-DD)")
    return parser.parse_args()

def main():
    args = parse_args()
    std_bucket = args.standardized_bucket
    trusted_bucket = args.trusted_bucket

    print(f"🚀 Iniciando Spark Job Gold - BRK Telemetry Aggregations")
    print(f"📥 Input: gs://{std_bucket}/telemetry_parquet/")
    print(f"📤 Output: gs://{trusted_bucket}/telemetry_gold_parquet/")

    spark = SparkSession.builder \
        .appName("BRK-Telemetry-StandardizedToGold") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    input_path = f"gs://{std_bucket}/telemetry_parquet/"

    # 1. Leitura dos dados da camada Silver (Standardized)
    try:
        df_silver = spark.read.parquet(input_path)
    except Exception as e:
        print(f"⚠️ Erro ao ler Parquet da camada Silver em {input_path}: {e}")
        spark.stop()
        return

    print(f"📊 Total de registros lidos da camada Silver: {df_silver.count()}")

    # 2. Agregação Gold: Métricas de Operação e Qualidade da Água por Unidade Operacional e Data
    df_gold = df_silver.groupBy(
        F.col("estado"),
        F.col("cidade"),
        F.col("unidade_operacional"),
        F.to_date(F.col("timestamp_evento")).alias("data_leitura")
    ).agg(
        F.count("event_id").alias("total_leituras"),
        F.round(F.avg("vazao_lps"), 2).alias("avg_vazao_lps"),
        F.round(F.max("vazao_lps"), 2).alias("max_vazao_lps"),
        F.round(F.avg("pressao_psi"), 2).alias("avg_pressao_psi"),
        F.round(F.max("pressao_psi"), 2).alias("max_pressao_psi"),
        F.round(F.avg("ph_agua"), 2).alias("avg_ph_agua"),
        F.round(F.avg("turbidez_unt"), 2).alias("avg_turbidez_unt"),
        F.round(F.avg("cloro_residual_mg_l"), 2).alias("avg_cloro_residual"),
        F.sum(F.when(F.col("status_sistema") != "NORMAL", 1).otherwise(0)).alias("total_alertas_anomalia")
    ).withColumn("data_processamento_gold", F.current_timestamp())

    print(f"✅ Registros agregados gerados na camada Gold: {df_gold.count()}")

    # 3. Escrita na camada Trusted (Gold) em Parquet particionado por estado e data
    output_path = f"gs://{trusted_bucket}/telemetry_gold_parquet/"

    df_gold.write \
        .mode("overwrite") \
        .partitionBy("estado") \
        .parquet(output_path)

    print(f"🎉 Processamento Gold concluído e salvo com sucesso em: {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()
