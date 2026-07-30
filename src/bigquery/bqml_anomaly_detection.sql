-- ==============================================================================
-- BRK Ambiental - BQML Anomaly Detection & Predictive Views
-- ==============================================================================

-- 1. Treinamento do Modelo de Detecção de Anomalias (K-Means Clustering)
CREATE OR REPLACE MODEL `brk_lakehouse.mdl_deteccao_anomalias_pressao`
OPTIONS(
  model_type='KMEANS',
  num_clusters=3,
  standardize_features=TRUE
) AS
SELECT
  vazao_lps,
  pressao_psi,
  ph_agua,
  turbidez_unt,
  cloro_residual_mg_l
FROM `brk_lakehouse.telemetry_silver`;

-- 2. View Detalhada de Telemetria
CREATE OR REPLACE VIEW `brk_lakehouse.vw_telemetria_detalhada` AS
SELECT
  hidrometro_id,
  unidade_operacional,
  cidade,
  timestamp_evento,
  vazao_lps,
  pressao_psi,
  ph_agua,
  turbidez_unt,
  cloro_residual_mg_l,
  status_sistema,
  operador_responsavel,
  DATE(timestamp_evento) AS data_evento
FROM `brk_lakehouse.telemetry_silver`;

-- 3. View de KPIs de Qualidade Operacional (Gold)
CREATE OR REPLACE VIEW `brk_lakehouse.vw_kpis_qualidade_operacao` AS
SELECT
  cidade,
  unidade_operacional,
  data_leitura,
  total_leituras,
  avg_vazao_lps,
  max_vazao_lps,
  avg_pressao_psi,
  avg_ph_agua,
  avg_turbidez_unt,
  avg_cloro_residual,
  total_alertas_anomalia,
  ROUND(SAFE_DIVIDE(total_alertas_anomalia, total_leituras) * 100, 2) AS pct_anomalias
FROM `brk_lakehouse.telemetry_gold_summary`;

-- 4. View de Resumo Crítico de Alertas de Manutenção
CREATE OR REPLACE VIEW `brk_lakehouse.vw_resumo_alertas_manutencao` AS
SELECT
  unidade_operacional,
  cidade,
  status_sistema,
  COUNT(1) AS quantidade_ocorrencias,
  ROUND(AVG(pressao_psi), 2) AS pressao_media_psi,
  ROUND(AVG(vazao_lps), 2) AS vazao_media_lps,
  MAX(timestamp_evento) AS ultima_ocorrencia
FROM `brk_lakehouse.telemetry_silver`
WHERE status_sistema != 'NORMAL'
GROUP BY unidade_operacional, cidade, status_sistema;

-- 5. View de Predição Preditiva de Risco de Vazamento (BQML ML.PREDICT)
CREATE OR REPLACE VIEW `brk_lakehouse.vw_predicao_risco_vazamento` AS
SELECT
  unidade_operacional,
  cidade,
  vazao_lps,
  pressao_psi,
  ph_agua,
  status_sistema,
  CENTROID_ID AS cluster_perfil_operacao,
  ROUND(NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance, 2) AS score_anomalia_distancia,
  CASE
    WHEN NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance > 2.5 THEN 'RISCO_CRITICO_ANOMALIA'
    WHEN NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].distance > 1.5 THEN 'RISCO_MODERADO'
    ELSE 'OPERACAO_NORMAL'
  END AS classificacao_risco
FROM
  ML.PREDICT(MODEL `brk_lakehouse.mdl_deteccao_anomalias_pressao`,
    (SELECT * FROM `brk_lakehouse.telemetry_silver`)
  );
