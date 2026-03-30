# =============================================================================
# SCRIPT: 06_train_model.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Entrena un modelo de detección de anomalías usando BigQuery ML.
#   Usa K-Means Clustering sobre transacciones de Gold.
#
# CONCEPTO K-MEANS:
#   Agrupa transacciones en N clusters según comportamiento similar.
#   Transacciones que quedan en clusters pequeños o con alta distancia
#   al centroide son candidatas a anomalías (fraude, error, UAF).
#
# FEATURES USADAS:
#   monto_clp    → monto de la transacción
#   hora         → hora del día (0-23)
#   dia_semana   → día de la semana (1-7)
#   score_anomalia → score previo del generador de datos
#
# SOLO USA REGISTROS VÁLIDOS:
#   es_registro_valido = TRUE
#   es_duplicado = FALSE
#   monto_clp >= 0
#   arcop_estado != 'ELIMINADO'
#
# USO:
#   python ai/06_train_model.py
# =============================================================================

import os
from datetime import datetime
import pytz
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET    = os.getenv("DATASET_GOLD", "gold")
TIMEZONE   = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))

# Nombre del modelo en BigQuery ML
MODEL_ID   = f"{PROJECT_ID}.{DATASET}.anomaly_detection_model"

def main():
    ahora  = datetime.now(TIMEZONE)
    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 65)
    print(" Chilean Bank — BigQuery ML — Entrenamiento Anomalías")
    print(f" Proyecto : {PROJECT_ID}")
    print(f" Modelo   : {MODEL_ID}")
    print(f" Ejecutado: {ahora.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 65)

    # -------------------------------------------------------------------------
    # PASO 1: Verificar que hay datos suficientes para entrenar
    # K-Means necesita al menos 100 registros para ser significativo
    # -------------------------------------------------------------------------
    print(f"\n📊 Verificando datos disponibles...")
    count_query = f"""
        SELECT COUNT(*) as total
        FROM `{PROJECT_ID}.{DATASET}.fact_transacciones`
        WHERE es_registro_valido = TRUE
          AND es_duplicado = FALSE
          AND monto_clp >= 0
          AND arcop_estado != 'ELIMINADO'
    """
    total = list(client.query(count_query).result())[0].total
    print(f"   Registros válidos para entrenar: {total:,}")

    # -------------------------------------------------------------------------
    # PASO 2: Entrenar modelo K-Means
    #
    # NUM_CLUSTERS = 5 — número de grupos de comportamiento:
    #   Cluster 1: transacciones pequeñas en horario laboral (normal)
    #   Cluster 2: transacciones medianas en horario laboral (normal)
    #   Cluster 3: transacciones grandes (alto valor, puede ser UAF)
    #   Cluster 4: transacciones nocturnas (fuera de horario)
    #   Cluster 5: transacciones anómalas (monto + hora inusuales)
    #
    # STANDARDIZE_FEATURES = TRUE — normaliza los valores para que
    # monto_clp (millones) no domine sobre hora (0-23)
    # -------------------------------------------------------------------------
    print(f"\n🤖 Entrenando modelo K-Means...")
    print(f"   Clusters: 5")
    print(f"   Features: monto_clp, hora, dia_semana, score_anomalia")
    print(f"   Esto puede tardar 1-2 minutos...")

    train_query = f"""
        CREATE OR REPLACE MODEL `{MODEL_ID}`
        OPTIONS (
            model_type         = 'kmeans',
            num_clusters       = 5,
            standardize_features = TRUE,
            max_iterations     = 20
        ) AS
        SELECT
            monto_clp,
            hora,
            dia_semana,
            score_anomalia
        FROM `{PROJECT_ID}.{DATASET}.fact_transacciones`
        WHERE es_registro_valido = TRUE
          AND es_duplicado       = FALSE
          AND monto_clp          >= 0
          AND arcop_estado       != 'ELIMINADO'
    """

    job = client.query(train_query)
    job.result()  # esperar que termine
    print(f"   ✅ Modelo entrenado: {MODEL_ID}")

    # -------------------------------------------------------------------------
    # PASO 3: Evaluar el modelo
    # Muestra las métricas de cada cluster:
    # - centroid_id: ID del cluster
    # - davies_bouldin_index: qué tan separados están los clusters (menor = mejor)
    # - mean_squared_distance: distancia promedio al centroide (menor = más compacto)
    # -------------------------------------------------------------------------
    print(f"\n📈 Evaluando modelo...")
    eval_query = f"""
        SELECT *
        FROM ML.EVALUATE(MODEL `{MODEL_ID}`)
    """
    eval_results = list(client.query(eval_query).result())
    if eval_results:
        row = eval_results[0]
        print(f"   Davies-Bouldin Index : {row.davies_bouldin_index:.4f} (menor es mejor)")
        print(f"   Mean Squared Distance: {row.mean_squared_distance:.4f}")

    # -------------------------------------------------------------------------
    # PASO 4: Ver centroides de cada cluster
    # Los centroides muestran el "cliente típico" de cada grupo
    # -------------------------------------------------------------------------
    print(f"\n🎯 Centroides de los clusters:")
    centroids_query = f"""
        SELECT
            centroid_id,
            feature,
            numerical_value
        FROM ML.CENTROIDS(MODEL `{MODEL_ID}`)
        ORDER BY centroid_id, feature
    """
    current_cluster = None
    for row in client.query(centroids_query).result():
        if row.centroid_id != current_cluster:
            current_cluster = row.centroid_id
            print(f"\n   Cluster {current_cluster}:")
        print(f"      {row.feature:<20} : {row.numerical_value:.2f}")

    # -------------------------------------------------------------------------
    # PASO 5: Aplicar modelo a todas las transacciones
    # Genera predicciones y las guarda en una tabla nueva
    # -------------------------------------------------------------------------
    print(f"\n🔍 Aplicando modelo a transacciones...")
    predict_query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.fact_transacciones_ml` AS
        SELECT
            t.id_transaccion,
            t.id_cliente,
            t.monto_clp,
            t.hora,
            t.dia_semana,
            t.canal,
            t.region,
            t.fecha,
            t.es_anomalia_flag,
            t.score_anomalia                           as score_generador,
            p.CENTROID_ID                              as cluster_id,
            p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE as distancia_centroide,

            -- Score ML: distancia alta = más anómalo
            -- Normalizado entre 0 y 1
            ROUND(
                p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE /
                NULLIF(MAX(p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE)
                    OVER(), 0)
            , 4) as score_ml,

            -- Clasificación combinada (generador + ML)
            CASE
                WHEN t.es_anomalia_flag = TRUE
                  OR p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE >
                     PERCENTILE_CONT(
                         p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE, 0.95
                     ) OVER()
                THEN 'ANOMALA'
                ELSE 'NORMAL'
            END as clasificacion_ml,

            t.flag_uaf_threshold,
            t.nivel_riesgo_anomalia,
            t.categoria_riesgo,
            t.arcop_estado,
            t.fecha_gold

        FROM `{PROJECT_ID}.{DATASET}.fact_transacciones` t
        JOIN ML.PREDICT(
            MODEL `{MODEL_ID}`,
            (
                SELECT
                    id_transaccion,
                    monto_clp,
                    hora,
                    dia_semana,
                    score_anomalia
                FROM `{PROJECT_ID}.{DATASET}.fact_transacciones`
                WHERE es_registro_valido = TRUE
                  AND es_duplicado       = FALSE
                  AND monto_clp          >= 0
            )
        ) p ON t.id_transaccion = p.id_transaccion
        WHERE t.es_registro_valido = TRUE
    """

    job = client.query(predict_query)
    job.result()
    tabla = client.get_table(f"{PROJECT_ID}.{DATASET}.fact_transacciones_ml")
    print(f"   ✅ Predicciones guardadas: {tabla.num_rows:,} registros")

    # -------------------------------------------------------------------------
    # PASO 6: Resumen de anomalías detectadas
    # -------------------------------------------------------------------------
    print(f"\n📊 Resumen de anomalías detectadas:")
    resumen_query = f"""
        SELECT
            clasificacion_ml,
            COUNT(*)           as total,
            ROUND(AVG(monto_clp), 0)  as monto_promedio,
            ROUND(AVG(score_ml), 4)   as score_ml_promedio,
            COUNTIF(flag_uaf_threshold) as alertas_uaf
        FROM `{PROJECT_ID}.{DATASET}.fact_transacciones_ml`
        GROUP BY clasificacion_ml
        ORDER BY clasificacion_ml
    """
    print(f"   {'Clasificación':<12} {'Total':>8} {'Monto Prom':>14} {'Score ML':>10} {'Alertas UAF':>12}")
    print(f"   {'-'*60}")
    for row in client.query(resumen_query).result():
        print(f"   {row.clasificacion_ml:<12} {row.total:>8,} {row.monto_promedio:>14,.0f} {row.score_ml_promedio:>10.4f} {row.alertas_uaf:>12,}")

    print(f"\n{'=' * 65}")
    print(f" ✅ BigQuery ML completado")
    print(f"{'=' * 65}")
    print(f" Modelo  : {MODEL_ID}")
    print(f" Tabla   : {PROJECT_ID}.{DATASET}.fact_transacciones_ml")
    print(f" Ver en  : console.cloud.google.com/bigquery")
    print(f"\n PRÓXIMO PASO: banco_chile_dag.py (Composer)")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
