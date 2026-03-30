import functions_framework
from google.cloud import bigquery
import json

client = bigquery.Client(project='gs-gcp-batch-chb-datagov')

def run_query(sql):
    rows = client.query(sql).result()
    return [dict(row) for row in rows]

@functions_framework.http
def dashboard_api(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Content-Type': 'application/json'
    }

    endpoint = request.args.get('q', '')
    PROJECT = 'gs-gcp-batch-chb-datagov'

    try:
        if endpoint == 'kpis':
            data = run_query(f"""
                SELECT
                  (SELECT COUNT(*) FROM `{PROJECT}.gold.dim_cliente` WHERE is_current = TRUE) as clientes,
                  (SELECT COUNT(*) FROM `{PROJECT}.gold.fact_transacciones`) as transacciones,
                  (SELECT COUNT(*) FROM `{PROJECT}.gold.fact_transacciones_ml` WHERE clasificacion_ml = 'ANOMALA') as anomalias,
                  (SELECT COUNTIF(flag_uaf_threshold) FROM `{PROJECT}.gold.fact_transacciones_ml` WHERE clasificacion_ml = 'ANOMALA') as uaf
            """)

        elif endpoint == 'calidad':
            data = run_query(f"SELECT tabla, score_calidad, semaforo FROM `{PROJECT}.gold.mart_calidad_datos`")

        elif endpoint == 'arcop':
            data = run_query(f"SELECT * FROM `{PROJECT}.gold.mart_arcop_compliance` LIMIT 1")

        elif endpoint == 'regiones':
            data = run_query(f"""
                SELECT region, COUNT(*) as total
                FROM `{PROJECT}.gold.fact_transacciones_ml`
                WHERE clasificacion_ml = 'ANOMALA'
                GROUP BY region ORDER BY total DESC LIMIT 6
            """)

        elif endpoint == 'clientes':
            data = run_query(f"""
                SELECT id_cliente, rut_pseudo, nombre_masked, email_masked,
                       rango_edad, rango_saldo, region, categoria_riesgo
                FROM `{PROJECT}.gold.dim_cliente`
                WHERE is_current = TRUE LIMIT 20
            """)

        elif endpoint == 'anomalias':
            data = run_query(f"""
                SELECT id_transaccion, CAST(fecha AS STRING) as fecha, canal,
                       monto_clp, hora, cluster_id, clasificacion_ml,
                       CAST(flag_uaf_threshold AS STRING) as flag_uaf_threshold
                FROM `{PROJECT}.gold.fact_transacciones_ml`
                WHERE clasificacion_ml = 'ANOMALA'
                ORDER BY monto_clp DESC LIMIT 15
            """)

        elif endpoint == 'ml_summary':
            data = run_query(f"""
                SELECT clasificacion_ml,
                  COUNT(*) as total,
                  ROUND(AVG(monto_clp), 0) as monto_prom,
                  COUNTIF(flag_uaf_threshold) as uaf
                FROM `{PROJECT}.gold.fact_transacciones_ml`
                GROUP BY clasificacion_ml
            """)

        elif endpoint == 'cliente_ids':
            data = run_query(f"""
                SELECT DISTINCT id_cliente FROM `{PROJECT}.gold.dim_cliente`
                WHERE is_current = TRUE ORDER BY id_cliente LIMIT 100
            """)

        elif endpoint == 'cliente_detail':
            id_cliente = request.args.get('id', '')
            if not id_cliente or not id_cliente.startswith('C-'):
                return (json.dumps({'error': 'id inválido'}), 400, headers)
            data = run_query(f"""
                SELECT id_cliente, rut_pseudo, nombre_masked, email_masked,
                       rango_edad, rango_saldo, rango_renta, region, genero,
                       ocupacion, score_crediticio, categoria_riesgo,
                       arcop_estado, tipo_cuenta, CAST(valid_from AS STRING) as valid_from
                FROM `{PROJECT}.gold.dim_cliente`
                WHERE id_cliente = '{id_cliente}' AND is_current = TRUE LIMIT 1
            """)

        elif endpoint == 'cliente_txn':
            id_cliente = request.args.get('id', '')
            if not id_cliente or not id_cliente.startswith('C-'):
                return (json.dumps({'error': 'id inválido'}), 400, headers)
            data = run_query(f"""
                SELECT CAST(fecha AS STRING) as fecha, canal, monto_clp,
                       tipo_transaccion, clasificacion_ml,
                       CAST(flag_uaf_threshold AS STRING) as flag_uaf_threshold
                FROM `{PROJECT}.gold.fact_transacciones_ml`
                WHERE id_cliente = '{id_cliente}'
                ORDER BY fecha DESC LIMIT 8
            """)

        elif endpoint == 'cliente_arcop':
            id_cliente = request.args.get('id', '')
            if not id_cliente or not id_cliente.startswith('C-'):
                return (json.dumps({'error': 'id inválido'}), 400, headers)
            data = run_query(f"""
                SELECT tipo_derecho, estado, dias_restantes, prioridad
                FROM `{PROJECT}.gold.fact_arcop_solicitudes`
                WHERE id_cliente = '{id_cliente}'
                ORDER BY fecha_solicitud DESC LIMIT 5
            """)

        else:
            return (json.dumps({'error': 'endpoint no encontrado'}), 404, headers)

        return (json.dumps(data, default=str), 200, headers)

    except Exception as e:
        return (json.dumps({'error': str(e)}), 500, headers)
