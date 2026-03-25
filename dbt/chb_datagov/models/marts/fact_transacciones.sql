-- =============================================================================
-- MODELO: fact_transacciones
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   Tabla de hechos de transacciones bancarias.
--   Particionada por fecha para performance en consultas BigQuery.
--   Excluye clientes con cancelación ARCOP activa.
--   Marca duplicados sin eliminarlos — auditoría completa.
--
-- PARTICIONADO:
--   Por campo fecha (DATE) — BigQuery solo lee particiones necesarias.
--   Reduce costo y tiempo de consulta significativamente.
--
-- CLUSTERING:
--   Por canal y tipo_transaccion — optimiza filtros frecuentes en BI.
--
-- EXCLUSIONES:
--   Clientes con arcop_cancelacion_solicitado = TRUE no aparecen.
--   Ley 19.628 exige no procesar datos de clientes que solicitaron cancelación.
--
-- FUENTE: silver.stg_transacciones + gold.dim_cliente
-- =============================================================================

{{ config(
    materialized  = 'table',
    partition_by  = {
        'field': 'fecha',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by    = ['canal', 'tipo_transaccion']
) }}

with txn as (
    select * from {{ ref('stg_transacciones') }}
),

clientes as (
    -- Solo clientes vigentes y no cancelados
    -- Ley 19.628: no procesar datos de clientes con cancelación activa
    select
        id_cliente,
        rut_pseudo,
        region        as region_cliente,
        arcop_estado,
        arcop_cancelacion_solicitado,
        arcop_oposicion_activa,
        categoria_riesgo,
        score_crediticio
    from {{ ref('dim_cliente') }}
    where is_current = true
      and arcop_cancelacion_solicitado = false
      and arcop_estado != 'ELIMINADO'
),

joined as (
    select
        -- -----------------------------------------------------------------
        -- IDENTIFICADORES
        -- -----------------------------------------------------------------
        txn.id_transaccion,
        txn.id_cliente,
        txn.num_cuenta_fpe,
        txn.num_tarjeta_mask,

        -- -----------------------------------------------------------------
        -- NEGOCIO
        -- -----------------------------------------------------------------
        txn.monto_clp,
        txn.tipo_transaccion,
        txn.canal,
        txn.categoria_gasto,
        txn.region,
        txn.descripcion,
        txn.estado,

        -- -----------------------------------------------------------------
        -- TIEMPO — partición y análisis temporal
        -- -----------------------------------------------------------------
        txn.fecha,
        txn.fecha_hora,
        txn.hora,
        txn.dia_semana,
        txn.nombre_dia,

        -- -----------------------------------------------------------------
        -- MODELO IA — scores de anomalía
        -- -----------------------------------------------------------------
        txn.es_anomalia_flag,
        txn.score_anomalia,

        -- Clasificación del score para dashboards
        case
            when txn.score_anomalia >= 0.8 then 'ALTO'
            when txn.score_anomalia >= 0.5 then 'MEDIO'
            else 'BAJO'
        end as nivel_riesgo_anomalia,

        -- -----------------------------------------------------------------
        -- FLAGS DE CALIDAD
        -- -----------------------------------------------------------------
        txn.flag_monto_negativo,
        txn.flag_fecha_futura,
        txn.flag_campos_nulos,
        txn.flag_pii_en_descripcion,
        txn.flag_uaf_threshold,
        txn.es_registro_valido,
        txn.es_duplicado,

        -- -----------------------------------------------------------------
        -- ENRIQUECIMIENTO DESDE dim_cliente
        -- -----------------------------------------------------------------
        clientes.rut_pseudo,
        clientes.categoria_riesgo,
        clientes.score_crediticio,
        clientes.arcop_estado,
        clientes.arcop_oposicion_activa,

        -- -----------------------------------------------------------------
        -- METADATOS
        -- -----------------------------------------------------------------
        txn.fecha_deidentify,
        txn.fecha_staging,
        current_timestamp() as fecha_gold

    from txn
    -- LEFT JOIN para no perder transacciones aunque el cliente no esté en dim
    left join clientes
        on txn.id_cliente = clientes.id_cliente

    -- Excluir transacciones de clientes con cancelación activa
    where clientes.id_cliente is not null
)

select * from joined
