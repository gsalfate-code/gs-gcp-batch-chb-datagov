-- =============================================================================
-- MODELO: mart_anomalias
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   Agrega transacciones anómalas para el equipo de riesgo y fraude.
--   Combina score_anomalia del generador con reglas de negocio UAF.
--   Alimenta dashboard de anomalías en Power BI.
--
-- NIVELES DE RIESGO:
--   ALTO   → score >= 0.8 o monto UAF (>= $5M CLP)
--   MEDIO  → score >= 0.5
--   BAJO   → score < 0.5
--
-- AUDIENCIA: Equipo Riesgo, Fraude, UAF
-- ACTUALIZACIÓN: cada hora (via DAG)
-- =============================================================================

with fact as (
    select * from {{ ref('fact_transacciones') }}
    where es_registro_valido = true
),

anomalias as (
    select
        -- -----------------------------------------------------------------
        -- IDENTIFICADORES
        -- -----------------------------------------------------------------
        id_transaccion,
        id_cliente,
        rut_pseudo,
        num_cuenta_fpe,

        -- -----------------------------------------------------------------
        -- TRANSACCIÓN
        -- -----------------------------------------------------------------
        monto_clp,
        tipo_transaccion,
        canal,
        categoria_gasto,
        region,
        fecha,
        fecha_hora,
        hora,
        nombre_dia,

        -- -----------------------------------------------------------------
        -- SCORES Y NIVELES
        -- -----------------------------------------------------------------
        score_anomalia,
        nivel_riesgo_anomalia,
        es_anomalia_flag,
        flag_uaf_threshold,

        -- Motivo de la alerta
        case
            when flag_uaf_threshold and es_anomalia_flag
                then 'MONTO_UAF_Y_PATRON_ANOMALO'
            when flag_uaf_threshold
                then 'MONTO_SOBRE_UMBRAL_UAF'
            when score_anomalia >= 0.8
                then 'PATRON_ANOMALO_ALTO'
            when score_anomalia >= 0.5
                then 'PATRON_ANOMALO_MEDIO'
            else
                'REVISION_PREVENTIVA'
        end as motivo_alerta,

        -- -----------------------------------------------------------------
        -- CONTEXTO DEL CLIENTE
        -- -----------------------------------------------------------------
        categoria_riesgo,
        score_crediticio,
        arcop_estado,

        -- -----------------------------------------------------------------
        -- METADATOS
        -- -----------------------------------------------------------------
        fecha_gold

    from fact
    where es_anomalia_flag = true
       or flag_uaf_threshold = true
       or score_anomalia >= 0.5
),

-- Resumen por día y región para dashboard
resumen_diario as (
    select
        fecha,
        region,
        nivel_riesgo_anomalia,
        count(*)                    as total_alertas,
        sum(monto_clp)              as monto_total_alertado,
        avg(score_anomalia)         as score_promedio,
        countif(flag_uaf_threshold) as alertas_uaf,
        max(monto_clp)              as monto_maximo
    from anomalias
    group by fecha, region, nivel_riesgo_anomalia
)

-- Retornamos el detalle completo para drill-down en Power BI
select * from anomalias
