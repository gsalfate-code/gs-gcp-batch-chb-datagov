-- =============================================================================
-- MODELO: mart_arcop_compliance
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   KPIs de cumplimiento Ley 19.628 para el DPO y reportes CMF.
--   Agrega solicitudes ARCOP por tipo, estado y prioridad.
--   Alerta solicitudes vencidas e incumplimientos legales.
--
-- AUDIENCIA: DPO, Compliance, CMF
-- ACTUALIZACIÓN: cada hora (via DAG)
-- =============================================================================

with fact as (
    select * from {{ ref('fact_arcop_solicitudes') }}
),

resumen as (
    select
        -- Fecha del reporte
        current_date()                                          as fecha_reporte,
        current_timestamp()                                     as timestamp_reporte,

        -- -----------------------------------------------------------------
        -- VOLUMEN TOTAL
        -- -----------------------------------------------------------------
        count(*)                                                as total_solicitudes,
        countif(estado = 'PENDIENTE')                          as total_pendientes,
        countif(estado = 'EN_PROCESO')                         as total_en_proceso,
        countif(estado = 'COMPLETADO')                         as total_completadas,
        countif(estado = 'VENCIDO')                            as total_vencidas,

        -- -----------------------------------------------------------------
        -- POR TIPO DE DERECHO
        -- -----------------------------------------------------------------
        countif(tipo_derecho = 'A')                            as solicitudes_acceso,
        countif(tipo_derecho = 'R')                            as solicitudes_rectificacion,
        countif(tipo_derecho = 'C')                            as solicitudes_cancelacion,
        countif(tipo_derecho = 'O')                            as solicitudes_oposicion,
        countif(tipo_derecho = 'P')                            as solicitudes_portabilidad,

        -- -----------------------------------------------------------------
        -- ALERTAS LEGALES
        -- -----------------------------------------------------------------
        countif(flag_vencida)                                  as alertas_vencidas,
        countif(flag_proxima_vencer)                           as alertas_proximas_vencer,
        countif(flag_cancelacion_pendiente)                    as clientes_bloqueados_pipeline,

        -- -----------------------------------------------------------------
        -- CUMPLIMIENTO
        -- -----------------------------------------------------------------
        countif(flag_cumplida_en_plazo)                        as cumplidas_en_plazo,

        -- % cumplimiento = solicitudes resueltas en plazo / total completadas
        safe_divide(
            countif(flag_cumplida_en_plazo),
            countif(estado = 'COMPLETADO')
        ) * 100                                                as pct_cumplimiento,

        -- % vencidas sobre total activas
        safe_divide(
            countif(flag_vencida),
            countif(estado in ('PENDIENTE', 'EN_PROCESO'))
        ) * 100                                                as pct_vencidas,

        -- -----------------------------------------------------------------
        -- TIEMPOS PROMEDIO
        -- -----------------------------------------------------------------
        avg(dias_transcurridos)                                as promedio_dias_resolucion,
        max(dias_transcurridos)                                as max_dias_sin_resolver,

        -- -----------------------------------------------------------------
        -- CANAL MÁS USADO
        -- -----------------------------------------------------------------
        countif(canal = 'WEB')                                 as canal_web,
        countif(canal = 'APP')                                 as canal_app,
        countif(canal = 'SUCURSAL')                            as canal_sucursal,
        countif(canal = 'EMAIL')                               as canal_email,

        -- -----------------------------------------------------------------
        -- SEMÁFORO GENERAL DE CUMPLIMIENTO
        -- VERDE: 0 vencidas, >95% en plazo
        -- AMARILLO: 1-3 vencidas o <95% en plazo
        -- ROJO: >3 vencidas o <80% en plazo
        -- -----------------------------------------------------------------
        case
            when countif(flag_vencida) = 0
             and safe_divide(countif(flag_cumplida_en_plazo), countif(estado = 'COMPLETADO')) >= 0.95
            then 'VERDE'
            when countif(flag_vencida) <= 3
             and safe_divide(countif(flag_cumplida_en_plazo), countif(estado = 'COMPLETADO')) >= 0.80
            then 'AMARILLO'
            else 'ROJO'
        end as semaforo_cumplimiento

    from fact
)

select * from resumen
