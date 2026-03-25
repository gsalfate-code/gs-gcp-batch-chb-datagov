-- =============================================================================
-- MODELO: stg_arcop
-- CAPA: Staging (Silver → validado)
-- MATERIALIZACIÓN: view
-- DESCRIPCIÓN:
--   Valida solicitudes ARCOP desde Silver.
--   Controla plazos legales Ley 19.628 (15 días hábiles máximo).
--   Detecta solicitudes próximas a vencer y vencidas.
--   Bloquea clientes con cancelación activa del pipeline.
--
-- ERRORES QUE DETECTA:
--   - Solicitudes vencidas (estado PENDIENTE/EN_PROCESO + fecha_limite < hoy)
--   - Solicitudes críticas sin resolver (tipo C = cancelación)
--   - Solicitudes próximas a vencer (< 3 días hábiles)
--
-- FUENTE: silver.arcop_solicitudes
-- DESTINO: gold (via fact_arcop_solicitudes, mart_arcop_compliance)
-- =============================================================================

with source as (
    select * from {{ source('silver', 'arcop_solicitudes') }}
),

validado as (
    select
        -- -------------------------------------------------------------------------
        -- IDENTIFICADORES
        -- -------------------------------------------------------------------------
        id_solicitud,
        id_cliente,

        -- -------------------------------------------------------------------------
        -- TIPO Y DESCRIPCIÓN
        -- -------------------------------------------------------------------------
        tipo_derecho,
        descripcion_tipo,
        canal,
        responsable,

        -- -------------------------------------------------------------------------
        -- FECHAS
        -- -------------------------------------------------------------------------
        date(fecha_solicitud)   as fecha_solicitud,
        date(fecha_limite)      as fecha_limite,
        date(fecha_resolucion)  as fecha_resolucion,

        -- -------------------------------------------------------------------------
        -- ESTADO Y PLAZOS
        -- -------------------------------------------------------------------------
        estado,
        dias_habiles_max,
        dias_resolucion_estimados,
        vence_en_dias,
        es_critico,

        -- -------------------------------------------------------------------------
        -- DÍAS TRANSCURRIDOS DESDE LA SOLICITUD
        -- -------------------------------------------------------------------------
        date_diff(current_date(), date(fecha_solicitud), DAY) as dias_transcurridos,

        -- -------------------------------------------------------------------------
        -- FLAGS DE CONTROL LEGAL
        -- -------------------------------------------------------------------------

        -- Solicitud vencida — incumplimiento CMF
        case
            when estado in ('PENDIENTE', 'EN_PROCESO')
              and date(fecha_limite) < current_date()
            then true else false
        end as flag_vencida,

        -- Próxima a vencer — menos de 3 días hábiles
        case
            when estado in ('PENDIENTE', 'EN_PROCESO')
              and vence_en_dias <= 3
              and vence_en_dias > 0
            then true else false
        end as flag_proxima_vencer,

        -- Cancelación crítica sin resolver
        -- Si tipo=C y no está COMPLETADO, el cliente está bloqueado en el pipeline
        case
            when tipo_derecho = 'C'
              and estado != 'COMPLETADO'
            then true else false
        end as flag_cancelacion_pendiente,

        -- Cumplida dentro del plazo legal
        case
            when estado = 'COMPLETADO'
              and date(fecha_resolucion) <= date(fecha_limite)
            then true else false
        end as flag_cumplida_en_plazo,

        -- -------------------------------------------------------------------------
        -- METADATOS
        -- -------------------------------------------------------------------------
        current_timestamp() as fecha_staging

    from source
)

select * from validado
