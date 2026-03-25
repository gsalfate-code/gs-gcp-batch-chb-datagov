-- =============================================================================
-- MODELO: fact_arcop_solicitudes
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   Historial completo de solicitudes de derechos ARCOP.
--   Permite auditoría completa para CMF y seguimiento de plazos Ley 19.628.
--
-- CAMPOS CLAVE:
--   flag_vencida          → incumplimiento legal — debe reportarse a CMF
--   flag_proxima_vencer   → alerta preventiva — quedan menos de 3 días
--   flag_cancelacion_pendiente → cliente bloqueado en el pipeline
--   dias_restantes        → días hábiles hasta vencimiento
--
-- FUENTE: silver.stg_arcop + gold.dim_cliente
-- =============================================================================

with arcop as (
    select * from {{ ref('stg_arcop') }}
),

clientes as (
    select
        id_cliente,
        rut_pseudo,
        region,
        arcop_estado
    from {{ ref('dim_cliente') }}
    where is_current = true
),

joined as (
    select
        -- -----------------------------------------------------------------
        -- IDENTIFICADORES
        -- -----------------------------------------------------------------
        arcop.id_solicitud,
        arcop.id_cliente,

        -- -----------------------------------------------------------------
        -- TIPO Y CANAL
        -- -----------------------------------------------------------------
        arcop.tipo_derecho,
        arcop.descripcion_tipo,
        arcop.canal,
        arcop.responsable,

        -- -----------------------------------------------------------------
        -- FECHAS Y PLAZOS
        -- -----------------------------------------------------------------
        arcop.fecha_solicitud,
        arcop.fecha_limite,
        arcop.fecha_resolucion,
        arcop.dias_habiles_max,
        arcop.dias_resolucion_estimados,
        arcop.vence_en_dias,
        arcop.dias_transcurridos,

        -- -----------------------------------------------------------------
        -- ESTADO
        -- -----------------------------------------------------------------
        arcop.estado,
        arcop.es_critico,

        -- -----------------------------------------------------------------
        -- FLAGS LEGALES
        -- -----------------------------------------------------------------
        arcop.flag_vencida,
        arcop.flag_proxima_vencer,
        arcop.flag_cancelacion_pendiente,
        arcop.flag_cumplida_en_plazo,

        -- Prioridad para dashboard CMF
        case
            when arcop.flag_vencida              then 'CRITICA'
            when arcop.flag_cancelacion_pendiente then 'ALTA'
            when arcop.flag_proxima_vencer        then 'MEDIA'
            else                                       'NORMAL'
        end as prioridad,

        -- -----------------------------------------------------------------
        -- ENRIQUECIMIENTO DESDE dim_cliente
        -- -----------------------------------------------------------------
        clientes.rut_pseudo,
        clientes.region,
        clientes.arcop_estado,

        -- -----------------------------------------------------------------
        -- METADATOS
        -- -----------------------------------------------------------------
        current_timestamp() as fecha_gold

    from arcop
    left join clientes
        on arcop.id_cliente = clientes.id_cliente
)

select * from joined
