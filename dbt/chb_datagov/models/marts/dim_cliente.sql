-- =============================================================================
-- MODELO: dim_cliente
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   Dimensión de clientes con SCD2 (Slowly Changing Dimension Type 2).
--   Mantiene historial completo de cambios en estado ARCOP y datos.
--   Incluye flags de calidad detectados en staging.
--
-- SCD2:
--   Cada cambio genera una nueva fila.
--   Registro vigente: is_current = TRUE, valid_to = '9999-12-31'
--   Históricos: is_current = FALSE, valid_to = fecha del cambio
--
-- ARCOP:
--   arcop_cancelacion_solicitado = TRUE → cliente bloqueado en el DAG
--   arcop_oposicion_activa = TRUE → excluido de analítica y modelos ML
--
-- FUENTE: silver.stg_clientes
-- =============================================================================

with stg as (
    select * from {{ ref('stg_clientes') }}
),

con_scd2 as (
    select
        -- -----------------------------------------------------------------
        -- SURROGATE KEY
        -- Combina id_cliente + arcop_estado + fecha para garantizar
        -- unicidad entre versiones del mismo cliente
        -- -----------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key([
            'id_cliente',
            'arcop_estado',
            'fecha_deidentify'
        ]) }} as sk_cliente,

        -- -----------------------------------------------------------------
        -- IDENTIFICADORES
        -- -----------------------------------------------------------------
        id_cliente,
        rut_pseudo,

        -- -----------------------------------------------------------------
        -- DATOS MASKEADOS
        -- -----------------------------------------------------------------
        nombre_masked,
        email_masked,
        telefono_masked,
        num_tarjeta_fpe,
        num_cuenta_fpe,

        -- -----------------------------------------------------------------
        -- DATOS INDIRECTOS
        -- -----------------------------------------------------------------
        rango_edad,
        genero,
        region,
        ocupacion,
        estado_civil,
        nivel_educacion,
        tipo_cuenta,

        -- -----------------------------------------------------------------
        -- DATOS SENSIBLES — bucketeados
        -- Valores exactos protegidos por Column Security en esta capa
        -- -----------------------------------------------------------------
        rango_saldo,
        rango_renta,
        rango_deuda,
        score_crediticio,
        categoria_riesgo,
        tiene_credito,

        -- -----------------------------------------------------------------
        -- TEXTO LIBRE DESIDENTIFICADO
        -- -----------------------------------------------------------------

        -- -----------------------------------------------------------------
        -- ARCOP — Ley 19.628
        -- -----------------------------------------------------------------
        arcop_estado,
        arcop_acceso_solicitado,
        arcop_rectificacion_pending,
        arcop_cancelacion_solicitado,
        arcop_oposicion_activa,
        arcop_portabilidad_entregada,
        consentimiento_ley19628,
        fecha_consentimiento,
        fecha_creacion,

        -- -----------------------------------------------------------------
        -- FLAGS DE CALIDAD
        -- Permiten filtrar registros con errores en dashboards de calidad
        -- -----------------------------------------------------------------
        flag_email_invalido,
        flag_genero_invalido,
        flag_score_fuera_rango,
        flag_score_categoria_inconsistente,
        flag_edad_invalida,
        flag_saldo_invalido,
        es_registro_valido,

        -- -----------------------------------------------------------------
        -- SCD2 — campos de versionamiento
        -- valid_from: cuándo entró en vigencia este registro
        -- valid_to:   cuándo dejó de ser vigente (9999-12-31 = vigente hoy)
        -- is_current: TRUE solo para el registro más reciente
        -- -----------------------------------------------------------------
        cast(fecha_deidentify as timestamp) as valid_from,
        cast('9999-12-31'     as date)      as valid_to,
        true                                as is_current,

        -- -----------------------------------------------------------------
        -- METADATOS
        -- -----------------------------------------------------------------
        fecha_staging

    from stg
)

select * from con_scd2
