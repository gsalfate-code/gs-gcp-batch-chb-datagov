-- =============================================================================
-- MODELO: stg_clientes
-- CAPA: Staging (Silver → validado)
-- MATERIALIZACIÓN: view
-- DESCRIPCIÓN:
--   Limpia y valida los clientes desidentificados de Silver.
--   Aplica tests de calidad sobre datos directos, indirectos y sensibles.
--   Detecta errores intencionales inyectados en Bronze para demostrar
--   la capacidad de validación de DBT.
--
-- ERRORES QUE DETECTA:
--   - Email mal formado (sin @)
--   - Fecha nacimiento futura → rango_edad = 'menor_18'
--   - Género inválido (no M/F)
--   - Score fuera de rango (300-900)
--   - Score vs categoría_riesgo inconsistente
--   - Saldo negativo (rango_saldo = 'desconocido')
--
-- FUENTE: silver.clientes_deidentified
-- DESTINO: gold (via marts)
-- =============================================================================

with source as (
    -- Leer desde Silver desidentificado
    select * from {{ source('silver', 'clientes_deidentified') }}
),

validado as (
    select
        -- -------------------------------------------------------------------------
        -- IDENTIFICADORES
        -- -------------------------------------------------------------------------
        id_cliente,
        rut_pseudo,

        -- -------------------------------------------------------------------------
        -- DATOS MASKEADOS — tal como vienen de DLP
        -- -------------------------------------------------------------------------
        nombre_masked,
        email_masked,
        telefono_masked,
        num_tarjeta_fpe,
        num_cuenta_fpe,

        -- -------------------------------------------------------------------------
        -- DATOS INDIRECTOS
        -- -------------------------------------------------------------------------
        rango_edad,
        genero,
        region,
        ocupacion,
        estado_civil,
        nivel_educacion,
        tipo_cuenta,

        -- -------------------------------------------------------------------------
        -- DATOS SENSIBLES — bucketing aplicado por DLP
        -- -------------------------------------------------------------------------
        rango_saldo,
        rango_renta,
        rango_deuda,
        score_crediticio,
        categoria_riesgo,
        tiene_credito,

        -- -------------------------------------------------------------------------
        -- VALIDACIONES Y FLAGS DE CALIDAD
        -- Cada flag detecta un error específico para mart_calidad_datos
        -- -------------------------------------------------------------------------

        -- Email válido: debe contener @ y al menos un punto después
        case
            when email_masked not like '%@%'
            then true else false
        end as flag_email_invalido,

        -- Género válido: solo M o F
        case
            when genero not in ('M', 'F')
            then true else false
        end as flag_genero_invalido,

        -- Score en rango válido: 300-900
        case
            when score_crediticio < 300 or score_crediticio > 900
            then true else false
        end as flag_score_fuera_rango,

        -- Score vs categoría inconsistente
        -- Score alto (>750) con categoría ALTO es contradicción
        case
            when score_crediticio > 750 and categoria_riesgo = 'ALTO'
            then true else false
        end as flag_score_categoria_inconsistente,

        -- Edad inválida: menor_18 o desconocido
        case
            when rango_edad in ('menor_18', 'desconocido')
            then true else false
        end as flag_edad_invalida,

        -- Saldo negativo: bucketing retorna 'desconocido' para negativos
        case
            when rango_saldo = 'desconocido'
            then true else false
        end as flag_saldo_invalido,

        -- Registro limpio: ningún flag activo
        case
            when email_masked not like '%@%'
              or genero not in ('M', 'F')
              or score_crediticio < 300 or score_crediticio > 900
              or (score_crediticio > 750 and categoria_riesgo = 'ALTO')
              or rango_edad in ('menor_18', 'desconocido')
              or rango_saldo = 'desconocido'
            then false else true
        end as es_registro_valido,

        -- -------------------------------------------------------------------------
        -- ARCOP — Ley 19.628
        -- -------------------------------------------------------------------------
        arcop_estado,
        arcop_acceso_solicitado,
        arcop_rectificacion_pending,
        arcop_cancelacion_solicitado,
        arcop_oposicion_activa,
        arcop_portabilidad_entregada,
        consentimiento_ley19628,
        fecha_consentimiento,
        fecha_creacion,

        -- -------------------------------------------------------------------------
        -- METADATOS
        -- -------------------------------------------------------------------------
        fecha_deidentify,
        version_dlp,
        current_timestamp() as fecha_staging

    from source
)

select * from validado
