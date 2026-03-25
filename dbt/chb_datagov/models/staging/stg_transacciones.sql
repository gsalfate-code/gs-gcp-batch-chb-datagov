-- =============================================================================
-- MODELO: stg_transacciones
-- CAPA: Staging (Silver → validado)
-- MATERIALIZACIÓN: view
-- DESCRIPCIÓN:
--   Limpia y valida transacciones desidentificadas de Silver.
--   Detecta errores intencionales: montos negativos, fechas futuras,
--   duplicados y campos nulos.
--
-- ERRORES QUE DETECTA:
--   - Monto negativo (reversión mal codificada)
--   - Fecha futura (bug scheduler pagos programados)
--   - Registro duplicado (error de sistema)
--   - Campos nulos (datos incompletos)
--   - PII reemplazado en descripción (viene de DLP)
--
-- FUENTE: silver.transacciones_deidentified
-- DESTINO: gold (via fact_transacciones)
-- =============================================================================

with source as (
    select * from {{ source('silver', 'transacciones_deidentified') }}
),

validado as (
    select
        -- -------------------------------------------------------------------------
        -- IDENTIFICADORES
        -- -------------------------------------------------------------------------
        id_transaccion,
        id_cliente,
        num_cuenta_fpe,
        num_tarjeta_mask,

        -- -------------------------------------------------------------------------
        -- NEGOCIO
        -- -------------------------------------------------------------------------
        monto_clp,
        tipo_transaccion,
        canal,
        categoria_gasto,
        region,
        descripcion,
        estado,

        -- -------------------------------------------------------------------------
        -- TIMESTAMPS
        -- Parseamos fecha_hora a TIMESTAMP para operaciones temporales
        -- -------------------------------------------------------------------------
        timestamp(fecha_hora) as fecha_hora,
        date(timestamp(fecha_hora))                          as fecha,
        extract(hour from timestamp(fecha_hora))             as hora,
        extract(dayofweek from timestamp(fecha_hora))        as dia_semana,
        format_timestamp('%A', timestamp(fecha_hora), 'es')  as nombre_dia,

        -- -------------------------------------------------------------------------
        -- FLAGS ML
        -- -------------------------------------------------------------------------
        es_anomalia_flag,
        score_anomalia,
        es_duplicado,

        -- -------------------------------------------------------------------------
        -- VALIDACIONES Y FLAGS DE CALIDAD
        -- -------------------------------------------------------------------------

        -- Monto negativo — reversión mal codificada
        case
            when monto_clp < 0
            then true else false
        end as flag_monto_negativo,

        -- Fecha futura — bug en scheduler
        case
            when timestamp(fecha_hora) > current_timestamp()
            then true else false
        end as flag_fecha_futura,

        -- Campo nulo — datos incompletos
        case
            when categoria_gasto is null or region is null
            then true else false
        end as flag_campos_nulos,

        -- PII detectado en descripción por DLP
        case
            when descripcion like '%[CHILE_RUT]%'
              or descripcion like '%[EMAIL_ADDRESS]%'
              or descripcion like '%[PHONE_NUMBER]%'
              or descripcion like '%[CREDIT_CARD_NUMBER]%'
              or descripcion like '%[PERSON_NAME]%'
            then true else false
        end as flag_pii_en_descripcion,

        -- Monto sospechoso UAF — sobre 5 millones debe reportarse
        case
            when monto_clp >= 5000000
            then true else false
        end as flag_uaf_threshold,

        -- Registro limpio
        case
            when monto_clp < 0
              or timestamp(fecha_hora) > current_timestamp()
              or categoria_gasto is null
            then false else true
        end as es_registro_valido,

        -- -------------------------------------------------------------------------
        -- METADATOS
        -- -------------------------------------------------------------------------
        fecha_deidentify,
        current_timestamp() as fecha_staging

    from source
)

select * from validado
