-- =============================================================================
-- MODELO: mart_calidad_datos
-- CAPA: Gold
-- MATERIALIZACIÓN: table
-- DESCRIPCIÓN:
--   Score de calidad de datos por tabla y tipo de error.
--   Permite monitorear tendencia de errores día a día.
--   Alimenta el dashboard de calidad en Looker Studio y Power BI.
--
-- AUDIENCIA: Data Steward, Data Engineer, Calidad
-- ACTUALIZACIÓN: cada hora (via DAG)
-- =============================================================================

with clientes as (
    select * from {{ ref('stg_clientes') }}
),

transacciones as (
    select * from {{ ref('stg_transacciones') }}
),

-- -----------------------------------------------------------------
-- CALIDAD CLIENTES
-- -----------------------------------------------------------------
calidad_clientes as (
    select
        current_date()                                      as fecha,
        current_timestamp()                                 as timestamp_reporte,
        'clientes_deidentified'                             as tabla,

        count(*)                                            as total_registros,
        countif(es_registro_valido)                         as registros_validos,
        countif(not es_registro_valido)                     as registros_invalidos,

        -- Por tipo de error
        countif(flag_email_invalido)                        as errores_email,
        countif(flag_genero_invalido)                       as errores_genero,
        countif(flag_score_fuera_rango)                     as errores_score_rango,
        countif(flag_score_categoria_inconsistente)         as errores_score_categoria,
        countif(flag_edad_invalida)                         as errores_edad,
        countif(flag_saldo_invalido)                        as errores_saldo,

        -- Score de calidad (0-100)
        round(
            safe_divide(countif(es_registro_valido), count(*)) * 100
        , 2)                                                as score_calidad,

        -- Semáforo
        case
            when safe_divide(countif(es_registro_valido), count(*)) >= 0.98 then 'VERDE'
            when safe_divide(countif(es_registro_valido), count(*)) >= 0.95 then 'AMARILLO'
            else 'ROJO'
        end                                                 as semaforo

    from clientes
),

-- -----------------------------------------------------------------
-- CALIDAD TRANSACCIONES
-- -----------------------------------------------------------------
calidad_transacciones as (
    select
        current_date()                                      as fecha,
        current_timestamp()                                 as timestamp_reporte,
        'transacciones_deidentified'                        as tabla,

        count(*)                                            as total_registros,
        countif(es_registro_valido)                         as registros_validos,
        countif(not es_registro_valido)                     as registros_invalidos,

        -- Por tipo de error
        countif(flag_monto_negativo)                        as errores_monto_negativo,
        countif(flag_fecha_futura)                          as errores_fecha_futura,
        countif(flag_campos_nulos)                          as errores_campos_nulos,
        countif(flag_pii_en_descripcion)                    as errores_pii_descripcion,
        countif(flag_uaf_threshold)                         as alertas_uaf,
        countif(es_duplicado)                               as registros_duplicados,

        -- Score de calidad
        round(
            safe_divide(countif(es_registro_valido), count(*)) * 100
        , 2)                                                as score_calidad,

        -- Semáforo
        case
            when safe_divide(countif(es_registro_valido), count(*)) >= 0.98 then 'VERDE'
            when safe_divide(countif(es_registro_valido), count(*)) >= 0.95 then 'AMARILLO'
            else 'ROJO'
        end                                                 as semaforo

    from transacciones
),

-- Union de ambas tablas para dashboard unificado
unificado as (
    select
        fecha,
        timestamp_reporte,
        tabla,
        total_registros,
        registros_validos,
        registros_invalidos,
        score_calidad,
        semaforo,
        -- Errores clientes
        errores_email,
        errores_genero,
        errores_score_rango,
        errores_score_categoria,
        errores_edad,
        errores_saldo,
        -- Errores transacciones (null para clientes)
        null as errores_monto_negativo,
        null as errores_fecha_futura,
        null as errores_campos_nulos,
        null as errores_pii_descripcion,
        null as alertas_uaf,
        null as registros_duplicados
    from calidad_clientes

    union all

    select
        fecha,
        timestamp_reporte,
        tabla,
        total_registros,
        registros_validos,
        registros_invalidos,
        score_calidad,
        semaforo,
        -- Errores clientes (null para transacciones)
        null as errores_email,
        null as errores_genero,
        null as errores_score_rango,
        null as errores_score_categoria,
        null as errores_edad,
        null as errores_saldo,
        -- Errores transacciones
        errores_monto_negativo,
        errores_fecha_futura,
        errores_campos_nulos,
        errores_pii_descripcion,
        alertas_uaf,
        registros_duplicados
    from calidad_transacciones
)

select * from unificado
