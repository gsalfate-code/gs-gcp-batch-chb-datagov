-- =============================================================================
-- MACRO: generate_schema_name
-- DESCRIPCIÓN:
--   Sobrescribe el comportamiento por defecto de DBT que concatena
--   el dataset del profile con el schema del modelo.
--   Con esta macro, el schema definido en dbt_project.yml se usa tal cual.
--
-- SIN esta macro: profile_dataset + "_" + model_schema = "gold_silver"
-- CON esta macro: model_schema tal cual               = "silver"
-- =============================================================================
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
