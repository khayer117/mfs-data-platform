-- Override dbt's default schema naming.
-- Default behaviour produces "public_gold" or "public_staging".
-- This macro makes +schema: gold produce exactly "gold".
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}
    {%- endif -%}
{%- endmacro %}
