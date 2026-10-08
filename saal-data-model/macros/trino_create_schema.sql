{% macro trino__create_schema(relation) -%}
  {{ ensure_iceberg_schema(relation.database, relation.schema) }}
{%- endmacro %}
