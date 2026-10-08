{% macro iceberg_schema_location(database, schema_name) -%}
s3a://{{ database }}/{{ schema_name }}
{%- endmacro %}


{% macro ensure_iceberg_schema(database, schema_name) -%}
  {%- call statement('ensure_iceberg_schema') -%}
    create schema if not exists {{ database }}.{{ schema_name }}
    with (location = '{{ iceberg_schema_location(database, schema_name) }}')
  {%- endcall -%}
{%- endmacro %}
