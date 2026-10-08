{% macro source_or_empty(source_name, table_name, columns=[]) %}
{#-
    Leftover D365/Teamdeck staging calls this when those sources are absent.
    Defaults keep compile from failing if project vars were not loaded.
-#}
{%- if source_name == 'd365' -%}
    {%- set source_enabled = var('enable_d365_sources', false) -%}
{%- elif source_name == 'teamdeck' -%}
    {%- set source_enabled = var('enable_teamdeck_sources', false) -%}
{%- else -%}
    {%- set source_enabled = true -%}
{%- endif -%}
{%- if source_enabled is string -%}
    {%- set source_enabled = source_enabled | lower in ['true', '1', 'yes'] -%}
{%- endif -%}
{%- if source_enabled -%}
    {{ source(source_name, table_name) }}
{%- else -%}
    (
        select
        {%- if columns %}
            {%- for column in columns %}
                {%- if column is mapping %}
        cast(null as {{ column.get('type', 'varchar') }}) as "{{ column['name'] }}"
                {%- else %}
        cast(null as varchar) as "{{ column }}"
                {%- endif %}
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        {%- else %}
        cast(null as varchar) as _unused
        {%- endif %}
        where false
    )
{%- endif -%}
{% endmacro %}
