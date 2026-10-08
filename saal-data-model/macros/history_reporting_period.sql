{% macro history_reporting_year() -%}
    {%- if var('attendance_year', none) is not none
        and var('attendance_month', none) is not none -%}
        {{ var('attendance_year') | int }}
    {%- else -%}
        {%- set year = run_started_at.year -%}
        {%- set month = run_started_at.month - 1 -%}
        {%- if month == 0 -%}
            {%- set year = year - 1 -%}
        {%- endif -%}
        {{ year }}
    {%- endif -%}
{%- endmacro %}


{% macro history_reporting_month() -%}
    {%- if var('attendance_year', none) is not none
        and var('attendance_month', none) is not none -%}
        {{ var('attendance_month') | int }}
    {%- else -%}
        {%- set month = run_started_at.month - 1 -%}
        {%- if month == 0 -%}
            {%- set month = 12 -%}
        {%- endif -%}
        {{ month }}
    {%- endif -%}
{%- endmacro %}


{% macro history_period_delete_hook() -%}
    {%- if is_incremental() -%}
        delete from {{ this }}
        where reporting_year = {{ history_reporting_year() }}
          and reporting_month = {{ history_reporting_month() }}
    {%- endif -%}
{%- endmacro %}


{% macro history_period_columns() -%}
    {{ history_reporting_year() }} as reporting_year,
    {{ history_reporting_month() }} as reporting_month
{%- endmacro %}


{% macro history_period_filter(relation_alias=none) -%}
    {%- set prefix = (relation_alias ~ '.') if relation_alias else '' -%}
    {{ prefix }}reporting_year = {{ history_reporting_year() }}
    and {{ prefix }}reporting_month = {{ history_reporting_month() }}
{%- endmacro %}
