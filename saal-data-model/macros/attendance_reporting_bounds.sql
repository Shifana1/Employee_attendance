{% macro attendance_reporting_bounds() %}
    {%- if var('attendance_year', none) is not none
        and var('attendance_month', none) is not none -%}
        {%- set year = var('attendance_year') | int -%}
        {%- set month = var('attendance_month') | int -%}
    {%- else -%}
        {%- set year = run_started_at.year -%}
        {%- set month = run_started_at.month - 1 -%}
        {%- if month == 0 -%}
            {%- set year = year - 1 -%}
            {%- set month = 12 -%}
        {%- endif -%}
    {%- endif -%}
    {%- set month_str = '%02d' | format(month | int) -%}
    select
        date '{{ year }}-{{ month_str }}-01'                    as period_start,
        date_add(
            'month',
            1,
            date '{{ year }}-{{ month_str }}-01'
        )                                                       as period_end
{% endmacro %}
