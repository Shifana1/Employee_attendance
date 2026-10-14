{% macro onsite_clock_hms(clock_expr) %}
    {# Normalize Iceberg clock_in to HH:mm:ss. Raw values are "06:35" (varchar or time). #}
    case
        when {{ clock_expr }} is null then null
        when trim(cast({{ clock_expr }} as varchar)) in ('', '-', '--', 'null', 'NULL')
        then null
        when regexp_like(
            trim(regexp_replace(cast({{ clock_expr }} as varchar), '\.[0-9]+$', '')),
            '^[0-9]{1,2}:[0-9]{2}$'
        )
        then concat(
            trim(regexp_replace(cast({{ clock_expr }} as varchar), '\.[0-9]+$', '')),
            ':00'
        )
        when regexp_like(
            trim(regexp_replace(cast({{ clock_expr }} as varchar), '\.[0-9]+$', '')),
            '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
        )
        then trim(regexp_replace(cast({{ clock_expr }} as varchar), '\.[0-9]+$', ''))
        else regexp_extract(
            cast({{ clock_expr }} as varchar),
            '([0-9]{1,2}:[0-9]{2}:[0-9]{2})',
            1
        )
    end
{% endmacro %}


{% macro parse_onsite_punch(clock_expr, date_expr) %}
    {#
      dataproducts.raw.onsite_employee.clock_in is "06:35", not a full timestamp.
      try_cast('06:35' AS timestamp) is NULL. Build DATE + ' ' + HH:mm:ss.
      Format the date with %Y-%m-%d so we never get "2026-08-26 00:00:00.000".
    #}
    try(
        date_parse(
            concat(
                date_format(cast({{ date_expr }} as timestamp), '%Y-%m-%d'),
                ' ',
                {{ onsite_clock_hms(clock_expr) }}
            ),
            '%Y-%m-%d %H:%i:%s'
        )
    )
{% endmacro %}
