{% macro non_email_employee_id(expr) %}
    case
        when {{ expr }} is null then null
        when trim(cast({{ expr }} as varchar)) = '' then null
        when strpos(trim(cast({{ expr }} as varchar)), '@') > 0 then null
        when {{ is_pickle_leak_sql(expr) }} then null
        else trim(cast({{ expr }} as varchar))
    end
{% endmacro %}


{% macro is_pickle_leak_sql(expr) %}
    (
        {{ expr }} is not null
        and (
            lower(trim(cast({{ expr }} as varchar))) like '%razorvine.pickle%'
            or lower(trim(cast({{ expr }} as varchar))) like '%classdictconstructor%'
        )
    )
{% endmacro %}


{% macro usable_source_email(expr) %}
    {# Real email for joins/grain. Drops Spark pickle leftovers such as ClassDictConstructor@hex. #}
    case
        when {{ expr }} is null then null
        when {{ is_pickle_leak_sql(expr) }} then null
        when strpos(trim(cast({{ expr }} as varchar)), '@') = 0 then null
        else lower(trim(cast({{ expr }} as varchar)))
    end
{% endmacro %}


{% macro normalized_person_name(expr) %}
    nullif(
        lower(trim(regexp_replace(coalesce({{ expr }}, ''), '\s+', ' '))),
        ''
    )
{% endmacro %}


{% macro is_truncated_email(expr) %}
    {# ZKTeco EmailN is capped at 20 chars: ahmed.hussein@saal.a, mohamed.alwahedi@saa #}
    (
        {{ expr }} is not null
        and strpos({{ expr }}, '@') > 0
        and not {{ is_pickle_leak_sql(expr) }}
        and (
            strpos(split_part({{ expr }}, '@', 2), '.') = 0
            or length(split_part(split_part({{ expr }}, '@', 2), '.', 2)) < 2
        )
    )
{% endmacro %}
