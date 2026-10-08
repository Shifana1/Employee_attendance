{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_table_exists='skip',
        full_refresh=false,
        database='dataproducts',
        schema='employee',
        pre_hook="{{ history_period_delete_hook() }}"
    )
}}

/*
    Monthly snapshot of current HR departments (no Spark HR history).
*/

with hr as (

    select
        department,
        department_head,
        employee_id
    from {{ ref('stg_employee_hr') }}
    where department is not null
      and trim(department) <> ''

)

select
    department,
    max(department_head)                                    as department_head,
    count(distinct employee_id)                             as employee_count,
    {{ history_period_columns() }}
from hr
group by department
