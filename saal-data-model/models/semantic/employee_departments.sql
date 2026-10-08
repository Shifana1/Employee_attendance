{{
    config(
        materialized='table',
        database='dataproducts',
        schema='employee',
        on_table_exists='drop'
    )
}}

/*
    Distinct HR departments from the current export. One row per department.
    department_head is unused in this source (no department-head column).
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
    count(distinct employee_id)                             as employee_count
from hr
group by department
order by department
