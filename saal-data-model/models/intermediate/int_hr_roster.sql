{{
    config(
        materialized='table',
        on_table_exists='drop'
    )
}}

/*
    One HR person per employee_id. name_count = 1 means the full name is
    unique. email_rn = 1 is the chosen row for that email.
*/

with base as (

    select
        employee_id,
        employee_name,
        business_email_normalized,
        business_email,
        department,
        {{ normalized_person_name('employee_name') }}       as name_key,
        row_number() over (
            partition by employee_id
            order by business_email_normalized
        )                                                   as id_rn
    from {{ ref('stg_employee_hr') }}
    where employee_id is not null
      and trim(employee_id) <> ''

),

deduped as (

    select
        employee_id,
        employee_name,
        business_email_normalized,
        business_email,
        department,
        name_key
    from base
    where id_rn = 1

)

select
    employee_id,
    employee_name,
    business_email_normalized,
    business_email,
    department,
    name_key,
    count(*) over (partition by name_key)                   as name_count,
    row_number() over (
        partition by business_email_normalized
        order by employee_id
    )                                                       as email_rn
from deduped
