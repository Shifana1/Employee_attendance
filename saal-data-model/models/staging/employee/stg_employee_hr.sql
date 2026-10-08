{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for the current HR export.

    Employee ID is the real emp id (join to iClock last_name and, as a
    fallback, punch emp_code). Full Name, Email, and Department come from
    the same file. There is no badge emp_code in this source.

    Cleansing applied:
    - TRIM all string fields
    - Normalize business_email to lowercase
    - Parse joining_date from "15-Nov-2023" or "9-Jun-19"
*/

with source as (

    select * from {{ source('employee_attendance', 'employee_hr_data') }}

),

cleaned as (

    select
        trim(cast(employee_id as varchar))                  as employee_id,
        trim(employee_name)                                 as employee_name,
        lower(trim(business_email))                         as business_email_normalized,
        trim(business_email)                                as business_email,
        nullif(nullif(trim(department), ''), 'null')        as department,
        trim(job_role)                                      as job_role,
        trim(company)                                       as company,
        nullif(trim(line_manager), '')                      as line_manager,
        cast(null as varchar)                               as department_head,
        coalesce(
            try(date_parse(trim(joining_date), '%d-%b-%Y')),
            try(date_parse(trim(joining_date), '%d-%b-%y')),
            try_cast(joining_date as date)
        )                                                   as joining_date,
        nullif(trim(current_status), '')                    as employment_status

    from source
    where employee_id is not null
      and trim(cast(employee_id as varchar)) <> ''

)

select * from cleaned
