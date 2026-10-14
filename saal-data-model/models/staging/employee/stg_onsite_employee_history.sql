{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='employee_model',
        on_table_exists='skip',
        full_refresh=false,
        pre_hook="{{ history_period_delete_hook() }}"
    )
}}

/*
    Same cleanses as stg_onsite_employee.sql from
    dataproducts.raw.onsite_employee_history. HR match uses current stg_employee_hr.
*/

with source as (

    select * from {{ source('employee_attendance', 'onsite_employee_history') }}
    where {{ history_period_filter() }}

),

source_clean as (

    select
        trim(name)                                          as employee_name,
        {{ normalized_person_name('name') }}                as name_key,
        {{ usable_source_email('email') }}                  as row_email,
        max({{ usable_source_email('email') }}) over (
            partition by {{ normalized_person_name('name') }}
        )                                                   as filled_email,
        coalesce(
            try_cast(date as date),
            try(date_parse(trim(cast(date as varchar)), '%Y-%m-%d')),
            try(date_parse(trim(cast(date as varchar)), '%m/%d/%Y')),
            try(date_parse(trim(cast(date as varchar)), '%d-%b-%Y')),
            try(date_parse(trim(cast(date as varchar)), '%d-%b-%y')),
            try(date_parse(trim(cast(date as varchar)), '%d/%m/%Y')),
            cast(try_cast(date as timestamp) as date)
        )                                                   as attendance_date,
        clock_in,
        clock_out
    from source
    where {{ usable_source_email('email') }} is not null
       or (
            name is not null
        and trim(name) <> ''
        and lower(trim(name)) not in ('null', 'nan', 'none', 'nat')
        and not {{ is_pickle_leak_sql('name') }}
       )

),

hr as (

    select
        employee_id,
        business_email_normalized,
        business_email
    from (
        select
            employee_id,
            business_email_normalized,
            business_email,
            row_number() over (
                partition by business_email_normalized
                order by employee_id
            )                                               as rn
        from {{ ref('stg_employee_hr') }}
        where business_email_normalized is not null
    ) h
    where rn = 1

)

select
    hr.employee_id,
    src.employee_name,
    coalesce(src.row_email, src.filled_email)               as email_normalized,
    coalesce(src.row_email, src.filled_email)               as email,
    src.attendance_date,
    src.clock_in,
    src.clock_out,
    {{ parse_onsite_punch('src.clock_in', 'src.attendance_date') }}
                                                            as first_punch_in,
    {{ parse_onsite_punch('src.clock_out', 'src.attendance_date') }}
                                                            as last_punch_in,
    {{ history_period_columns() }}
from source_clean src
left join hr
    on coalesce(src.row_email, src.filled_email) = hr.business_email_normalized
where src.attendance_date is not null
