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
    Same cleanses as stg_leave_data.sql from dataproducts.raw.leave_data_history.
    Incremental delete+append for the reporting year-month only.
*/

with source as (

    select * from {{ source('employee_attendance', 'leave_data_history') }}
    where {{ history_period_filter() }}

),

cleaned as (

    select
        trim(employee_id)                                   as employee_id,
        trim(employee_name)                                 as employee_name,
        lower(trim(email_id))                               as email_normalized,
        trim(email_id)                                      as email_id,
        cast(coalesce(
            try_cast(leave_from_date as date),
            try(cast(date_parse(trim(leave_from_date), '%Y-%m-%d') as date)),
            try(cast(date_parse(trim(leave_from_date), '%d-%b-%y') as date)),
            try(cast(date_parse(trim(leave_from_date), '%d/%m/%Y') as date)),
            try(cast(try_cast(leave_from_date as timestamp) as date))
        ) as date)                                          as leave_from_date,
        cast(coalesce(
            try_cast(leave_to_date as date),
            try(cast(date_parse(trim(leave_to_date), '%Y-%m-%d') as date)),
            try(cast(date_parse(trim(leave_to_date), '%d-%b-%y') as date)),
            try(cast(date_parse(trim(leave_to_date), '%d/%m/%Y') as date)),
            try(cast(try_cast(leave_to_date as timestamp) as date))
        ) as date)                                          as leave_to_date,
        nullif(trim(leave_type), '')                        as leave_type,
        nullif(trim(status), '')                            as status,
        nullif(trim(working_days_hours), '')                as working_days_hours,
        nullif(trim(duration_unit), '')                     as duration_unit,
        try_cast(
            regexp_replace(trim(working_days_hours), ',', '')
            as double
        )                                                   as working_days_num,
        (
            (
                nullif(trim(duration_unit), '') is null
                or lower(trim(duration_unit)) like '%day%'
            )
            and try_cast(
                regexp_replace(trim(working_days_hours), ',', '')
                as double
            ) = 0.5
        )                                                   as is_half_day

    from source
    where employee_id is not null
      and trim(employee_id) <> ''
      and lower(trim(employee_id)) not in ('null', 'nan', 'none', 'nat')
      and lower(trim(status)) in ('approved', 'pending')

)

select
    *,
    {{ history_period_columns() }}
from cleaned
where leave_from_date is not null
  and leave_to_date is not null
  and leave_to_date >= leave_from_date
