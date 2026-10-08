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
    Same daily first/last punch as stg_daily_attendance.sql from
    stg_attendance_event_history (Spark metrics history).
*/

with events as (

    select
        emp_code,
        first_name,
        last_name,
        nullif(nullif(trim(department), ''), 'null')        as department,
        punch_date,
        punch_timestamp
    from {{ ref('stg_attendance_event_history') }}
    where punch_timestamp is not null
      and punch_date is not null
      and {{ history_period_filter() }}

),

daily as (

    select
        emp_code,
        max(first_name)                                     as first_name,
        max(last_name)                                      as last_name,
        max(department)                                     as department,
        punch_date                                          as attendance_date,
        min(punch_timestamp)                                as checkin_timestamp,
        max(punch_timestamp)                                as checkout_timestamp,
        count(*)                                            as office_punch_count
    from events
    group by emp_code, punch_date

)

select
    emp_code,
    first_name,
    last_name,
    department,
    cast(null as varchar)                                   as email_normalized,
    cast(null as varchar)                                   as business_email,
    cast(attendance_date as varchar)                        as punch_date_raw,
    attendance_date,
    checkin_timestamp,
    checkout_timestamp,
    office_punch_count,
    {{ history_period_columns() }}
from daily
