{{
    config(
        materialized='view'
    )
}}

/*
    Daily first and last office punch from raw iClock events.

    Grain: one row per emp_code + punch_date.
    Names can vary across events; take any non-null with max().
    last_name on iClock is the HR emp_id, not a surname.
    Department comes from the punch terminal (employee_attendance_metrics).
    SAAL email is filled in int_attendance_day from HR.
*/

with events as (

    select
        emp_code,
        first_name,
        last_name,
        nullif(nullif(trim(department), ''), 'null')        as department,
        punch_date,
        punch_timestamp
    from {{ ref('stg_attendance_event') }}
    where punch_timestamp is not null
      and punch_date is not null

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
    office_punch_count
from daily
