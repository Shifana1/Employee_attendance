{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_table_exists='skip',
        full_refresh=false,
        schema='employee_model',
        pre_hook="{{ history_period_delete_hook() }}"
    )
}}

/*
    Same as int_office_day.sql from stg_daily_attendance_history.
*/

with office_daily as (

    select
        d.emp_code,
        max(d.first_name)                                   as first_name,
        max(d.last_name)                                    as last_name,
        max(d.department)                                   as department,
        max(d.email_normalized)                             as email_normalized,
        max(d.business_email)                               as source_email,
        d.attendance_date,
        min(d.checkin_timestamp)                            as checkin_timestamp,
        max(d.checkout_timestamp)                           as checkout_timestamp,
        sum(d.office_punch_count)                           as punch_count
    from {{ ref('stg_daily_attendance_history') }} d
    where {{ history_period_filter('d') }}
    group by d.emp_code, d.attendance_date

),

office as (

    select
        coalesce(
            hr_ln.employee_id,
            hr_id.employee_id,
            {{ non_email_employee_id("nullif(trim(o.last_name), '')") }},
            {{ non_email_employee_id('o.emp_code') }}
        )                                                   as employee_id,
        coalesce(
            hr_ln.business_email_normalized,
            hr_id.business_email_normalized,
            o.email_normalized
        )                                                   as email_normalized,
        coalesce(
            hr_ln.employee_name,
            hr_id.employee_name,
            nullif(trim(o.first_name), '')
        )                                                   as source_name,
        coalesce(
            hr_ln.business_email,
            hr_id.business_email,
            o.source_email
        )                                                   as source_email,
        coalesce(hr_ln.department, hr_id.department, o.department)
                                                            as department,
        o.attendance_date,
        o.checkin_timestamp                                 as first_punch_ts,
        coalesce(o.checkout_timestamp, o.checkin_timestamp)
                                                            as last_punch_ts,
        o.punch_count
    from office_daily o
    left join {{ ref('int_hr_roster_history') }} hr_ln
        on nullif(trim(o.last_name), '') = hr_ln.employee_id
       and {{ history_period_filter('hr_ln') }}
    left join {{ ref('int_hr_roster_history') }} hr_id
        on o.emp_code = hr_id.employee_id
       and {{ history_period_filter('hr_id') }}

)

select
    coalesce(email_normalized, employee_id)                 as person_key,
    min(employee_id)                                        as employee_id,
    max(email_normalized)                                   as email_normalized,
    max(source_name)                                        as source_name,
    max(source_email)                                       as source_email,
    max(department)                                         as department,
    attendance_date,
    min(first_punch_ts)                                     as first_punch_ts,
    max(last_punch_ts)                                      as last_punch_ts,
    sum(punch_count)                                        as punch_count,
    {{ history_period_columns() }}
from office
group by coalesce(email_normalized, employee_id), attendance_date
