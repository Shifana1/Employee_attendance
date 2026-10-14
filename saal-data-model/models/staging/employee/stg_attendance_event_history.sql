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
    Same cleanses as stg_attendance_event.sql from
    dataproducts.raw.employee_attendance_metrics_history.
*/

with source as (

    select * from {{ source('employee_attendance', 'employee_attendance_metrics_history') }}
    where {{ history_period_filter() }}

),

cleaned as (

    select
        id                                                  as event_id,
        trim(emp_code)                                      as emp_code,
        try_cast(punch_time as timestamp)                   as punch_timestamp,
        cast(try_cast(punch_time as timestamp) as date)     as punch_date,
        trim(punch_state_display)                           as punch_direction,
        trim(punch_state)                                   as punch_state_code,
        trim(verify_type_display)                           as verification_method,
        trim(gps_location)                                  as gps_location,
        trim(area_alias)                                    as area_name,
        trim(terminal_alias)                                as terminal_name,
        trim(terminal_sn)                                   as terminal_serial,
        temperature                                         as body_temperature_celsius,
        trim(is_mask)                                       as mask_worn,
        trim(department)                                    as department,
        trim(first_name)                                    as first_name,
        trim(last_name)                                     as last_name,
        try_cast(upload_time as timestamp)                  as upload_timestamp
    from source
    where emp_code is not null
      and punch_time is not null

)

select
    *,
    {{ history_period_columns() }}
from cleaned
