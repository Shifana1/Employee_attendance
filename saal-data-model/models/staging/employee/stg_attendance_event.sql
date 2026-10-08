{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for raw punch events from attendance terminals.

    Cleansing applied:
    - Parse punch_time varchar to TIMESTAMP
    - Extract punch_date as DATE
    - TRIM all string fields
    - Filter out NULL emp_code and NULL punch_time

    NOTE: 89% of punch_state_display is "Unknown". Daily first/last punch
    is computed in stg_daily_attendance from punch_timestamp, not punch_state.
*/

with source as (

    select * from {{ source('employee_attendance', 'employee_attendance_metrics') }}

),

cleaned as (

    select
        id                                                  as event_id,
        trim(emp_code)                                      as emp_code,

        -- timestamps
        try_cast(punch_time as timestamp)                   as punch_timestamp,
        cast(try_cast(punch_time as timestamp) as date)     as punch_date,

        -- punch classification
        trim(punch_state_display)                           as punch_direction,
        trim(punch_state)                                   as punch_state_code,
        trim(verify_type_display)                           as verification_method,

        -- location & device
        trim(gps_location)                                  as gps_location,
        trim(area_alias)                                    as area_name,
        trim(terminal_alias)                                as terminal_name,
        trim(terminal_sn)                                   as terminal_serial,

        -- biometric
        temperature                                         as body_temperature_celsius,
        trim(is_mask)                                       as mask_worn,

        -- context
        trim(department)                                    as department,
        trim(first_name)                                    as first_name,
        trim(last_name)                                     as last_name,

        -- metadata
        try_cast(upload_time as timestamp)                  as upload_timestamp

    from source
    where emp_code is not null
      and punch_time is not null

)

select * from cleaned
