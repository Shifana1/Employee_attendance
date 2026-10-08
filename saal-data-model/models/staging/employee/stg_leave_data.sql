{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for leave application data.

    Keeps employee id, email, leave dates, type, status, and duration.
    Filters to Approved and Pending. business_email is the join key to HR.
    Half-day leave is duration 0.5 when the unit is days or blank.
*/

with source as (

    select * from {{ source('employee_attendance', 'leave_data') }}

),

cleaned as (

    select
        trim(employee_id)                                   as employee_id,
        trim(employee_name)                                 as employee_name,
        lower(trim(email_id))                               as email_normalized,
        trim(email_id)                                      as email_id,
        -- date_parse returns timestamp; wrap coalesce so Trino does not
        -- promote leave bounds to timestamp(3) (sequence() then fails).
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

select * from cleaned
where leave_from_date is not null
  and leave_to_date is not null
  and leave_to_date >= leave_from_date
