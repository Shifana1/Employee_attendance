{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_table_exists='skip',
        full_refresh=false,
        database='dataproducts',
        schema='employee',
        pre_hook="{{ history_period_delete_hook() }}"
    )
}}

/*
    Same as employee_attendance_status.sql from int_attendance_day_history.
*/

with reporting_bounds as (

    {{ attendance_reporting_bounds() }}

),

src as (

    select * from {{ ref('int_attendance_day_history') }}
    where {{ history_period_filter() }}

),

filtered as (

    select
        s.employee_id,
        s.employee_name,
        s.business_email,
        s.department,
        s.attendance_date,
        s.first_punch_ts,
        s.last_punch_ts,
        s.punch_count,
        s.is_leave,
        s.is_half_day_leave
    from src s
    cross join reporting_bounds b
    where s.attendance_date >= b.period_start
      and s.attendance_date < b.period_end

),

adjusted as (

    select
        employee_id,
        employee_name,
        business_email,
        department,
        attendance_date,
        first_punch_ts,
        last_punch_ts                                     as last_punch_ts_raw,
        case
            when first_punch_ts is not null
                and last_punch_ts is not null
                and date_format(first_punch_ts, '%H:%i')
                    = date_format(last_punch_ts, '%H:%i')
            then null
            when punch_count = 1 then null
            else last_punch_ts
        end                                               as last_punch_ts,
        punch_count,
        is_leave,
        is_half_day_leave
    from filtered

),

classified as (

    select
        employee_id,
        employee_name,
        business_email,
        department,
        attendance_date,
        first_punch_ts,
        last_punch_ts,
        punch_count,
        case
            when first_punch_ts is null then 'No punch'
            when last_punch_ts is null then 'Punch incomplete'
            else 'Punch complete'
        end                                               as punch_status,
        case
            when is_leave = 'Yes' and not is_half_day_leave then 'Leave'
            when is_half_day_leave
                and first_punch_ts is not null
                and (
                    hour(first_punch_ts) * 3600
                    + minute(first_punch_ts) * 60
                    + second(first_punch_ts)
                ) > 13 * 3600
            then 'HalfDay - 1st half leave'
            when is_half_day_leave
                and last_punch_ts_raw is not null
                and (
                    hour(last_punch_ts_raw) * 3600
                    + minute(last_punch_ts_raw) * 60
                    + second(last_punch_ts_raw)
                ) < 13 * 3600
            then 'HalfDay - 2nd half leave'
            when is_half_day_leave then 'HalfDay - 1st half leave'
            when first_punch_ts is not null
                and (
                    hour(first_punch_ts) * 3600
                    + minute(first_punch_ts) * 60
                    + second(first_punch_ts)
                ) > 13 * 3600
            then 'Unauthorized half-day leave'
            when last_punch_ts is not null
                and (
                    hour(last_punch_ts) * 3600
                    + minute(last_punch_ts) * 60
                    + second(last_punch_ts)
                ) < 13 * 3600
            then 'Unauthorized half-day leave'
            else 'Present'
        end                                               as attendance_status
    from adjusted

)

select
    employee_id                                           as "EmployeeID",
    employee_name                                         as "EmployeeName",
    business_email                                        as "BusinessEmail",
    date_format(cast(attendance_date as date), '%Y-%m-%d') as "Date",
    department                                            as "Department",
    date_format(first_punch_ts, '%H:%i')                  as "FirstPunch",
    date_format(last_punch_ts, '%H:%i')                   as "LastPunch",
    case
        when first_punch_ts is null or last_punch_ts is null then null
        else concat(
            lpad(
                cast(
                    cast(
                        date_diff('minute', first_punch_ts, last_punch_ts) / 60
                        as integer
                    ) as varchar
                ),
                2,
                '0'
            ),
            ':',
            lpad(
                cast(
                    cast(
                        date_diff('minute', first_punch_ts, last_punch_ts) % 60
                        as integer
                    ) as varchar
                ),
                2,
                '0'
            )
        )
    end                                                   as "TotalHoursInOffice",
    punch_count                                           as "PunchCount",
    punch_status                                          as "PunchStatus",
    attendance_status                                     as "AttendanceStatus",
    {{ history_period_columns() }}
from classified
