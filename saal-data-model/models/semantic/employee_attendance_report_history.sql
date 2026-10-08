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
    Same as employee_attendance_report.sql from int_attendance_day_history.
*/

with reporting_bounds as (

    {{ attendance_reporting_bounds() }}

),

src as (

    select * from {{ ref('int_attendance_day_history') }}
    where {{ history_period_filter() }}

),

adjusted as (

    select
        s.employee_id,
        s.employee_name,
        s.business_email,
        s.attendance_date,
        s.first_punch_ts,
        case
            when s.first_punch_ts is not null
                and s.last_punch_ts is not null
                and date_format(s.first_punch_ts, '%H:%i')
                    = date_format(s.last_punch_ts, '%H:%i')
                and hour(s.first_punch_ts) < 12
            then null
            else s.last_punch_ts
        end                                               as last_punch_ts,
        s.saal_office_or_onsite,
        s.is_leave
    from src s
    cross join reporting_bounds b
    where s.attendance_date >= b.period_start
      and s.attendance_date < b.period_end

)

select
    employee_id                                           as "EmployeeID",
    employee_name                                         as "EmployeeName",
    business_email                                        as "BusinessEmail",
    date_format(cast(attendance_date as date), '%Y-%m-%d') as "Date",
    date_format(first_punch_ts, '%H:%i')                  as "FirstPunchIn",
    date_format(last_punch_ts, '%H:%i')                   as "LastPunchOut",
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
    saal_office_or_onsite                                 as "SaalOfficeOrOnsite",
    is_leave                                              as "IsLeave",
    {{ history_period_columns() }}
from adjusted
