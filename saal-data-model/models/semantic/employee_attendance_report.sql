{{
    config(
        materialized='table',
        database='dataproducts',
        schema='employee',
        on_table_exists='drop'
    )
}}

/*
    Employee attendance report — Excel column format.

    Grain: one row per coalesce(BusinessEmail, EmployeeID) + Date.
    Office and onsite punches for the same email on the same day are
    combined. Leave days still show punch times when they exist.
    People who are not in HR are kept. Office, onsite, and leave days
    without an email match stay in the table with null BusinessEmail.

    A single punch (first time equals last time) before 12:00 is check-in
    only: LastPunchOut is null. Restricted to the configured reporting month
    (dbt vars attendance_year, attendance_month, or the previous calendar month).
    Rows are ordered by Date ascending. Date is YYYY-MM-DD with no timestamp.
*/

with reporting_bounds as (

    {{ attendance_reporting_bounds() }}

),

src as (

    select * from {{ ref('int_attendance_day') }}

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
    is_leave                                              as "IsLeave"
from adjusted
order by
    attendance_date asc,
    business_email,
    employee_id
