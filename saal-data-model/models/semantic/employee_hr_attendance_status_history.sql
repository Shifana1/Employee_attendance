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
    Same as employee_hr_attendance_status.sql from int_attendance_day_history.
    HR identity is the current export (no Spark HR history).
*/

with reporting_bounds as (

    {{ attendance_reporting_bounds() }}

),

calendar_dates as (

    select date_day
    from reporting_bounds b
    cross join unnest(
        sequence(
            b.period_start,
            date_add('day', -1, b.period_end),
            interval '1' day
        )
    ) as t(date_day)
    where day_of_week(date_day) between 1 and 5

),

hr_roster as (

    select
        employee_id,
        employee_name,
        business_email,
        business_email_normalized,
        department
    from {{ ref('stg_employee_hr') }}

),

period_attendance as (

    select
        {{ non_email_employee_id('i.employee_id') }}        as employee_id,
        i.employee_name,
        i.business_email,
        lower(trim(i.business_email))                       as business_email_normalized,
        i.department,
        i.attendance_date,
        i.first_punch_ts,
        i.last_punch_ts,
        i.punch_count,
        i.is_leave,
        i.is_half_day_leave
    from {{ ref('int_attendance_day_history') }} i
    cross join reporting_bounds b
    where {{ history_period_filter('i') }}
      and i.attendance_date >= b.period_start
      and i.attendance_date < b.period_end

),

non_hr_roster as (

    select
        coalesce(p.employee_id, p.employee_name)            as employee_id,
        max(p.employee_name)                                as employee_name,
        max(p.business_email)                               as business_email,
        max(p.business_email_normalized)                    as business_email_normalized,
        max(p.department)                                   as department
    from period_attendance p
    where coalesce(p.employee_id, p.employee_name) is not null
      and not exists (
            select 1
            from hr_roster h
            where h.employee_id = p.employee_id
               or (
                    h.business_email_normalized is not null
                and p.business_email_normalized = h.business_email_normalized
               )
               or (
                    {{ normalized_person_name('h.employee_name') }} is not null
                and {{ normalized_person_name('p.employee_name') }}
                    = {{ normalized_person_name('h.employee_name') }}
               )
      )
    group by coalesce(p.employee_id, p.employee_name)

),

roster as (

    select
        employee_id,
        employee_name,
        business_email,
        department
    from hr_roster

    union

    select
        employee_id,
        employee_name,
        business_email,
        department
    from non_hr_roster

),

hr_calendar as (

    select
        h.employee_id,
        h.employee_name,
        h.business_email,
        h.department,
        d.date_day                                          as attendance_date
    from roster h
    cross join calendar_dates d

),

attendance_facts as (

    select
        employee_id,
        employee_name,
        business_email,
        department,
        attendance_date,
        first_punch_ts,
        last_punch_ts,
        punch_count,
        is_leave,
        is_half_day_leave
    from period_attendance

),

joined as (

    select
        h.employee_id,
        h.employee_name,
        h.business_email,
        h.department,
        h.attendance_date,
        a.first_punch_ts,
        a.last_punch_ts,
        coalesce(a.punch_count, 0)                          as punch_count,
        coalesce(a.is_leave, 'No')                          as is_leave,
        coalesce(a.is_half_day_leave, false)                as is_half_day_leave
    from hr_calendar h
    left join attendance_facts a
        on coalesce(
                {{ non_email_employee_id('h.employee_id') }},
                h.business_email
            )
            = coalesce(
                {{ non_email_employee_id('a.employee_id') }},
                a.business_email
            )
       and h.attendance_date = a.attendance_date

),

adjusted as (

    select
        employee_id,
        employee_name,
        business_email,
        department,
        attendance_date,
        first_punch_ts,
        last_punch_ts                                       as last_punch_ts_raw,
        case
            when first_punch_ts is not null
                and last_punch_ts is not null
                and date_format(first_punch_ts, '%H:%i')
                    = date_format(last_punch_ts, '%H:%i')
            then null
            when punch_count = 1 then null
            else last_punch_ts
        end                                                 as last_punch_ts,
        punch_count,
        is_leave,
        is_half_day_leave
    from joined

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
            when first_punch_ts is null and is_leave = 'No' then 'Missing punch'
            when first_punch_ts is null then 'No punch'
            when last_punch_ts is null then 'Punch incomplete'
            else 'Punch complete'
        end                                                 as punch_status,
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
            when first_punch_ts is null and is_leave = 'No' then 'Unauthorized leave'
            else 'Present'
        end                                                 as attendance_status
    from adjusted

)

select
    employee_id                                             as "EmployeeID",
    employee_name                                           as "EmployeeName",
    business_email                                          as "BusinessEmail",
    date_format(cast(attendance_date as date), '%Y-%m-%d')  as "Date",
    department                                              as "Department",
    date_format(first_punch_ts, '%H:%i')                    as "FirstPunch",
    date_format(last_punch_ts, '%H:%i')                     as "LastPunch",
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
    end                                                     as "TotalHoursInOffice",
    punch_count                                             as "PunchCount",
    punch_status                                            as "PunchStatus",
    attendance_status                                       as "AttendanceStatus",
    {{ history_period_columns() }}
from classified
