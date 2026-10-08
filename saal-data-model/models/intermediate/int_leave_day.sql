{{
    config(
        materialized='table',
        on_table_exists='drop'
    )
}}

/*
    Leave days — one row per person_key + calendar date in the leave range.
*/

with leave_days as (

    select
        coalesce(
            hr_email.employee_id,
            hr_prefix.employee_id,
            hr_name.employee_id,
            hr_id.employee_id,
            {{ non_email_employee_id('l.employee_id') }}
        )                                                   as employee_id,
        coalesce(
            hr_email.business_email_normalized,
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            hr_id.business_email_normalized,
            l.email_normalized
        )                                                   as email_normalized,
        coalesce(
            hr_email.employee_name,
            hr_prefix.employee_name,
            hr_name.employee_name,
            hr_id.employee_name,
            l.employee_name
        )                                                   as source_name,
        coalesce(
            hr_email.business_email,
            hr_prefix.business_email,
            hr_name.business_email,
            hr_id.business_email,
            l.email_id
        )                                                   as source_email,
        coalesce(
            hr_email.business_email_normalized,
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            hr_id.business_email_normalized,
            {{ non_email_employee_id('l.employee_id') }},
            l.email_normalized
        )                                                   as person_key,
        u.date_day                                          as attendance_date,
        coalesce(l.is_half_day, false)                      as is_half_day
    from {{ ref('stg_leave_data') }} l
    left join {{ ref('int_hr_roster') }} hr_email
        on l.email_normalized = hr_email.business_email_normalized
       and hr_email.email_rn = 1
    left join {{ ref('int_hr_by_prefix') }} hr_prefix
        on l.email_normalized = hr_prefix.email_prefix
    left join {{ ref('int_hr_roster') }} hr_name
        on {{ normalized_person_name('l.employee_name') }} = hr_name.name_key
       and hr_name.name_count = 1
    left join {{ ref('int_hr_roster') }} hr_id
        on {{ non_email_employee_id('l.employee_id') }} = hr_id.employee_id
    cross join unnest(
        sequence(
            cast(l.leave_from_date as date),
            cast(l.leave_to_date as date)
        )
    ) as u(date_day)
    where coalesce(
            hr_email.business_email_normalized,
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            hr_id.business_email_normalized,
            {{ non_email_employee_id('l.employee_id') }},
            l.email_normalized
          ) is not null

)

select
    person_key,
    min(employee_id)                                        as employee_id,
    max(email_normalized)                                   as email_normalized,
    max(source_name)                                        as source_name,
    max(source_email)                                       as source_email,
    attendance_date,
    max(case when is_half_day then 1 else 0 end) = 1        as is_half_day_leave
from leave_days
group by person_key, attendance_date
