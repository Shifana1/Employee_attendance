{{
    config(
        materialized='table',
        on_table_exists='drop'
    )
}}

/*
    Onsite ZKTeco days — one row per person_key + date.
*/

with onsite as (

    select
        {{ non_email_employee_id("coalesce(s.employee_id, hr_prefix.employee_id, hr_name.employee_id)") }}
                                                            as employee_id,
        coalesce(
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            {{ usable_source_email('s.email_normalized') }}
        )                                                   as email_normalized,
        coalesce(
            hr_prefix.employee_name,
            hr_name.employee_name,
            s.employee_name
        )                                                   as source_name,
        coalesce(
            hr_prefix.business_email,
            hr_name.business_email,
            {{ usable_source_email('s.email') }}
        )                                                   as source_email,
        coalesce(
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            {{ usable_source_email('s.email_normalized') }},
            {{ non_email_employee_id('s.employee_id') }},
            nullif(s.employee_name, '')
        )                                                   as person_key,
        s.attendance_date,
        s.first_punch_in,
        s.last_punch_in,
        (
            case when s.first_punch_in is not null then 1 else 0 end
            + case when s.last_punch_in is not null then 1 else 0 end
        )                                                   as punch_count
    from {{ ref('stg_onsite_employee') }} s
    left join {{ ref('int_hr_by_prefix') }} hr_prefix
        on s.email_normalized = hr_prefix.email_prefix
    left join {{ ref('int_hr_roster') }} hr_name
        on {{ normalized_person_name('s.employee_name') }} = hr_name.name_key
       and hr_name.name_count = 1
    where s.attendance_date is not null
      and coalesce(
            hr_prefix.business_email_normalized,
            hr_name.business_email_normalized,
            {{ usable_source_email('s.email_normalized') }},
            {{ non_email_employee_id('s.employee_id') }},
            nullif(s.employee_name, '')
          ) is not null

)

select
    person_key,
    max(employee_id)                                        as employee_id,
    max(email_normalized)                                   as email_normalized,
    max(source_name)                                        as source_name,
    max(source_email)                                       as source_email,
    attendance_date,
    min(coalesce(first_punch_in, last_punch_in))            as first_punch_ts,
    max(coalesce(last_punch_in, first_punch_in))            as last_punch_ts,
    sum(punch_count)                                        as punch_count
from onsite
group by person_key, attendance_date
