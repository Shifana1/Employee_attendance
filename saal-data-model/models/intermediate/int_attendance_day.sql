{{
    config(
        materialized='table',
        on_table_exists='drop'
    )
}}

/*
    Daily attendance merge — one row per person_key + calendar date.

    Office, onsite, and leave are pre-materialized tables so Trino does not
    inline those queries twice. HR overlay uses int_hr_roster.
*/

with merged as (

    select
        coalesce(of.person_key, os.person_key, lv.person_key)
                                                            as person_key,
        coalesce(of.attendance_date, os.attendance_date, lv.attendance_date)
                                                            as attendance_date,
        coalesce(of.email_normalized, os.email_normalized, lv.email_normalized)
                                                            as email_normalized,
        coalesce(of.employee_id, os.employee_id, lv.employee_id)
                                                            as source_employee_id,
        coalesce(of.source_name, os.source_name, lv.source_name)
                                                            as source_name,
        coalesce(of.source_email, os.source_email, lv.source_email)
                                                            as source_email,
        of.department                                       as department,

        case
            when of.first_punch_ts is not null and os.first_punch_ts is not null
                then least(of.first_punch_ts, os.first_punch_ts)
            else coalesce(of.first_punch_ts, os.first_punch_ts)
        end                                                 as first_punch_ts,
        case
            when of.last_punch_ts is not null and os.last_punch_ts is not null
                then greatest(of.last_punch_ts, os.last_punch_ts)
            else coalesce(of.last_punch_ts, os.last_punch_ts)
        end                                                 as last_punch_ts,

        case
            when of.person_key is not null and os.person_key is not null
                then 'Saal Office / Onsite'
            when os.person_key is not null then 'Onsite'
            when of.person_key is not null then 'Saal Office'
            when lv.person_key is not null then 'Leave'
        end                                                 as saal_office_or_onsite,

        coalesce(of.punch_count, 0) + coalesce(os.punch_count, 0)
                                                            as punch_count,

        case
            when lv.person_key is not null then 'Yes'
            else 'No'
        end                                                 as is_leave,
        coalesce(lv.is_half_day_leave, false)               as is_half_day_leave
    from {{ ref('int_office_day') }} of
    full outer join {{ ref('int_onsite_day') }} os
        on of.person_key = os.person_key
       and of.attendance_date = os.attendance_date
    full outer join {{ ref('int_leave_day') }} lv
        on coalesce(of.person_key, os.person_key) = lv.person_key
       and coalesce(of.attendance_date, os.attendance_date) = lv.attendance_date

)

select
    {{ non_email_employee_id("coalesce(
        hr_email.employee_id,
        hr_prefix.employee_id,
        hr_name.employee_id,
        m.source_employee_id,
        m.person_key
    )") }}                                                  as employee_id,
    nullif(
        trim(
            coalesce(
                hr_email.employee_name,
                hr_prefix.employee_name,
                hr_name.employee_name,
                m.source_name,
                ''
            )
        ),
        ''
    )                                                       as employee_name,
    coalesce(
        hr_email.business_email,
        hr_prefix.business_email,
        hr_name.business_email,
        m.source_email
    )                                                       as business_email,
    coalesce(
        hr_email.department,
        hr_prefix.department,
        hr_name.department,
        m.department
    )                                                       as department,
    m.attendance_date,
    m.first_punch_ts,
    m.last_punch_ts,
    m.punch_count,
    m.saal_office_or_onsite,
    m.is_leave,
    m.is_half_day_leave
from merged m
left join {{ ref('int_hr_roster') }} hr_email
    on m.email_normalized = hr_email.business_email_normalized
   and hr_email.email_rn = 1
left join {{ ref('int_hr_by_prefix') }} hr_prefix
    on m.email_normalized = hr_prefix.email_prefix
left join {{ ref('int_hr_roster') }} hr_name
    on {{ normalized_person_name('m.source_name') }} = hr_name.name_key
   and hr_name.name_count = 1
