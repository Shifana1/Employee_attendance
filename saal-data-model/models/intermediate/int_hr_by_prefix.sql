{{
    config(
        materialized='table',
        on_table_exists='drop'
    )
}}

/*
    Unique HR match for truncated ZKTeco/leave emails
    (ahmed.hussein@saal.a → ahmed.hussein@saal.ai).
*/

with truncated_email_keys as (

    select email_normalized                                 as email_prefix
    from {{ ref('stg_leave_data') }}
    where {{ is_truncated_email('email_normalized') }}

)

select
    k.email_prefix,
    min(h.employee_id)                                      as employee_id,
    min(h.employee_name)                                    as employee_name,
    min(h.business_email)                                   as business_email,
    min(h.business_email_normalized)                        as business_email_normalized,
    min(h.department)                                       as department
from truncated_email_keys k
inner join {{ ref('int_hr_roster') }} h
    on h.email_rn = 1
   and h.business_email_normalized like k.email_prefix || '%'
group by k.email_prefix
having count(*) = 1
