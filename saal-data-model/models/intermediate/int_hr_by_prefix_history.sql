{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_table_exists='skip',
        full_refresh=false,
        schema='employee_model',
        pre_hook="{{ history_period_delete_hook() }}"
    )
}}

/*
    Same as int_hr_by_prefix.sql using truncated emails from leave history
    and current-month HR roster snapshot.
*/

with truncated_email_keys as (

    select email_normalized                                 as email_prefix
    from {{ ref('stg_leave_data_history') }}
    where {{ is_truncated_email('email_normalized') }}
      and {{ history_period_filter() }}

)

select
    k.email_prefix,
    min(h.employee_id)                                      as employee_id,
    min(h.employee_name)                                    as employee_name,
    min(h.business_email)                                   as business_email,
    min(h.business_email_normalized)                        as business_email_normalized,
    min(h.department)                                       as department,
    {{ history_period_columns() }}
from truncated_email_keys k
inner join {{ ref('int_hr_roster_history') }} h
    on h.email_rn = 1
   and {{ history_period_filter('h') }}
   and h.business_email_normalized like k.email_prefix || '%'
group by k.email_prefix
having count(*) = 1
