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
    Monthly snapshot of current int_hr_roster (no Spark HR history).
*/

select
    *,
    {{ history_period_columns() }}
from {{ ref('int_hr_roster') }}
