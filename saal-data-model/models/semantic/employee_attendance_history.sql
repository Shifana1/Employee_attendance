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
    Same columns as employee_attendance from the history report (not a
    snapshot of the overwritten current table).
*/

select
    "EmployeeID",
    "EmployeeName",
    "BusinessEmail",
    "Date",
    "FirstPunchIn",
    "LastPunchOut",
    "TotalHoursInOffice",
    "SaalOfficeOrOnsite",
    "IsLeave",
    {{ history_period_columns() }}
from {{ ref('employee_attendance_report_history') }}
where {{ history_period_filter() }}
