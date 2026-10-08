{{
    config(
        materialized='table',
        database='dataproducts',
        schema='employee',
        on_table_exists='drop'
    )
}}

/*
    Previous-month attendance table.

    Replaces the leftover Spark table dataproducts.employee.employee_attendance.
    Same rows as employee_attendance_report for the previous calendar month.
*/

select * from {{ ref('employee_attendance_report') }}
