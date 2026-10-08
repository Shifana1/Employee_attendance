-- Fails if any row falls on Saturday (6) or Sunday (7).
select
    "EmployeeID",
    "Date"
from {{ ref('employee_hr_attendance_status') }}
where day_of_week(date_parse("Date", '%Y-%m-%d')) not between 1 and 5
