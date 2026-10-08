-- Fails if the HR attendance status table has more than one row per employee + date.
select
    "EmployeeID",
    "Date",
    count(*) as row_count
from {{ ref('employee_hr_attendance_status') }}
group by
    "EmployeeID",
    "Date"
having count(*) > 1
