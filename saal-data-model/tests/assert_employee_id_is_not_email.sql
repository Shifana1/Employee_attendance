-- Fails if EmployeeID stores an email instead of the HR / punch employee id.
select "EmployeeID"
from {{ ref('employee_hr_attendance_status') }}
where strpos(cast("EmployeeID" as varchar), '@') > 0

union all

select "EmployeeID"
from {{ ref('employee_attendance_status') }}
where strpos(cast("EmployeeID" as varchar), '@') > 0

union all

select "EmployeeID"
from {{ ref('employee_attendance_report') }}
where strpos(cast("EmployeeID" as varchar), '@') > 0
