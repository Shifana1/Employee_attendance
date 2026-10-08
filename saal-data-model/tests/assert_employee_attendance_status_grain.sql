-- Fails if the attendance status table has more than one row per person + date.
-- Person key is SAAL email when present, else EmployeeID.
select
    coalesce("BusinessEmail", "EmployeeID") as person_key,
    "Date",
    count(*) as row_count
from {{ ref('employee_attendance_status') }}
group by
    coalesce("BusinessEmail", "EmployeeID"),
    "Date"
having count(*) > 1
