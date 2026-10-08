-- Fails if any stg_daily_attendance emp_code + date is missing from
-- int_attendance_day. Matched people may merge on SAAL email (punch window
-- contained in the merged first/last). Unmatched office days appear with
-- employee_id = last_name (HR emp_id) or emp_code and the same attendance_date.
select
    d.emp_code,
    d.attendance_date
from {{ ref('stg_daily_attendance') }} d
where not exists (
    select 1
    from {{ ref('int_attendance_day') }} i
    where i.attendance_date = d.attendance_date
      and (
            i.employee_id = d.emp_code
         or i.employee_id = d.last_name
         or (
                i.first_punch_ts <= d.checkin_timestamp
            and i.last_punch_ts >= coalesce(d.checkout_timestamp, d.checkin_timestamp)
         )
      )
)
