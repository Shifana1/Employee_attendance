{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for onsite (ZKTeco) daily attendance.

    Name, email, date, clock_in, and clock_out are kept from raw.
    emp_no is not a real employee id and is ignored.
    employee_id is matched from HR on exact email when present.
    Rows without an HR match are still kept.

    Weekend EmailN blanks are filled from the same person's weekday email.
    Truncated EmailN is rematched to HR in int_onsite_day / int_hr_by_prefix.

    clock_in  → first_punch_in
    clock_out → last_punch_in

    Raw Iceberg may store clocks as timestamp (Spark) or varchar ("09:15").
    Native timestamps are used directly. Stringify-and-parse is fallback only;
    varchar forms like "2026-08-03 06:59:00.0" / "... UTC" used to null punches.
*/

with source as (

    select * from {{ source('employee_attendance', 'onsite_employee') }}

),

source_clean as (

    select
        trim(name)                                          as employee_name,
        {{ normalized_person_name('name') }}                as name_key,
        {{ usable_source_email('email') }}                  as row_email,
        max({{ usable_source_email('email') }}) over (
            partition by {{ normalized_person_name('name') }}
        )                                                   as filled_email,
        coalesce(
            try_cast(date as date),
            try(date_parse(trim(cast(date as varchar)), '%Y-%m-%d')),
            try(date_parse(trim(cast(date as varchar)), '%m/%d/%Y')),
            try(date_parse(trim(cast(date as varchar)), '%d-%b-%Y')),
            try(date_parse(trim(cast(date as varchar)), '%d-%b-%y')),
            try(date_parse(trim(cast(date as varchar)), '%d/%m/%Y')),
            cast(try_cast(date as timestamp) as date)
        )                                                   as attendance_date,
        clock_in,
        clock_out
    from source
    where {{ usable_source_email('email') }} is not null
       or (
            name is not null
        and trim(name) <> ''
        and lower(trim(name)) not in ('null', 'nan', 'none', 'nat')
        and not {{ is_pickle_leak_sql('name') }}
       )

),

hr as (

    select
        employee_id,
        business_email_normalized,
        business_email
    from (
        select
            employee_id,
            business_email_normalized,
            business_email,
            row_number() over (
                partition by business_email_normalized
                order by employee_id
            )                                               as rn
        from {{ ref('stg_employee_hr') }}
        where business_email_normalized is not null
    ) h
    where rn = 1

)

select
    hr.employee_id,
    src.employee_name,
    coalesce(src.row_email, src.filled_email)               as email_normalized,
    coalesce(src.row_email, src.filled_email)               as email,
    src.attendance_date,
    src.clock_in,
    src.clock_out,
    {{ parse_onsite_punch('src.clock_in', 'src.attendance_date') }}
                                                            as first_punch_in,
    {{ parse_onsite_punch('src.clock_out', 'src.attendance_date') }}
                                                            as last_punch_in
from source_clean src
left join hr
    on coalesce(src.row_email, src.filled_email) = hr.business_email_normalized
where src.attendance_date is not null
