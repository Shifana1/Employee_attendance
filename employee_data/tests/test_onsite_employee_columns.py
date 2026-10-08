import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dependencies" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from onsite_employee import (  # noqa: E402
    DEFAULT_ONSITE_EXCEL_PATH,
    DEFAULT_ONSITE_TABLE,
    ONSITE_COLUMN_ORDER,
    ONSITE_COLUMN_RENAME_MAP,
    current_landing_period,
    iceberg_column_ddl,
    iceberg_create_namespace_sql,
    iceberg_create_table_sql,
    iceberg_drop_table_sql,
    iceberg_namespace,
    iceberg_schema_location,
    iceberg_table_location,
    is_pickle_leak,
    normalize_onsite_columns,
    onsite_spark_rows,
    resolve_monthly_excel_path,
    should_replace_iceberg_schema,
    assert_onsite_clocks_landed,
    clock_text_is_time,
)

SAMPLE_HEADERS = [
    "State",
    "Emp No.",
    "AC-No.",
    "No.",
    "Name",
    "Auto-Assign",
    "Date",
    "Timetable",
    "On duty",
    "Off duty",
    "Clock In",
    "Machines Name",
    "Clock Out",
    "Machines Name.1",
    "Normal",
    "Real time",
    "Late",
    "Early",
    "Absent",
    "OT Time",
    "Work Time",
    "Exception",
    "Must C/In",
    "Must C/Out",
    "Department",
    "NDays",
    "WeekEnd",
    "Holiday",
    "ATT_Time",
    "NDays_OT",
    "WeekEnd_OT",
    "Holiday_OT",
    "EmailN",
]


def test_normalize_onsite_columns_matches_sample_headers() -> None:
    assert normalize_onsite_columns(SAMPLE_HEADERS) == ONSITE_COLUMN_ORDER


def test_rename_map_covers_ordered_columns() -> None:
    assert list(ONSITE_COLUMN_RENAME_MAP.values()) == ONSITE_COLUMN_ORDER
    assert len(ONSITE_COLUMN_RENAME_MAP) == len(ONSITE_COLUMN_ORDER)


def test_unknown_headers_are_preserved() -> None:
    assert normalize_onsite_columns(["Emp No.", "Extra"]) == ["emp_no", "Extra"]


def test_iceberg_namespace_from_fully_qualified_table() -> None:
    assert iceberg_namespace(DEFAULT_ONSITE_TABLE) == "raw.employee"
    assert iceberg_schema_location(DEFAULT_ONSITE_TABLE) == "s3a://raw/employee"
    assert iceberg_table_location(DEFAULT_ONSITE_TABLE) == (
        "s3a://raw/employee/onsite_employee"
    )


def test_iceberg_namespace_rejects_bare_table_name() -> None:
    try:
        iceberg_namespace("onsite_employee")
    except ValueError as exc:
        assert "catalog.schema.table" in str(exc)
    else:
        raise AssertionError("expected ValueError for a table without a namespace")


def test_overwrite_replaces_iceberg_schema_when_clock_types_differ() -> None:
    string_clock = [("clock_in", "string"), ("clock_out", "string")]
    timestamp_clock = [("clock_in", "timestamp"), ("clock_out", "timestamp")]
    assert should_replace_iceberg_schema("overwrite", string_clock, timestamp_clock) is True
    assert should_replace_iceberg_schema("append", string_clock, timestamp_clock) is True
    assert should_replace_iceberg_schema("append", string_clock, string_clock) is False


def test_create_table_sql_is_idempotent_iceberg_ddl() -> None:
    table = DEFAULT_ONSITE_TABLE
    column_ddl = iceberg_column_ddl(
        [("emp_no", "string"), ("clock_in", "string")]
    )
    sql = iceberg_create_table_sql(table, column_ddl)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS %s" % table)
    assert "`emp_no` string" in sql
    assert "`clock_in` string" in sql
    assert "USING iceberg" in sql
    assert "LOCATION 's3a://raw/employee/onsite_employee'" in sql
    assert iceberg_create_namespace_sql(table) == (
        "CREATE NAMESPACE IF NOT EXISTS raw.employee LOCATION 's3a://raw/employee'"
    )
    assert iceberg_drop_table_sql(table) == (
        "DROP TABLE IF EXISTS raw.employee.onsite_employee"
    )
    script = (SCRIPTS_DIR / "onsite_employee.py").read_text(encoding="utf-8")
    assert "Dropping Iceberg table so clock columns stay string" in script


def test_default_onsite_excel_path_uses_lakehouse_bucket() -> None:
    assert DEFAULT_ONSITE_EXCEL_PATH == "s3a://lakehouse/saal_employee/onsite_employee/"
    assert DEFAULT_ONSITE_TABLE == "raw.employee.onsite_employee"
    script = (SCRIPTS_DIR / "onsite_employee.py").read_text(encoding="utf-8")
    assert "attach_hr_employee_id_by_email" not in script
    assert "load_hr_employee_id_by_email" not in script
    from datetime import datetime

    year, month = current_landing_period()
    now = datetime.now()
    if now.month == 1:
        assert (year, month) == (now.year - 1, 12)
    else:
        assert (year, month) == (now.year, now.month - 1)


def test_stringify_keeps_source_clock_and_email_strings() -> None:
    import pandas as pd

    from onsite_employee import _stringify_frame

    frame = _stringify_frame(
        pd.DataFrame(
            {
                "email": ["Mahesh@saal.ai"],
                "clock_in": ["09:15"],
                "clock_out": ["-"],
            }
        )
    )
    assert frame["email"].iloc[0] == "Mahesh@saal.ai"
    assert frame["clock_in"].iloc[0] == "09:15"
    assert frame["clock_out"].iloc[0] == "-"


def test_stringify_drops_pandas_na_and_pickle_leaks() -> None:
    from datetime import time

    import pandas as pd

    from onsite_employee import _stringify_cell, _stringify_frame

    assert _stringify_cell(pd.NA, "email") is None
    assert (
        _stringify_cell(
            "net.razorvine.pickle.objects.ClassDictConstructor@1910338d",
            "email",
        )
        is None
    )
    assert _stringify_cell(time(9, 15), "clock_in") == "09:15:00"
    assert _stringify_cell(0.3854166666666667, "clock_in") == "09:15:00"
    assert _stringify_cell("08/03/2026", "date") == "2026-08-03"
    assert _stringify_cell("08/13/2026", "date") == "2026-08-13"
    assert _stringify_cell("25/08/2026", "date") == "2026-08-25"

    frame = _stringify_frame(
        pd.DataFrame(
            {
                "email": [pd.NA, "noura@saal.ai"],
                "clock_in": [time(8, 30), "09:15"],
            }
        )
    )
    assert frame["email"].tolist() == [None, "noura@saal.ai"]
    assert frame["clock_in"].tolist() == ["08:30:00", "09:15"]
    assert all(value is None or type(value) is str for value in frame["email"].tolist())


def test_pickle_leak_detector() -> None:
    assert is_pickle_leak(
        "net.razorvine.pickle.objects.ClassDictConstructor@1910338d"
    )
    assert not is_pickle_leak("noura@saal.ai")


def test_onsite_spark_rows_are_plain_python_strings() -> None:
    import pandas as pd

    frame = pd.DataFrame({column: [None] for column in ONSITE_COLUMN_ORDER})
    frame["email"] = pd.array(["noura@saal.ai"], dtype="string")
    frame["clock_in"] = pd.array(["09:15"], dtype="string")
    rows = onsite_spark_rows(frame)
    assert len(rows) == 1
    email_index = ONSITE_COLUMN_ORDER.index("email")
    clock_index = ONSITE_COLUMN_ORDER.index("clock_in")
    assert rows[0][email_index] == "noura@saal.ai"
    assert rows[0][clock_index] == "09:15"
    assert all(value is None or type(value) is str for value in rows[0])


def test_assert_onsite_clocks_landed_rejects_pickle_only_rows() -> None:
    clock_index = ONSITE_COLUMN_ORDER.index("clock_in")
    empty = [None] * len(ONSITE_COLUMN_ORDER)
    pickle_row = list(empty)
    pickle_row[clock_index] = (
        "net.razorvine.pickle.objects.ClassDictConstructor@1910338d"
    )
    try:
        assert_onsite_clocks_landed([tuple(pickle_row)])
    except ValueError as exc:
        assert "0 parseable clock_in" in str(exc)
    else:
        raise AssertionError("expected ValueError")
    good = list(empty)
    good[clock_index] = "06:59"
    assert clock_text_is_time("06:59") is True
    assert assert_onsite_clocks_landed([tuple(good)]) == 1


def test_resolve_monthly_excel_path_uses_explicit_month_folder() -> None:
    assert (
        resolve_monthly_excel_path(
            "s3a://lakehouse/saal_employee/onsite_employee/2026/july/"
        )
        == "s3a://lakehouse/saal_employee/onsite_employee/2026/july/"
    )


def test_resolve_monthly_excel_path_uses_explicit_period() -> None:
    path = resolve_monthly_excel_path(
        "s3a://lakehouse/saal_employee/onsite_employee/",
        year=2026,
        month=8,
    )
    assert path == "s3a://lakehouse/saal_employee/onsite_employee/2026/august/"


def test_resolve_monthly_excel_path_keeps_single_workbook() -> None:
    assert (
        resolve_monthly_excel_path(
            "s3a://lakehouse/saal_employee/onsite_employee/2026/july/export.xlsx"
        )
        == "s3a://lakehouse/saal_employee/onsite_employee/2026/july/export.xlsx"
    )
