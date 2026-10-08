import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dependencies" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from leave_data import (  # noqa: E402
    DEFAULT_LEAVE_EXCEL_PATH,
    DEFAULT_LEAVE_TABLE,
    LEAVE_COLUMN_ORDER,
    LEAVE_COLUMN_RENAME_MAP,
    current_landing_period,
    find_leave_header_row_index,
    iceberg_column_ddl,
    iceberg_create_namespace_sql,
    iceberg_create_table_sql,
    iceberg_namespace,
    iceberg_schema_location,
    iceberg_table_location,
    is_leave_data_row,
    normalize_leave_columns,
    resolve_monthly_excel_path,
    strip_leave_header,
)

SAMPLE_HEADERS = [
    "Employee Id",
    "Employee Name",
    "Department\t",
    "Email ID",
    "Leave From Date",
    "Leave To Date",
    "Working Days/Hours",
    "Duration Unit",
    "Leave Type",
    "Status",
]


def test_normalize_leave_columns_matches_sample_headers() -> None:
    assert normalize_leave_columns(SAMPLE_HEADERS) == LEAVE_COLUMN_ORDER


def test_rename_map_covers_ordered_columns() -> None:
    assert list(LEAVE_COLUMN_RENAME_MAP.values()) == LEAVE_COLUMN_ORDER
    assert len(LEAVE_COLUMN_RENAME_MAP) == len(LEAVE_COLUMN_ORDER)


def test_unknown_headers_are_preserved() -> None:
    assert normalize_leave_columns(["Employee Id", "Extra"]) == ["employee_id", "Extra"]


def test_strip_leave_header_removes_tabs() -> None:
    assert strip_leave_header("Department\t") == "Department"


def test_find_leave_header_row_skips_title_rows() -> None:
    rows = [
        ["Technova", None],
        ["Leave Details", "1 June to 31 July 2026"],
        [None, None],
        ["Employee Id", "Employee Name"],
        ["190", "Vardhman Golchha"],
    ]
    assert find_leave_header_row_index(rows) == 3


def test_is_leave_data_row_drops_totals_footer() -> None:
    assert is_leave_data_row("190") is True
    assert is_leave_data_row(None) is False
    assert is_leave_data_row("nan") is False
    assert is_leave_data_row("") is False


def test_parse_leave_workbook_skips_title_and_totals() -> None:
    import pandas as pd

    from leave_data import parse_leave_workbook

    pdf_raw = pd.DataFrame(
        [
            ["Technova", None, None, None, None, None, None, None, None, None],
            ["Leave Details", "1 June to 31 July 2026", None, None, None, None, None, None, None, None],
            [None, None, None, None, None, None, None, None, None, None],
            SAMPLE_HEADERS,
            [
                "190",
                "Vardhman Golchha",
                "Engineering",
                "vardhman@saal.ai",
                "2026-07-31 00:00:00",
                "2026-08-14 00:00:00",
                "1",
                "days",
                "Annual Leave",
                "Approved",
            ],
            [None, None, None, None, None, None, "703", None, None, None],
        ]
    )
    parsed = parse_leave_workbook(pdf_raw)
    assert list(parsed.columns) == LEAVE_COLUMN_ORDER
    assert len(parsed) == 1
    assert parsed.iloc[0]["employee_id"] == "190"
    assert parsed.iloc[0]["department"] == "Engineering"


def test_iceberg_namespace_from_fully_qualified_table() -> None:
    assert iceberg_namespace(DEFAULT_LEAVE_TABLE) == "raw.employee"
    assert iceberg_schema_location(DEFAULT_LEAVE_TABLE) == "s3a://raw/employee"
    assert iceberg_table_location(DEFAULT_LEAVE_TABLE) == "s3a://raw/employee/leave_data"


def test_iceberg_namespace_rejects_bare_table_name() -> None:
    try:
        iceberg_namespace("leave_data")
    except ValueError as exc:
        assert "catalog.schema.table" in str(exc)
    else:
        raise AssertionError("expected ValueError for a table without a namespace")


def test_create_table_sql_is_idempotent_iceberg_ddl() -> None:
    table = DEFAULT_LEAVE_TABLE
    column_ddl = iceberg_column_ddl(
        [("employee_id", "string"), ("leave_type", "string")]
    )
    sql = iceberg_create_table_sql(table, column_ddl)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS %s" % table)
    assert "`employee_id` string" in sql
    assert "`leave_type` string" in sql
    assert "USING iceberg" in sql
    assert "LOCATION 's3a://raw/employee/leave_data'" in sql
    namespace_sql = iceberg_create_namespace_sql(table)
    assert namespace_sql == (
        "CREATE NAMESPACE IF NOT EXISTS raw.employee LOCATION 's3a://raw/employee'"
    )


def test_default_leave_excel_path_uses_lakehouse_bucket() -> None:
    assert DEFAULT_LEAVE_EXCEL_PATH == "s3a://lakehouse/saal_employee/leave_employee/"


def test_resolve_monthly_excel_path_defaults_to_previous_month() -> None:
    from datetime import datetime

    year, month = current_landing_period()
    now = datetime.now()
    if now.month == 1:
        assert (year, month) == (now.year - 1, 12)
    else:
        assert (year, month) == (now.year, now.month - 1)
    path = resolve_monthly_excel_path(DEFAULT_LEAVE_EXCEL_PATH)
    assert path.endswith("/%s/" % (
        ["january", "february", "march", "april", "may", "june",
         "july", "august", "september", "october", "november", "december"][month - 1]
    ))
    assert "/%s/" % year in path


def test_resolve_monthly_excel_path_uses_explicit_month_folder() -> None:
    assert (
        resolve_monthly_excel_path(
            "s3a://lakehouse/saal_employee/leave_employee/2026/july/"
        )
        == "s3a://lakehouse/saal_employee/leave_employee/2026/july/"
    )


def test_resolve_monthly_excel_path_uses_explicit_period() -> None:
    path = resolve_monthly_excel_path(
        "s3a://lakehouse/saal_employee/leave_employee/",
        year=2026,
        month=8,
    )
    assert path == "s3a://lakehouse/saal_employee/leave_employee/2026/august/"


def test_resolve_monthly_excel_path_keeps_single_workbook() -> None:
    assert (
        resolve_monthly_excel_path(
            "s3a://lakehouse/saal_employee/leave_employee/2026/july/report.xlsx"
        )
        == "s3a://lakehouse/saal_employee/leave_employee/2026/july/report.xlsx"
    )
