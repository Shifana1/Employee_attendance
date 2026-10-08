import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dependencies" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from employee_hr_data import (  # noqa: E402
    DEFAULT_HR_TABLE,
    EMPLOYEE_HR_COLUMNS,
    iceberg_column_ddl,
    iceberg_create_namespace_sql,
    iceberg_create_table_sql,
    iceberg_namespace,
    iceberg_schema_location,
    iceberg_table_location,
    parse_hr_csv_lines,
)


HR_CSV_PATH = (
    Path(__file__).resolve().parents[1]
    / "dependencies"
    / "resources"
    / "employee_hr_master - employee_master.csv"
)


def test_iceberg_namespace_from_fully_qualified_table() -> None:
    assert iceberg_namespace(DEFAULT_HR_TABLE) == "raw.employee"
    assert iceberg_schema_location(DEFAULT_HR_TABLE) == "s3a://raw/employee"
    assert iceberg_table_location(DEFAULT_HR_TABLE) == (
        "s3a://raw/employee/employee_hr_data"
    )


def test_iceberg_namespace_rejects_bare_table_name() -> None:
    try:
        iceberg_namespace("employee_hr_data")
    except ValueError as exc:
        assert "catalog.schema.table" in str(exc)
    else:
        raise AssertionError("expected ValueError for a table without a namespace")


def test_create_table_sql_is_idempotent_iceberg_ddl() -> None:
    column_ddl = iceberg_column_ddl(
        [("employee_id", "string"), ("employee_name", "string")]
    )
    sql = iceberg_create_table_sql(DEFAULT_HR_TABLE, column_ddl)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS %s" % DEFAULT_HR_TABLE)
    assert "`employee_id` string" in sql
    assert "`employee_name` string" in sql
    assert "USING iceberg" in sql
    assert "LOCATION 's3a://raw/employee/employee_hr_data'" in sql
    assert iceberg_create_namespace_sql(DEFAULT_HR_TABLE) == (
        "CREATE NAMESPACE IF NOT EXISTS raw.employee LOCATION 's3a://raw/employee'"
    )


def test_employee_hr_columns_match_source_schema() -> None:
    assert EMPLOYEE_HR_COLUMNS[0] == "employee_id"
    assert "emp_code" not in EMPLOYEE_HR_COLUMNS
    assert "department_heads" not in EMPLOYEE_HR_COLUMNS
    assert len(set(EMPLOYEE_HR_COLUMNS)) == len(EMPLOYEE_HR_COLUMNS)


def test_parse_hr_csv_uses_employee_id_as_real_emp_id() -> None:
    lines = HR_CSV_PATH.read_text(encoding="utf-8").splitlines()
    records = parse_hr_csv_lines(lines)
    by_id = {row["employee_id"]: row for row in records}
    ahmed = by_id["150"]
    assert ahmed["employee_name"] == "Ahmed Albahnasawi"
    assert ahmed["business_email"] == "ahmed.albahnasawy@saal.ai"
    assert ahmed["department"] == "Artificial Intelligence"
    assert ahmed["job_role"] == "Machine Learning Engineer"
    abrar = by_id["135"]
    assert abrar["employee_name"] == "Abrar Abdul Nabi"
    assert abrar["business_email"] == "abrar@saal.ai"
    quoted = by_id["434"]
    assert quoted["department"] == "Business Development, Marketing and Sales"
    assert all(row["employee_id"] and row["employee_id"].isdigit() for row in records)
    assert len(records) >= 90
