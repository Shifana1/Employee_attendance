"""Unit tests for replace-month Iceberg history helpers."""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dependencies" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from iceberg_history import (  # noqa: E402
    REPORTING_MONTH_COL,
    REPORTING_YEAR_COL,
    default_history_table_name,
    iceberg_create_table_sql,
    iceberg_history_delete_sql,
    schema_to_history_column_ddl,
)


class _FakeField:
    def __init__(self, name, dtype):
        self.name = name
        self.dataType = type("DT", (), {"simpleString": lambda self, d=dtype: d})()


class _FakeSchema:
    def __init__(self, fields):
        self.fields = fields


def test_default_history_table_name() -> None:
    assert (
        default_history_table_name("raw.employee.onsite_employee")
        == "raw.employee.onsite_employee_history"
    )
    assert (
        default_history_table_name("raw.employee.leave_data_history")
        == "raw.employee.leave_data_history"
    )


def test_iceberg_history_delete_sql() -> None:
    sql = iceberg_history_delete_sql(
        "raw.employee.onsite_employee_history", 2026, 8
    )
    assert sql == (
        "DELETE FROM raw.employee.onsite_employee_history "
        "WHERE reporting_year = 2026 AND reporting_month = 8"
    )


def test_history_create_sql_includes_period_ints() -> None:
    schema = _FakeSchema(
        [_FakeField("email", "string"), _FakeField("clock_in", "string")]
    )
    ddl = schema_to_history_column_ddl(schema)
    assert "`email` string" in ddl
    assert "`clock_in` string" in ddl
    assert "`reporting_year` int" in ddl
    assert "`reporting_month` int" in ddl
    sql = iceberg_create_table_sql(
        "raw.employee.onsite_employee_history", ddl
    )
    assert sql.startswith(
        "CREATE TABLE IF NOT EXISTS raw.employee.onsite_employee_history"
    )
    assert "USING iceberg" in sql
    assert "LOCATION 's3a://raw/employee/onsite_employee_history'" in sql
    assert "DROP TABLE" not in sql


def test_onsite_script_dual_writes_history_never_drops() -> None:
    script = (SCRIPTS_DIR / "onsite_employee.py").read_text(encoding="utf-8")
    assert "DEFAULT_ONSITE_HISTORY_TABLE" in script
    assert "write_iceberg_month_history" in script
    assert "output_warehouse_fq_history_table" in script
    assert "Never DROP history" in script
    assert REPORTING_YEAR_COL in script
    assert REPORTING_MONTH_COL in script
    # Current table may drop; history writer must not.
    history_fn_start = script.index("def write_iceberg_month_history")
    history_fn = script[history_fn_start : history_fn_start + 1200]
    assert "DROP TABLE" not in history_fn
    assert "overwrite-schema" not in history_fn


def test_leave_and_metrics_dual_write_hr_does_not() -> None:
    leave = (SCRIPTS_DIR / "leave_data.py").read_text(encoding="utf-8")
    metrics = (SCRIPTS_DIR / "employee_data.py").read_text(encoding="utf-8")
    hr = (SCRIPTS_DIR / "employee_hr_data.py").read_text(encoding="utf-8")
    assert "DEFAULT_LEAVE_HISTORY_TABLE" in leave
    assert "write_iceberg_month_history" in leave
    assert "DEFAULT_METRICS_HISTORY_TABLE" in metrics
    assert "write_iceberg_month_history" in metrics
    assert "write_iceberg_month_history" not in hr
    assert "output_warehouse_fq_history_table" not in hr
    assert "employee_hr_data_history" not in hr


def test_airflow_configs_set_history_tables() -> None:
    config_dir = Path(__file__).resolve().parents[1] / "config"
    onsite = (config_dir / "onsite_employee.json").read_text(encoding="utf-8")
    leave = (config_dir / "leave_data.json").read_text(encoding="utf-8")
    metrics = (config_dir / "employee_data.json").read_text(encoding="utf-8")
    hr = (config_dir / "employee_hr_data.json").read_text(encoding="utf-8")
    assert "raw.employee.onsite_employee_history" in onsite
    assert "raw.employee.leave_data_history" in leave
    assert "raw.employee.employee_attendance_metrics_history" in metrics
    assert "history_table" not in hr
