"""Tests for dbt history period macros and history model wiring."""

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MACRO = ROOT / "macros" / "history_reporting_period.sql"
SOURCES = ROOT / "models" / "staging" / "sources.yml"
STAGING = ROOT / "models" / "staging" / "employee"
INTERMEDIATE = ROOT / "models" / "intermediate"
SEMANTIC = ROOT / "models" / "semantic"


class HistoryPeriodMacroTests(unittest.TestCase):
    def test_macro_defines_year_month_and_delete_hook(self) -> None:
        text = MACRO.read_text(encoding="utf-8")
        self.assertIn("macro history_reporting_year", text)
        self.assertIn("macro history_reporting_month", text)
        self.assertIn("macro history_period_delete_hook", text)
        self.assertIn("macro history_period_columns", text)
        self.assertIn("macro history_period_filter", text)
        self.assertIn("attendance_year", text)
        self.assertIn("attendance_month", text)
        self.assertIn("delete from {{ this }}", text)
        self.assertIn("reporting_year", text)
        self.assertIn("reporting_month", text)

    def test_history_models_are_incremental_append_with_delete_hook(self) -> None:
        expected = [
            STAGING / "stg_onsite_employee_history.sql",
            STAGING / "stg_leave_data_history.sql",
            STAGING / "stg_attendance_event_history.sql",
            STAGING / "stg_daily_attendance_history.sql",
            INTERMEDIATE / "int_hr_roster_history.sql",
            INTERMEDIATE / "int_hr_by_prefix_history.sql",
            INTERMEDIATE / "int_office_day_history.sql",
            INTERMEDIATE / "int_onsite_day_history.sql",
            INTERMEDIATE / "int_leave_day_history.sql",
            INTERMEDIATE / "int_attendance_day_history.sql",
            SEMANTIC / "employee_attendance_report_history.sql",
            SEMANTIC / "employee_attendance_history.sql",
            SEMANTIC / "employee_attendance_status_history.sql",
            SEMANTIC / "employee_hr_attendance_status_history.sql",
            SEMANTIC / "employee_departments_history.sql",
        ]
        for path in expected:
            self.assertTrue(path.is_file(), "missing %s" % path)
            text = path.read_text(encoding="utf-8")
            self.assertIn("materialized='incremental'", text, path.name)
            self.assertIn("incremental_strategy='append'", text, path.name)
            self.assertIn("history_period_delete_hook", text, path.name)
            self.assertIn("history_period_columns", text, path.name)
            self.assertIn("on_table_exists='skip'", text, path.name)
            self.assertIn("full_refresh=false", text, path.name)
            self.assertNotIn("on_table_exists='drop'", text, path.name)

    def test_no_stg_employee_hr_history(self) -> None:
        self.assertFalse((STAGING / "stg_employee_hr_history.sql").exists())

    def test_unused_raw_employee_attendance_source_removed(self) -> None:
        tables = SOURCES.read_text(encoding="utf-8").split("tables:", 1)[1]
        self.assertNotRegex(tables, r"(?m)^      - name: employee_attendance$")
        self.assertIn("- name: employee_attendance_metrics\n", tables)
        self.assertIn("- name: leave_data_history\n", tables)

    def test_staging_history_reads_spark_history_sources(self) -> None:
        leave = (STAGING / "stg_leave_data_history.sql").read_text(encoding="utf-8")
        onsite = (STAGING / "stg_onsite_employee_history.sql").read_text(
            encoding="utf-8"
        )
        events = (STAGING / "stg_attendance_event_history.sql").read_text(
            encoding="utf-8"
        )
        daily = (STAGING / "stg_daily_attendance_history.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("leave_data_history", leave)
        self.assertNotIn("ref('stg_leave_data')", leave)
        self.assertIn("onsite_employee_history", onsite)
        self.assertNotIn("ref('stg_onsite_employee')", onsite)
        self.assertIn("employee_attendance_metrics_history", events)
        self.assertNotIn("ref('stg_attendance_event')", events)
        self.assertIn("ref('stg_attendance_event_history')", daily)
        self.assertNotIn("ref('stg_daily_attendance')", daily)

    def test_downstream_history_reads_history_models(self) -> None:
        office = (INTERMEDIATE / "int_office_day_history.sql").read_text(
            encoding="utf-8"
        )
        onsite = (INTERMEDIATE / "int_onsite_day_history.sql").read_text(
            encoding="utf-8"
        )
        leave = (INTERMEDIATE / "int_leave_day_history.sql").read_text(encoding="utf-8")
        merged = (INTERMEDIATE / "int_attendance_day_history.sql").read_text(
            encoding="utf-8"
        )
        prefix = (INTERMEDIATE / "int_hr_by_prefix_history.sql").read_text(
            encoding="utf-8"
        )
        report = (SEMANTIC / "employee_attendance_report_history.sql").read_text(
            encoding="utf-8"
        )
        attendance = (SEMANTIC / "employee_attendance_history.sql").read_text(
            encoding="utf-8"
        )
        status = (SEMANTIC / "employee_attendance_status_history.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("ref('stg_daily_attendance_history')", office)
        self.assertIn("ref('stg_onsite_employee_history')", onsite)
        self.assertIn("ref('stg_leave_data_history')", leave)
        self.assertIn("ref('stg_leave_data_history')", prefix)
        self.assertIn("ref('int_office_day_history')", merged)
        self.assertIn("ref('int_onsite_day_history')", merged)
        self.assertIn("ref('int_leave_day_history')", merged)
        self.assertNotIn("ref('int_attendance_day')", merged)
        self.assertIn("ref('int_attendance_day_history')", report)
        self.assertNotIn("ref('employee_attendance_report')", report)
        self.assertIn("ref('employee_attendance_report_history')", attendance)
        self.assertNotIn("ref('employee_attendance')", attendance)
        self.assertIn("ref('int_attendance_day_history')", status)


if __name__ == "__main__":
    unittest.main()
