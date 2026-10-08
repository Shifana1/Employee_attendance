import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

FLOWS_DIR = Path(__file__).resolve().parents[1] / "flows"
sys.path.insert(0, str(FLOWS_DIR))

from datetime import datetime
from zoneinfo import ZoneInfo

from saal_model import (  # noqa: E402
    _attendance_dbt_vars,
    _attendance_period_from_airflow_context,
    _current_attendance_period,
    build_invoke_argv,
    dbt_cli_args,
    dbt_log_dir,
    dbt_target_dir,
    dbt_work_dir,
    format_dbt_failure,
    prepare_dbt_project_dir,
    resolve_dbt_argv,
    resolve_dbt_bin,
    resolve_dbt_python,
    tail_dbt_log_files,
    tail_text,
)


class DbtRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_attendance_vars_use_previous_month(self) -> None:
        class _FakeDate:
            def __init__(self, year, month, day):
                self.year = year
                self.month = month
                self.day = day

            def in_timezone(self, _tz):
                return self

            def replace(self, day=1):
                return _FakeDate(self.year, self.month, day)

            def subtract(self, months=0):
                month = self.month - months
                year = self.year
                while month < 1:
                    month += 12
                    year -= 1
                return _FakeDate(year, month, self.day)

        class _Run:
            run_type = "manual"

        context = {
            "logical_date": _FakeDate(2026, 9, 1),
            "data_interval_end": _FakeDate(2026, 8, 2),
            "dag_run": _Run(),
        }
        self.assertEqual(_attendance_period_from_airflow_context(context), (2026, 8))
        for key in ("SAAL_REPORTING_YEAR", "SAAL_REPORTING_MONTH"):
            os.environ.pop(key, None)
        self.assertEqual(
            _attendance_dbt_vars({}, context=context),
            '{"attendance_year": 2026, "attendance_month": 8}',
        )
        expected_year, expected_month = _current_attendance_period()
        dubai = ZoneInfo("Asia/Dubai")
        dubai_now = datetime.now(dubai)
        if dubai_now.month == 1:
            self.assertEqual(
                (expected_year, expected_month), (dubai_now.year - 1, 12)
            )
        else:
            self.assertEqual(
                (expected_year, expected_month),
                (dubai_now.year, dubai_now.month - 1),
            )
        self.assertEqual(
            _attendance_dbt_vars({}),
            '{"attendance_year": %s, "attendance_month": %s}'
            % (expected_year, expected_month),
        )

    def test_dbt_cli_args_strips_leading_dbt(self) -> None:
        args = dbt_cli_args(
            "dbt run --select path:models/staging/employee",
            "/opt/airflow/dags/saal-data-model",
            "prod",
        )
        self.assertEqual(
            args[:4],
            ["run", "--select", "path:models/staging/employee", "--project-dir"],
        )
        self.assertNotIn("--vars", args)
        self.assertNotIn("--no-quiet", args)
        self.assertNotIn("--log-path", args)

        with_vars = dbt_cli_args(
            "dbt run --select path:models/staging/employee",
            "/opt/airflow/dags/saal-data-model",
            "prod",
            "{enable_d365_sources: false}",
        )
        self.assertEqual(
            with_vars[with_vars.index("--vars") + 1], "{enable_d365_sources: false}"
        )

        logged = dbt_cli_args(
            "dbt run --select path:models/staging/employee",
            "/opt/airflow/dags/saal-data-model",
            "prod",
            log_dir="/tmp/dbt-logs",
            target_dir="/tmp/dbt-target",
        )
        self.assertEqual(logged[logged.index("--log-path") + 1], "/tmp/dbt-logs")
        self.assertEqual(logged[logged.index("--target-path") + 1], "/tmp/dbt-target")
        self.assertNotIn("--vars", logged)

    def test_resolve_dbt_bin_prefers_airflow_venv(self) -> None:
        venv_bin = self.tmp_path / "dbt_venv" / "bin"
        venv_bin.mkdir(parents=True)
        dbt_path = venv_bin / "dbt"
        dbt_path.write_text("#!/bin/sh\n", encoding="utf-8")
        dbt_path.chmod(dbt_path.stat().st_mode | stat.S_IEXEC)
        found = resolve_dbt_bin({"AIRFLOW_HOME": str(self.tmp_path), "PATH": "/usr/bin"})
        self.assertEqual(found, str(dbt_path))
        python_path = venv_bin / "python"
        python_path.write_text("#!/bin/sh\n", encoding="utf-8")
        python_path.chmod(python_path.stat().st_mode | stat.S_IEXEC)
        self.assertEqual(
            resolve_dbt_python({"AIRFLOW_HOME": str(self.tmp_path), "PATH": "/usr/bin"}),
            str(python_path),
        )
        invoke = build_invoke_argv(str(python_path), ["run", "--select", "stg_x"])
        self.assertEqual(invoke[0], str(python_path))
        self.assertEqual(invoke[1], "-c")
        self.assertEqual(invoke[-3:], ["run", "--select", "stg_x"])

    def test_resolve_dbt_bin_raises_when_missing(self) -> None:
        with self.assertRaises(FileNotFoundError) as caught:
            resolve_dbt_bin(
                {"AIRFLOW_HOME": str(self.tmp_path), "PATH": str(self.tmp_path)}
            )
        self.assertIn("dbt executable not found", str(caught.exception))

    def test_prepare_dbt_project_dir_removes_package_artifacts(self) -> None:
        packages = self.tmp_path / "dbt_packages"
        packages.mkdir()
        (packages / "leftover.txt").write_text("x", encoding="utf-8")
        (self.tmp_path / "packages.yml").write_text("packages: []\n", encoding="utf-8")
        (self.tmp_path / "package-lock.yml").write_text("lock\n", encoding="utf-8")
        prepare_dbt_project_dir(str(self.tmp_path))
        self.assertFalse(packages.exists())
        self.assertFalse((self.tmp_path / "packages.yml").exists())
        self.assertFalse((self.tmp_path / "package-lock.yml").exists())

    def test_format_dbt_failure_includes_log_tail(self) -> None:
        log_dir = self.tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "dbt.log").write_text(
            "line-a\nline-b\ncompile error\n", encoding="utf-8"
        )
        message = format_dbt_failure(2, "", str(self.tmp_path), ["dbt", "run"])
        self.assertIn("exit code 2", message)
        self.assertIn("(no dbt stdout/stderr)", message)
        self.assertIn("compile error", message)
        extra = self.tmp_path / "target" / "dbt_airflow_logs"
        extra.mkdir(parents=True)
        (extra / "dbt.log").write_text("forced log path error\n", encoding="utf-8")
        extra_message = format_dbt_failure(
            2, "", str(self.tmp_path), ["dbt", "run"], extra_log_dirs=[str(extra)]
        )
        self.assertIn("forced log path error", extra_message)

    def test_dbt_work_dir_is_under_airflow_home(self) -> None:
        work = dbt_work_dir({"AIRFLOW_HOME": str(self.tmp_path)})
        self.assertEqual(work, str(self.tmp_path / "tmp" / "saal-data-model-dbt"))
        self.assertTrue(Path(work).is_dir())
        logs = dbt_log_dir(work)
        target = dbt_target_dir(work)
        self.assertTrue(logs.endswith("logs"))
        self.assertTrue(target.endswith("target"))
        self.assertTrue(Path(logs).is_dir())
        self.assertTrue(Path(target).is_dir())

    def test_tail_text_empty(self) -> None:
        self.assertEqual(tail_text("  \n"), "")

    def test_tail_dbt_log_files_missing(self) -> None:
        self.assertEqual(tail_dbt_log_files(str(self.tmp_path)), "")


if __name__ == "__main__":
    unittest.main()
