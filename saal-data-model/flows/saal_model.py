# Airflow UI dag_id is saal_model. Airflow 2.10.3.
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
import re
import shutil
import subprocess
import sys
from typing import List, Mapping, Optional, Sequence

_FLOWS_DIR = os.path.dirname(os.path.abspath(__file__))
_DAG_PACKAGE_ROOT = os.path.dirname(_FLOWS_DIR)

# Runs inside dbt_venv so exceptions print even when the dbt CLI is silent.
DBT_INVOKE_PY = r"""
import sys
import traceback

def _run(argv):
    from dbt.cli.main import dbtRunner
    result = dbtRunner().invoke(argv)
    exception = getattr(result, "exception", None)
    if exception is not None:
        traceback.print_exception(type(exception), exception, exception.__traceback__)
        sys.stdout.flush()
        sys.stderr.flush()
        return 2
    if not getattr(result, "success", False):
        print("dbtRunner success=False")
        sys.stdout.flush()
        return 2
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(_run(sys.argv[1:]))
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(2)
"""


def resolve_dbt_python(env: Optional[Mapping[str, str]] = None) -> str:
    """Python that owns the dbt install (same bin dir as the dbt script)."""
    environ = env if env is not None else os.environ
    dbt_bin = resolve_dbt_bin(environ)
    sibling = os.path.join(os.path.dirname(dbt_bin), "python")
    if os.path.isfile(sibling) and os.access(sibling, os.X_OK):
        return sibling
    merged = dict(os.environ)
    merged.update(environ)
    for path in _python_candidates(merged):
        if path and os.path.exists(path):
            return path
    raise FileNotFoundError("python interpreter for dbt not found next to %s" % dbt_bin)


def build_invoke_argv(python_bin: str, dbt_args: Sequence[str]) -> List[str]:
    return [python_bin, "-c", DBT_INVOKE_PY] + list(dbt_args)


def resolve_dbt_bin(env: Optional[Mapping[str, str]] = None) -> str:
    """Return an executable dbt path or raise FileNotFoundError."""
    environ = env if env is not None else os.environ
    airflow_home = environ.get("AIRFLOW_HOME") or os.environ.get("AIRFLOW_HOME") or "/opt/airflow"
    path_value = environ.get("PATH") or os.environ.get("PATH") or ""
    candidates = [
        os.path.join(airflow_home, "dbt_venv", "bin", "dbt"),
        os.path.join(os.path.expanduser("~"), ".local", "bin", "dbt"),
        shutil.which("dbt", path=path_value),
    ]
    seen = set()
    for path in candidates:
        if not path or path in seen:
            continue
        seen.add(path)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    raise FileNotFoundError(
        "dbt executable not found. Checked {} and PATH={}".format(
            os.path.join(airflow_home, "dbt_venv", "bin", "dbt"),
            path_value,
        )
    )


def _python_candidates(env: Mapping[str, str]) -> List[str]:
    airflow_home = env.get("AIRFLOW_HOME") or "/opt/airflow"
    path_value = env.get("PATH") or ""
    candidates = [
        env.get("DBT_PYTHON"),
        sys.executable,
        os.path.join(airflow_home, "dbt_venv", "bin", "python"),
        shutil.which("python3", path=path_value),
        shutil.which("python", path=path_value),
    ]
    ordered: List[str] = []
    seen = set()
    for path in candidates:
        if not path or path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    return ordered


def resolve_dbt_argv(env: Optional[Mapping[str, str]] = None) -> List[str]:
    """Return argv that launches dbt (binary or ``python -m dbt``)."""
    environ = dict(os.environ)
    if env is not None:
        environ.update(env)
    try:
        return [resolve_dbt_bin(environ)]
    except FileNotFoundError:
        probe_notes = []
        for python_bin in _python_candidates(environ):
            if not os.path.exists(python_bin):
                continue
            probe = subprocess.run(
                [python_bin, "-m", "dbt", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=environ,
            )
            if probe.returncode == 0:
                return [python_bin, "-m", "dbt"]
            probe_notes.append(
                "{} -m dbt --version exit {}: {}".format(
                    python_bin,
                    probe.returncode,
                    (probe.stdout or "").strip()[:400],
                )
            )
        raise FileNotFoundError(
            "dbt executable not found and python -m dbt failed. {}".format(
                " | ".join(probe_notes) or "no python interpreter probed"
            )
        )


def dbt_work_dir(env: Optional[Mapping[str, str]] = None) -> str:
    """Writable scratch dir. The DAG folder on the worker is read-only."""
    environ = env if env is not None else os.environ
    airflow_home = environ.get("AIRFLOW_HOME") or os.environ.get("AIRFLOW_HOME") or "/tmp"
    path = os.path.join(airflow_home, "tmp", "saal-data-model-dbt")
    os.makedirs(path, exist_ok=True)
    return path


def dbt_log_dir(work_dir: str) -> str:
    path = os.path.join(work_dir, "logs")
    os.makedirs(path, exist_ok=True)
    return path


def dbt_target_dir(work_dir: str) -> str:
    path = os.path.join(work_dir, "target")
    os.makedirs(path, exist_ok=True)
    return path


def dbt_cli_args(
    dbt_command: str,
    dbt_dir: str,
    dbt_target: str,
    dbt_vars: Optional[str] = None,
    log_dir: Optional[str] = None,
    target_dir: Optional[str] = None,
) -> List[str]:
    """Build argv after the dbt binary (run/test plus project flags).

    Project vars already live in dbt_project.yml. Do not pass YAML ``{key: false}``
    --vars; dbt 1.10 can exit 2 with no stdout when that string is used.
    """
    parts = dbt_command.strip().split()
    if parts and parts[0] == "dbt":
        parts = parts[1:]
    if not parts:
        raise ValueError("dbt command is empty")
    args = parts + [
        "--project-dir",
        dbt_dir,
        "--profiles-dir",
        dbt_dir,
        "--target",
        dbt_target,
    ]
    if log_dir:
        args.extend(["--log-path", log_dir])
    if target_dir:
        args.extend(["--target-path", target_dir])
    if dbt_vars:
        args.extend(["--vars", dbt_vars])
    return args


def prepare_dbt_project_dir(dbt_dir: str) -> None:
    """Drop leftover package files so run does not require ``dbt deps``."""
    shutil.rmtree(os.path.join(dbt_dir, "dbt_packages"), ignore_errors=True)
    for name in ("package-lock.yml", "packages.yml"):
        path = os.path.join(dbt_dir, name)
        try:
            os.remove(path)
        except OSError:
            continue


def tail_text(text: str, max_lines: int = 80) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def tail_dbt_log_files(
    dbt_dir: str, max_lines: int = 80, extra_dirs: Optional[Sequence[str]] = None
) -> str:
    candidates = [
        os.path.join(dbt_dir, "logs", "dbt.log"),
        os.path.join(dbt_dir, "target", "dbt.log"),
        os.path.join(dbt_dir, "target", "dbt_airflow_logs", "dbt.log"),
        os.path.join(os.path.expanduser("~"), ".dbt", "logs", "dbt.log"),
    ]
    if extra_dirs:
        for directory in extra_dirs:
            candidates.append(os.path.join(directory, "dbt.log"))
            candidates.append(os.path.join(directory, "captured_stdout.log"))
    chunks = []
    seen = set()
    for path in candidates:
        if not path or path in seen or not os.path.isfile(path):
            continue
        seen.add(path)
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                content = handle.read()
        except OSError as exc:
            chunks.append("{}: {}".format(path, exc))
            continue
        excerpt = tail_text(content, max_lines=max_lines)
        if excerpt:
            chunks.append("----- {} -----\n{}".format(path, excerpt))
    return "\n".join(chunks)


def dbt_runtime_env(env: Mapping[str, str], log_dir: str, target_dir: str) -> dict:
    runtime = dict(env)
    runtime["PYTHONUNBUFFERED"] = "1"
    runtime["NO_COLOR"] = "1"
    runtime["TERM"] = "dumb"
    runtime["DBT_USE_COLORS"] = "false"
    runtime["DBT_LOG_FORMAT"] = "text"
    runtime["DBT_LOG_PATH"] = log_dir
    runtime["DBT_TARGET_PATH"] = target_dir
    runtime["DBT_SEND_ANONYMOUS_USAGE_STATS"] = runtime.get(
        "DBT_SEND_ANONYMOUS_USAGE_STATS", "false"
    )
    return runtime


def run_subprocess(
    argv: Sequence[str],
    *,
    cwd: str,
    env: Mapping[str, str],
    inherit_stdio: bool = False,
    capture_path: Optional[str] = None,
):
    """Run dbt. Inherit stdio so Airflow sees output; optionally tee to a file."""
    command = list(argv)
    merged_env = dict(env)
    if inherit_stdio and capture_path is None:
        return subprocess.run(command, cwd=cwd, env=merged_env)
    if capture_path:
        with open(capture_path, "w", encoding="utf-8") as capture:
            proc = subprocess.run(
                command,
                cwd=cwd,
                env=merged_env,
                stdout=capture,
                stderr=subprocess.STDOUT,
            )
        with open(capture_path, encoding="utf-8", errors="replace") as handle:
            proc.stdout = handle.read()
        return proc
    return subprocess.run(
        command,
        cwd=cwd,
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def format_dbt_failure(
    returncode: int,
    output: str,
    dbt_dir: str,
    argv: Sequence[str],
    extra_log_dirs: Optional[Sequence[str]] = None,
) -> str:
    parts = [
        "dbt command failed with exit code {}".format(returncode),
        "argv: {}".format(" ".join(argv)),
    ]
    stdout_tail = tail_text(output)
    if stdout_tail:
        parts.append(stdout_tail)
    else:
        parts.append("(no dbt stdout/stderr)")
    log_tail = tail_dbt_log_files(dbt_dir, extra_dirs=extra_log_dirs)
    if log_tail:
        parts.append(log_tail)
    else:
        parts.append("(no dbt.log under project, ~/.dbt/logs, or --log-path)")
    return "\n".join(parts)


try:
    from airflow.sdk import DAG
    from airflow.exceptions import AirflowException
    from airflow.sdk import Variable
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.utils.task_group import TaskGroup
except ImportError:
    DAG = None
    AirflowException = RuntimeError
    Variable = None
    PythonOperator = None
    TaskGroup = None
    EmptyOperator = None

_FALLBACK_PIPELINE = {
    "catalog": "dataproducts",
    "schedule": None,
    "tags": ["dbt", "saal", "employee"],
}
_DBT_COMMANDS = [
    "dbt run --select path:models/staging/employee",
    "dbt run --select path:models/intermediate",
    "dbt run --select path:models/semantic",
    "dbt test --select employee_attendance employee_attendance_report employee_attendance_status employee_hr_attendance_status employee_departments int_attendance_day",
]
_TASK_ID_BY_COMMAND = {
    "dbt run --select path:models/staging/employee": "dbt-run-staging",
    "dbt run --select path:models/intermediate": "dbt-run-intermediate",
    "dbt run --select path:models/semantic": "dbt-run-semantic",
    "dbt test --select employee_attendance employee_attendance_report employee_attendance_status employee_hr_attendance_status employee_departments int_attendance_day": "dbt-test",
}


def _saal_model_pipeline_vars():
    pipeline_vars = dict(_FALLBACK_PIPELINE)
    try:
        raw_pipelines = Variable.get("AIRFLOW_PIPELINES")
        if isinstance(raw_pipelines, dict):
            pipelines = raw_pipelines
        else:
            try:
                pipelines = json.loads(raw_pipelines)
            except json.JSONDecodeError:
                repaired = re.sub(r",(\s*[}\]])", r"\1", raw_pipelines)
                pipelines = json.loads(repaired)
        entry = pipelines.get("saal_model") or {}
        if entry.get("catalog"):
            pipeline_vars["catalog"] = entry["catalog"]
        if entry.get("tags"):
            pipeline_vars["tags"] = entry["tags"]
        schedule = entry.get("schedule", "")
        pipeline_vars["schedule"] = (
            None if schedule in ("", None) else schedule
        )
    except Exception as exc:
        print("WARNING: saal_model using pipeline fallback: %s" % exc)
    return pipeline_vars


def _normalize_trino_host(host):
    """dbt-trino host must be a hostname; profiles.yml already sets http_scheme."""
    if not host:
        return host
    value = str(host).strip()
    value = re.sub(r"^https?://", "", value, flags=re.IGNORECASE)
    value = value.split("/")[0]
    if ":" in value and not value.startswith("["):
        host_name, maybe_port = value.rsplit(":", 1)
        if maybe_port.isdigit():
            return host_name
    return value


def _trino_env_from_airflow(dbt_details):
    env_vars = {}
    for key, default in (
        ("TRINO_HOST", None),
        ("TRINO_USER", None),
        ("TRINO_PASSWORD", None),
        ("TRINO_PORT", "443"),
        ("TRINO_DATABASE", "dataproducts"),
        ("TRINO_SCHEMA", "employee"),
    ):
        # Airflow 3: a missing Variable raises AirflowRuntimeError, not KeyError.
        value = Variable.get(key, default=None)
        if value is not None:
            env_vars[key] = str(value)
        elif default is not None:
            env_vars[key] = default
    if dbt_details.get("trino_database"):
        env_vars["TRINO_DATABASE"] = str(dbt_details["trino_database"])
    if dbt_details.get("trino_schema"):
        env_vars["TRINO_SCHEMA"] = str(dbt_details["trino_schema"])
    if not (
        env_vars.get("TRINO_HOST")
        and env_vars.get("TRINO_USER")
        and env_vars.get("TRINO_PASSWORD")
    ):
        try:
            from airflow.hooks.base import BaseHook
        except ImportError:
            from airflow.hooks.base_hook import BaseHook
        conn = BaseHook.get_connection(dbt_details.get("trino_conn_id", "trino-ldap"))
        env_vars["TRINO_HOST"] = env_vars.get("TRINO_HOST") or conn.host
        env_vars["TRINO_USER"] = env_vars.get("TRINO_USER") or conn.login
        env_vars["TRINO_PASSWORD"] = env_vars.get("TRINO_PASSWORD") or conn.password
        if conn.port:
            env_vars["TRINO_PORT"] = str(conn.port)
    env_vars["TRINO_HOST"] = _normalize_trino_host(env_vars.get("TRINO_HOST"))
    missing = [k for k in ("TRINO_HOST", "TRINO_USER", "TRINO_PASSWORD") if not env_vars.get(k)]
    if missing:
        raise AirflowException("Missing Trino settings: {}".format(", ".join(missing)))
    return env_vars


REPORTING_TIMEZONE = ZoneInfo("Asia/Dubai")
PIPELINE_VERSION = "2026-09-01-runtime-period-v8"


def _pendulum_or_datetime_to_period(anchor):
    if anchor is None:
        return None
    if hasattr(anchor, "in_timezone") and hasattr(anchor, "subtract"):
        dubai = anchor.in_timezone("Asia/Dubai")
        previous = dubai.replace(day=1).subtract(months=1)
        return previous.year, previous.month
    anchor = _reporting_datetime(anchor)
    if anchor.month == 1:
        return anchor.year - 1, 12
    return anchor.year, anchor.month - 1


def _attendance_period_from_airflow_context(context=None):
    """Match spark_period_env v8: manual runs use logical_date, scheduled use interval end."""
    if not context:
        return None
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date") or context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    run_type = getattr(dag_run, "run_type", None) if dag_run else None
    if run_type == "manual":
        anchor = logical_date
    else:
        anchor = data_interval_end or logical_date
    return _pendulum_or_datetime_to_period(anchor)


def _attendance_dbt_vars(dbt_details, context=None):
    """Build dbt vars for the reporting month."""
    _ = dbt_details  # reserved for future overrides; period is never pinned in config
    from_env = _period_from_env()
    if from_env:
        year, month = from_env
        print(
            "saal_model using SAAL_REPORTING_* period: {}-{:02d}".format(
                year, month
            )
        )
    else:
        from_context = _attendance_period_from_airflow_context(context)
        if from_context:
            year, month = from_context
            print(
                "saal_model using Airflow run period: {}-{:02d}".format(
                    year, month
                )
            )
        else:
            year, month = _current_attendance_period()
            now = _reporting_datetime()
            print(
                "saal_model reporting timezone Asia/Dubai; now={}; period={}-{:02d}".format(
                    now.isoformat(),
                    year,
                    month,
                )
            )
    return json.dumps(
        {
            "attendance_year": int(year),
            "attendance_month": int(month),
        }
    )


def _period_from_env():
    year_raw = os.environ.get("SAAL_REPORTING_YEAR")
    month_raw = os.environ.get("SAAL_REPORTING_MONTH")
    if year_raw in (None, "") or month_raw in (None, ""):
        return None
    try:
        year, month = int(year_raw), int(month_raw)
    except ValueError:
        return None
    if month < 1 or month > 12:
        return None
    return year, month


def _reporting_datetime(now=None):
    if now is None:
        return datetime.now(REPORTING_TIMEZONE)
    if now.tzinfo is None:
        return now.replace(tzinfo=REPORTING_TIMEZONE)
    return now.astimezone(REPORTING_TIMEZONE)


def _current_attendance_period(now=None):
    """Return the previous calendar month in Dubai time (the reporting period)."""
    from_env = _period_from_env()
    if from_env:
        return from_env
    now = _reporting_datetime(now)
    if now.month == 1:
        return now.year - 1, 12
    return now.year, now.month - 1


def run_dbt_on_worker(dbt_details, dbt_command, dbt_dir, dbt_target, **context):
    trino_env = _trino_env_from_airflow(dbt_details)
    env = dict(os.environ)
    env.update(trino_env)
    work_dir = dbt_work_dir(env)
    log_dir = dbt_log_dir(work_dir)
    target_dir = dbt_target_dir(work_dir)
    capture_path = os.path.join(log_dir, "captured_stdout.log")
    env = dbt_runtime_env(env, log_dir, target_dir)
    print("saal_model dbt launcher=dbtRunner-v5")
    print(
        "Running dbt with Trino host={} user={} port={} database={} schema={}".format(
            trino_env.get("TRINO_HOST"),
            trino_env.get("TRINO_USER"),
            trino_env.get("TRINO_PORT"),
            trino_env.get("TRINO_DATABASE"),
            trino_env.get("TRINO_SCHEMA"),
        )
    )
    print("dbt work dir: {}".format(work_dir))
    try:
        dbt_python = resolve_dbt_python(env)
    except FileNotFoundError as exc:
        raise AirflowException(str(exc)) from exc
    prepare_dbt_project_dir(dbt_dir)
    dbt_vars = _attendance_dbt_vars(dbt_details, context=context)
    dbt_args = dbt_cli_args(
        dbt_command,
        dbt_dir,
        dbt_target,
        dbt_vars=dbt_vars,
        log_dir=log_dir,
        target_dir=target_dir,
    )
    argv = build_invoke_argv(dbt_python, dbt_args)
    print("dbt python: {}".format(dbt_python))
    print("dbt args: {}".format(" ".join(dbt_args)))
    print("dbt log path: {}".format(log_dir))
    version_proc = run_subprocess(
        [
            dbt_python,
            "-c",
            "from importlib.metadata import version; print(version('dbt-core'))",
        ],
        cwd=work_dir,
        env=env,
    )
    if version_proc.stdout:
        print("dbt package version: {}".format(version_proc.stdout.strip()))
    proc = run_subprocess(
        argv,
        cwd=work_dir,
        env=env,
        capture_path=capture_path,
    )
    output = proc.stdout or ""
    if output:
        print(output)
    if proc.returncode != 0:
        raise AirflowException(
            format_dbt_failure(
                proc.returncode,
                output,
                dbt_dir,
                dbt_args,
                extra_log_dirs=[log_dir, target_dir, work_dir],
            )
        )


if DAG is not None:
    _pipeline_vars = _saal_model_pipeline_vars()
    _dbt_details = {
        "trino_conn_id": "trino-ldap",
        "trino_database": "dataproducts",
        "trino_schema": "employee",
    }

    with DAG(
        dag_id="saal_model",
        default_args={"max_active_runs": 1},
        description="saal-data-model dbt via Trino; attendance grain is SAAL business email + date",
        schedule=_pipeline_vars["schedule"],
        start_date=datetime(2023, 5, 6),
        catchup=False,
        max_active_runs=1,
        tags=_pipeline_vars["tags"],
    ) as dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        with TaskGroup(group_id="saal-data-model-dbt") as dbt_group:
            previous = None
            for dbt_command in _DBT_COMMANDS:
                task = PythonOperator(
                    task_id=_TASK_ID_BY_COMMAND.get(
                        dbt_command,
                        dbt_command.replace(" ", "-").replace(":", "-"),
                    ),
                    python_callable=run_dbt_on_worker,
                    op_args=[
                        _dbt_details,
                        dbt_command,
                        _DAG_PACKAGE_ROOT,
                        "prod",
                    ],
                )
                if previous is not None:
                    previous >> task
                previous = task

        start >> dbt_group >> end
