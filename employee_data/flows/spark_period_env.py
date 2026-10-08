"""Execution-time reporting period for Spark pods (Airflow Jinja templates)."""

import yaml

PIPELINE_VERSION = "2026-09-01-runtime-period-v8"


def apply_reporting_period_args(spark_job_args):
    """Remove hardcoded period pins; Spark reads SAAL_REPORTING_* pod env instead."""
    stale_keys = (
        "iclock_year",
        "iclock_month",
        "excel_year",
        "excel_month",
        "reporting_year",
        "reporting_month",
    )
    for key in stale_keys:
        removed = spark_job_args.pop(key, None)
        if removed not in (None, "", False):
            print(
                "WARNING: removed stale period arg %s=%s from spark_job_args "
                "(period comes from SAAL_REPORTING_* env at task runtime)"
                % (key, removed)
            )
    return spark_job_args

# Rendered when the Spark task runs, not when the DAG file is parsed.
# Manual runs: logical_date. Scheduled/backfill: data_interval_end (see tests below).
_REPORTING_ANCHOR = (
    "{% if dag_run.run_type == 'manual' %}"
    "{% set anchor = logical_date.in_timezone('Asia/Dubai') %}"
    "{% else %}"
    "{% set anchor = (data_interval_end or logical_date).in_timezone('Asia/Dubai') %}"
    "{% endif %}"
    "{% set pm = anchor.replace(day=1).subtract(months=1) %}"
)
_REPORTING_YEAR_VALUE = _REPORTING_ANCHOR + "{{ pm.year }}"
_REPORTING_MONTH_VALUE = _REPORTING_ANCHOR + "{{ pm.month }}"

REPORTING_PERIOD_ENV = [
    {"name": "SAAL_REPORTING_YEAR", "value": _REPORTING_YEAR_VALUE},
    {"name": "SAAL_REPORTING_MONTH", "value": _REPORTING_MONTH_VALUE},
    {"name": "SAAL_PIPELINE_VERSION", "value": PIPELINE_VERSION},
]


def _yaml_jinja_string_representer(dumper, data):
    """Double-quote YAML scalars that contain Airflow/Jinja markers.

    PyYAML's default single-quoted style escapes inner `'` as `''`, which breaks
    Jinja compilation when Airflow templates the full SparkApplication YAML.
    """
    if "{%" in data or "{{" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


class _SparkApplicationYamlDumper(yaml.SafeDumper):
    pass


_SparkApplicationYamlDumper.add_representer(str, _yaml_jinja_string_representer)


def dump_spark_application(k8s_configuration):
    """Dump SparkApplication YAML without breaking embedded Jinja templates."""
    return yaml.dump(
        k8s_configuration,
        Dumper=_SparkApplicationYamlDumper,
        default_flow_style=False,
        width=10 ** 9,
        allow_unicode=True,
    )


def extend_pod_env(pod_spec):
    """Attach templated env vars to a Spark driver or executor spec."""
    env = list(pod_spec.get("env") or [])
    env.extend(dict(entry) for entry in REPORTING_PERIOD_ENV)
    pod_spec["env"] = env
    return pod_spec
