import os
import sys

import pytest
import yaml
from jinja2 import Environment

_FLOWS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "flows"
)
if _FLOWS_DIR not in sys.path:
    sys.path.insert(0, _FLOWS_DIR)

from spark_period_env import (
    REPORTING_PERIOD_ENV,
    apply_reporting_period_args,
    dump_spark_application,
    extend_pod_env,
)


@pytest.mark.parametrize("entry", REPORTING_PERIOD_ENV)
def test_reporting_env_values_compile_as_jinja_after_yaml_dump(entry):
    dumped = dump_spark_application({"value": entry["value"]})
    assert "''Asia/Dubai''" not in dumped
    Environment().from_string(dumped)


def test_extend_pod_env_preserves_jinja_compatibility():
    pod_spec = extend_pod_env({})
    dumped = dump_spark_application({"driver": pod_spec})
    Environment().from_string(dumped)


def test_default_yaml_dump_breaks_single_quote_jinja():
    legacy = (
        "{% set anchor = (data_interval_end or logical_date).in_timezone('Asia/Dubai') %}"
        "{{ pm.year }}"
    )
    broken = yaml.dump({"value": legacy}, default_flow_style=False, width=10 ** 9)
    with pytest.raises(Exception):
        Environment().from_string(broken)


def test_safe_dump_fixes_single_quote_jinja():
    legacy = (
        "{% set anchor = (data_interval_end or logical_date).in_timezone('Asia/Dubai') %}"
        "{{ pm.year }}"
    )
    dumped = dump_spark_application({"value": legacy})
    assert "''Asia/Dubai''" not in dumped
    Environment().from_string(dumped)


def test_apply_reporting_period_args_strips_hardcoded_month_pins():
    args = apply_reporting_period_args(
        {
            "iclock_year": 2026,
            "iclock_month": 7,
            "reporting_year": 2026,
            "reporting_month": 7,
            "iclock_transactions_api": "http://example/api/",
        }
    )
    assert args == {"iclock_transactions_api": "http://example/api/"}


class _FakeAirflowDate:
    """Minimal pendulum-like datetime for Jinja period tests."""

    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day

    def in_timezone(self, _tz):
        return self

    def replace(self, day=1):
        return _FakeAirflowDate(self.year, self.month, day)

    def subtract(self, months=0):
        month = self.month - months
        year = self.year
        while month < 1:
            month += 12
            year -= 1
        return _FakeAirflowDate(year, month, self.day)


def _render_period(template, context):
    return Environment().from_string(template).render(**context)


def _period_template():
    return REPORTING_PERIOD_ENV[0]["value"].replace(
        "{{ pm.year }}", "{{ pm.year }}-{{ pm.month }}"
    )


def test_reporting_period_manual_run_uses_logical_date():
    """Manual September trigger must fetch August, not July from August interval end."""

    class _Run:
        run_type = "manual"

    context = {
        "logical_date": _FakeAirflowDate(2026, 9, 1),
        "data_interval_end": _FakeAirflowDate(2026, 8, 2),
        "dag_run": _Run(),
    }
    broken_template = (
        "{% set anchor = (data_interval_end or logical_date).in_timezone('Asia/Dubai') %}"
        "{% set pm = anchor.replace(day=1).subtract(months=1) %}"
        "{{ pm.year }}-{{ pm.month }}"
    )
    assert _render_period(broken_template, context) == "2026-7"
    assert _render_period(_period_template(), context) == "2026-8"


def test_reporting_period_scheduled_run_uses_data_interval_end():
    """Scheduled 2nd-of-month run uses interval end (September) not interval start (August)."""

    class _Run:
        run_type = "scheduled"

    context = {
        "logical_date": _FakeAirflowDate(2026, 8, 2),
        "data_interval_end": _FakeAirflowDate(2026, 9, 2),
        "dag_run": _Run(),
    }
    logical_only = (
        "{% set anchor = logical_date.in_timezone('Asia/Dubai') %}"
        "{% set pm = anchor.replace(day=1).subtract(months=1) %}"
        "{{ pm.year }}-{{ pm.month }}"
    )
    assert _render_period(logical_only, context) == "2026-7"
    assert _render_period(_period_template(), context) == "2026-8"
