"""Shared helpers for cross-DAG pipeline chaining."""

from __future__ import annotations

from typing import Callable

from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State


def same_day_latest_success_execution_date_fn(
    external_dag_id: str,
    external_task_id: str,
) -> Callable:
    """Resolve an upstream task's logical date for manually triggered DAG runs.

    Manual triggers often use different logical_date values even when started on
    the same calendar day. Match the latest successful upstream run on the same
    day as the current DAG run instead of requiring an exact timestamp match.
    """

    def execution_date_fn(logical_date, **kwargs):
        from airflow.utils.session import provide_session
        from airflow.utils import timezone

        try:
            import pendulum

            anchor = pendulum.instance(logical_date)
            if anchor.timezone_name == "UTC":
                anchor = anchor.in_timezone("Asia/Dubai")
        except Exception:
            anchor = pendulum.instance(logical_date)

        day_start = timezone.convert_to_utc(anchor.start_of("day"))
        day_end = timezone.convert_to_utc(anchor.start_of("day").add(days=1))

        @provide_session
        def lookup(session=None):
            # Airflow 2.10 stores the run time on DagRun.execution_date;
            # logical_date is a read-only alias and cannot be used in SQL queries.
            dag_run = (
                session.query(DagRun)
                .join(
                    TaskInstance,
                    (TaskInstance.dag_id == DagRun.dag_id)
                    & (TaskInstance.run_id == DagRun.run_id),
                )
                .filter(
                    DagRun.dag_id == external_dag_id,
                    TaskInstance.task_id == external_task_id,
                    TaskInstance.state == State.SUCCESS,
                    DagRun.execution_date >= day_start,
                    DagRun.execution_date < day_end,
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )
            if dag_run is None:
                return logical_date
            return dag_run.execution_date

        return lookup()

    return execution_date_fn


def upstream_sensor_kwargs(external_dag_id: str, external_task_id: str) -> dict:
    return {
        "mode": "poke",
        "poke_interval": 60,
        "timeout": 7200,
        "failed_states": ["failed"],
        "execution_date_fn": same_day_latest_success_execution_date_fn(
            external_dag_id,
            external_task_id,
        ),
    }
