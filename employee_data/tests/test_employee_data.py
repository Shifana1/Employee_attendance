import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dependencies" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from employee_data import (  # noqa: E402
    DEFAULT_HR_TABLE,
    DEFAULT_ICLOCK_TRANSACTIONS_API,
    DEFAULT_METRICS_TABLE,
    build_iclock_month_url,
    canonicalize_punch_time,
    combine_date_and_time,
    current_iclock_period,
    dedupe_iclock_records,
    fetch_iclock_api_records,
    fetch_iclock_year_records,
    format_api_unreachable_error,
    hr_email_by_employee_id,
    hr_employee_id_by_email,
    iceberg_create_namespace_sql,
    iceberg_create_table_sql,
    iceberg_namespace,
    iceberg_schema_location,
    iceberg_table_location,
    iclock_fetch_windows,
    iclock_month_windows,
    iclock_period_for_job,
    resolve_iclock_window,
    resolve_reporting_period,
    onsite_row_to_punch_events,
    parse_iclock_page,
    raw_api_rows,
    resolve_job_arg,
    resolve_saal_email,
    sources_for_mode,
    split_person_name,
    strip_page_query,
)


def test_resolve_job_arg_uses_config_when_set() -> None:
    url = resolve_job_arg(
        {"iclock_transactions_api": "http://zkteco.internal/iclock/api/transactions/"},
        "iclock_transactions_api",
        DEFAULT_ICLOCK_TRANSACTIONS_API,
    )
    assert url == "http://zkteco.internal/iclock/api/transactions/"


def test_resolve_job_arg_falls_back_to_office_lan_default() -> None:
    assert resolve_job_arg({}, "iclock_transactions_api", DEFAULT_ICLOCK_TRANSACTIONS_API) == (
        DEFAULT_ICLOCK_TRANSACTIONS_API
    )
    assert "page_size=300000" in DEFAULT_ICLOCK_TRANSACTIONS_API
    assert resolve_job_arg(
        {"output_warehouse_fq_table": ""},
        "output_warehouse_fq_table",
        DEFAULT_METRICS_TABLE,
    ) == DEFAULT_METRICS_TABLE


def test_unreachable_error_names_the_host_and_config_key() -> None:
    message = format_api_unreachable_error(
        DEFAULT_ICLOCK_TRANSACTIONS_API, TimeoutError("timed out")
    )
    assert "192.168.145.32" in message
    assert "spark_job_args.iclock_transactions_api" in message
    assert "timed out" in message


def test_iceberg_write_helpers_use_raw_employee() -> None:
    table = DEFAULT_METRICS_TABLE
    assert iceberg_namespace(table) == "raw.employee"
    assert iceberg_schema_location(table) == "s3a://raw/employee"
    assert iceberg_table_location(table) == (
        "s3a://raw/employee/employee_attendance_metrics"
    )
    namespace_sql = iceberg_create_namespace_sql(table)
    assert namespace_sql == (
        "CREATE NAMESPACE IF NOT EXISTS raw.employee LOCATION 's3a://raw/employee'"
    )
    sql = iceberg_create_table_sql(table, "`emp_code` string")
    assert "CREATE TABLE IF NOT EXISTS %s" % table in sql
    assert "USING iceberg" in sql
    assert "LOCATION 's3a://raw/employee/employee_attendance_metrics'" in sql
    script = (SCRIPTS_DIR / "employee_data.py").read_text(encoding="utf-8")
    assert 'mode("Overwrite").save(' not in script
    assert "write_iceberg_overwrite(spark, df, table_path)" in script
    assert "curated_data_employee" not in script
    assert "write_iceberg_overwrite(spark, result_df, table_path)" not in script


def test_raw_api_rows_keep_every_api_column() -> None:
    columns, rows = raw_api_rows(
        [
            {
                "id": 1,
                "emp_code": "154",
                "punch_time": "2026-08-01 09:15:00",
                "longitude": 54.37,
                "latitude": 24.45,
                "source": 0,
                "purpose": 1,
                "is_attendance": 1,
                "sync_status": 1,
                "terminal": {"id": 9, "alias": "Lobby"},
            }
        ]
    )
    assert "emp_code" in columns
    for extra in (
        "longitude",
        "latitude",
        "source",
        "purpose",
        "is_attendance",
        "sync_status",
        "terminal",
    ):
        assert extra in columns
    row = rows[0]
    assert row["id"] == 1
    assert row["longitude"] == 54.37
    assert row["source"] == 0
    assert '"alias": "Lobby"' in row["terminal"]
    assert row["first_name"] is None


def test_office_attendance_uses_api_not_onsite() -> None:
    assert sources_for_mode("api") == ["api"]
    assert sources_for_mode("auto") == ["api", "landing"]
    assert "onsite_table" not in sources_for_mode("auto")
    assert "onsite_excel" not in sources_for_mode("auto")
    assert sources_for_mode("landing") == ["landing"]
    assert sources_for_mode("onsite") == ["onsite_table", "onsite_excel"]


def test_canonicalize_punch_time_accepts_mixed_formats() -> None:
    assert canonicalize_punch_time("2026-07-01 09:15:00") == "2026-07-01 09:15:00"
    assert canonicalize_punch_time("2026-07-01T09:15:00") == "2026-07-01 09:15:00"
    assert canonicalize_punch_time("2026-07-01T09:15:00.000") == "2026-07-01 09:15:00"
    assert canonicalize_punch_time("2026-07-01T09:15:00+04:00") == "2026-07-01 09:15:00"
    assert canonicalize_punch_time("2026-07-01 09:15") == "2026-07-01 09:15:00"
    assert canonicalize_punch_time(None) is None
    assert canonicalize_punch_time("nan") is None


def test_combine_date_and_time_builds_iclock_timestamp() -> None:
    assert combine_date_and_time("2026-07-01", "09:15") == "2026-07-01 09:15:00"
    assert combine_date_and_time("2026-07-01 00:00:00", "18:02:11") == "2026-07-01 18:02:11"
    assert combine_date_and_time("2026-07-01", None) is None
    assert combine_date_and_time("2026-07-01", "nan") is None
    assert combine_date_and_time("2026-07-01", "-") is None


def test_onsite_row_emits_check_in_and_check_out_events() -> None:
    events = onsite_row_to_punch_events(
        {
            "emp_no": "154",
            "employee_id": "199",
            "name": "Mahesh Kezhukkootte",
            "department": "DevOps",
            "date": "2026-07-01",
            "clock_in": "09:15",
            "clock_out": "18:02",
            "clock_in_machine_name": "Lobby",
            "clock_out_machine_name": "Lobby",
        }
    )
    assert len(events) == 2
    assert events[0]["emp_code"] == "199"
    assert events[0]["first_name"] == "Mahesh"
    assert events[0]["last_name"] == "Kezhukkootte"
    assert events[0]["punch_state_display"] == "Check In"
    assert events[0]["punch_time"] == "2026-07-01 09:15:00"
    assert events[1]["punch_state_display"] == "Check Out"
    assert events[1]["punch_time"] == "2026-07-01 18:02:00"


def test_split_person_name_handles_blank() -> None:
    assert split_person_name(None) == (None, None)
    assert split_person_name("  ") == (None, None)
    assert split_person_name("Talson") == ("Talson", None)


def test_hr_email_is_resolved_by_employee_id() -> None:
    rows = [
        {
            "employee_id": "199",
            "business_email": "Mahesh@saal.ai",
        },
        {
            "employee_id": "208",
            "business_email": "talson@saal.ai",
        },
    ]
    by_id = hr_email_by_employee_id(rows)
    by_email = hr_employee_id_by_email(rows)
    assert by_id["199"] == "Mahesh@saal.ai"
    assert by_email["mahesh@saal.ai"] == "199"
    assert resolve_saal_email("199", by_id) == "Mahesh@saal.ai"
    assert resolve_saal_email("208", by_id) == "talson@saal.ai"
    assert resolve_saal_email("999", by_id) is None
    assert (
        resolve_saal_email(
            "154",
            by_id,
            source_email="Mahesh@saal.ai",
            hr_by_email=by_email,
        )
        == "Mahesh@saal.ai"
    )
    assert DEFAULT_HR_TABLE == "raw.employee.employee_hr_data"


def test_parse_iclock_page_follows_relative_next() -> None:
    records, next_url = parse_iclock_page(
        {
            "count": 24,
            "next": "/iclock/api/transactions/?page=2&page_size=12",
            "data": [{"id": 1}, {"id": 2}],
        },
        "http://iclock/iclock/api/transactions/?page_size=12",
    )
    assert len(records) == 2
    assert next_url == "http://iclock/iclock/api/transactions/?page=2&page_size=12"


def test_parse_iclock_page_accepts_results_and_stops_when_next_null() -> None:
    records, next_url = parse_iclock_page(
        {"count": 2, "next": None, "results": [{"id": 1}]},
        "http://iclock/iclock/api/transactions/",
    )
    assert records == [{"id": 1}]
    assert next_url is None


def test_dedupe_iclock_records_keeps_first_id() -> None:
    unique = dedupe_iclock_records(
        [{"id": 1, "emp_code": "a"}, {"id": 1, "emp_code": "b"}, {"emp_code": "c"}]
    )
    assert unique == [{"id": 1, "emp_code": "a"}, {"emp_code": "c"}]


def test_strip_page_query_removes_page_number() -> None:
    url = strip_page_query(
        "http://iclock/api/transactions/?page=2&page_size=300000"
    )
    assert "page=" not in url
    assert "page_size=" not in url


def test_month_url_keeps_large_page_size() -> None:
    from urllib.parse import parse_qsl, urlparse

    url = build_iclock_month_url(
        "http://iclock/api/transactions/?page=3&page_size=300000",
        "2026-07-01 00:00:00",
        "2026-08-01 00:00:00",
    )
    params = dict(parse_qsl(urlparse(url).query))
    assert "page" not in params
    assert params["page_size"] == "300000"
    assert params["limit"] == "300000"
    assert params["start_time"] == "2026-07-01 00:00:00"
    assert params["end_time"] == "2026-08-01 00:00:00"


def test_month_windows_cover_calendar_year() -> None:
    windows = iclock_month_windows(2026)
    assert len(windows) == 12
    assert windows[0] == ("2026-01-01 00:00:00", "2026-02-01 00:00:00")
    assert windows[-1] == ("2026-12-01 00:00:00", "2027-01-01 00:00:00")


def test_month_windows_single_month() -> None:
    windows = iclock_month_windows(2026, month=8)
    assert windows == [("2026-08-01 00:00:00", "2026-09-01 00:00:00")]


def test_july_2026_is_split_past_biotime_21_day_cap() -> None:
    windows = iclock_fetch_windows(2026, month=7, chunk_days=7)
    assert windows[0] == ("2026-07-01 00:00:00", "2026-07-08 00:00:00")
    assert windows[-1][1] == "2026-08-01 00:00:00"
    starts = [start for start, _end in windows]
    assert "2026-07-22 00:00:00" in starts
    assert "2026-07-29 00:00:00" in starts


def test_resolve_iclock_window_defaults_to_previous_month() -> None:
    expected_year, expected_month = current_iclock_period()
    assert resolve_iclock_window("", "") == (expected_year, expected_month)
    assert resolve_iclock_window(None, None) == (expected_year, expected_month)
    assert resolve_iclock_window(2026, None) == (2026, expected_month)
    assert resolve_iclock_window("2026", "8") == (2026, 8)
    try:
        resolve_iclock_window(2026, 13)
    except ValueError as exc:
        assert "iclock_month must be 1-12" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_current_iclock_period_uses_airflow_env() -> None:
    import os

    previous = {
        key: os.environ.get(key)
        for key in (
            "SAAL_REPORTING_YEAR",
            "SAAL_REPORTING_MONTH",
            "SAAL_PIPELINE_VERSION",
        )
    }
    try:
        os.environ["SAAL_REPORTING_YEAR"] = "2026"
        os.environ["SAAL_REPORTING_MONTH"] = "8"
        os.environ["SAAL_PIPELINE_VERSION"] = "2026-09-01-runtime-period-v6"
        assert current_iclock_period() == (2026, 8)
        assert resolve_reporting_period(
            {"reporting_year": 2026, "reporting_month": 7}
        ) == (2026, 8)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_current_iclock_period_uses_dubai_at_month_boundary() -> None:
    import os

    previous = {
        key: os.environ.get(key)
        for key in ("SAAL_REPORTING_YEAR", "SAAL_REPORTING_MONTH")
    }
    try:
        os.environ.pop("SAAL_REPORTING_YEAR", None)
        os.environ.pop("SAAL_REPORTING_MONTH", None)
        dubai = ZoneInfo("Asia/Dubai")
        # Aug 31 22:00 UTC is already Sep 1 in Dubai — previous month must be August.
        september_morning_dubai = datetime(2026, 9, 1, 2, 0, tzinfo=dubai)
        assert current_iclock_period(september_morning_dubai) == (2026, 8)
        # Same instant in UTC is still August — naive UTC would wrongly return July.
        august_late_utc = datetime(2026, 8, 31, 22, 0, tzinfo=ZoneInfo("UTC"))
        assert current_iclock_period(august_late_utc) == (2026, 8)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_resolve_reporting_period_uses_config_when_env_missing() -> None:
    import os

    previous = {
        key: os.environ.get(key)
        for key in ("SAAL_REPORTING_YEAR", "SAAL_REPORTING_MONTH")
    }
    try:
        os.environ.pop("SAAL_REPORTING_YEAR", None)
        os.environ.pop("SAAL_REPORTING_MONTH", None)
        assert resolve_reporting_period(
            {"reporting_year": 2026, "reporting_month": 8}
        ) == (2026, 8)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_job_period_ignores_stale_july_config() -> None:
    import os

    previous = {
        key: os.environ.get(key)
        for key in ("SAAL_REPORTING_YEAR", "SAAL_REPORTING_MONTH")
    }
    try:
        os.environ.pop("SAAL_REPORTING_YEAR", None)
        os.environ.pop("SAAL_REPORTING_MONTH", None)
        assert resolve_reporting_period(
            {
                "iclock_year": 2026,
                "iclock_month": 7,
                "reporting_year": 2026,
                "reporting_month": 8,
            }
        ) == (2026, 8)
        expected = current_iclock_period()
        assert iclock_period_for_job({"iclock_year": 2026, "iclock_month": 7}) == expected
        assert iclock_period_for_job({}) == expected
        config = json.loads(
            (
                Path(__file__).resolve().parents[1] / "config" / "employee_data.json"
            ).read_text(encoding="utf-8")
        )
        args = config["airflow-task-details"]["employee-data"]["spark_details"][
            "spark_job_args"
        ]
        assert "reporting_year" not in args
        assert "reporting_month" not in args
        assert "iclock_year" not in args
        assert "iclock_month" not in args
        script = (SCRIPTS_DIR / "employee_data.py").read_text(encoding="utf-8")
        assert "iclock_year, iclock_month = iclock_period_for_job(spark_job_args)" in script
        assert "resolve_reporting_period" in script
        assert "year, month = int(year), int(month)" in script
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_fetch_iclock_api_records_follows_next_pages(monkeypatch) -> None:
    class _Response:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, **_kwargs):
        from urllib.parse import parse_qsl, urlparse

        params = dict(parse_qsl(urlparse(url).query))
        if params.get("page") == "2":
            return _Response({"count": 3, "next": None, "data": [{"id": 3}]})
        return _Response(
            {
                "count": 3,
                "next": "http://iclock/api/transactions/?page=2&page_size=300000",
                "data": [{"id": 1}, {"id": 2}],
            }
        )

    monkeypatch.setattr("employee_data.requests.get", fake_get)
    records = fetch_iclock_api_records(
        "http://iclock/api/transactions/?page_size=300000",
        start_time="2026-07-01 00:00:00",
        end_time="2026-08-01 00:00:00",
    )
    assert [row["id"] for row in records] == [1, 2, 3]


def test_fetch_iclock_year_records_calls_each_month(monkeypatch) -> None:
    class _Response:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, **_kwargs):
        from urllib.parse import parse_qsl, urlparse

        params = dict(parse_qsl(urlparse(url).query))
        assert params["page_size"] == "300000"
        assert params["limit"] == "300000"
        month = params["start_time"][5:7]
        return _Response({"data": [{"id": int(month)}]})

    monkeypatch.setattr("employee_data.requests.get", fake_get)
    records = fetch_iclock_year_records(
        "http://iclock/api/transactions/?page_size=300000", 2026
    )
    assert len(records) == 12
    assert [row["id"] for row in records] == list(range(1, 13))


def test_fetch_iclock_year_records_one_month(monkeypatch) -> None:
    class _Response:
        status_code = 200
        text = ""

        def json(self):
            return {"data": [{"id": 8}]}

    def fake_get(url, **_kwargs):
        from urllib.parse import parse_qsl, urlparse

        params = dict(parse_qsl(urlparse(url).query))
        assert params["start_time"] >= "2026-08-01 00:00:00"
        assert params["end_time"] <= "2026-09-01 00:00:00"
        assert params["page_size"] == "300000"
        return _Response()

    monkeypatch.setattr("employee_data.requests.get", fake_get)
    records = fetch_iclock_year_records(
        "http://iclock/api/transactions/?page_size=300000", 2026, month=8
    )
    assert records == [{"id": 8}]

