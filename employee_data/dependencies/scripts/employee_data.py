import sys
import tempfile
import requests
import json, csv, io
from collections import OrderedDict
from datetime import date, timedelta
from datetime import datetime, timezone
import ast
import os
import pyspark
import uuid
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DecimalType
from pyspark.sql.types import FloatType, LongType, ArrayType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql import functions as T
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark import SparkFiles
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import to_date, current_timestamp
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.functions import concat_ws,upper,date_format
import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql.functions import date_add, date_format
import xml.etree.ElementTree as ET
import json
import pandas as pd
import logging
import os
import shutil
import uuid
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

api_url = "https://api.rpb2b.io/horses/profile/horse/"
race_runner_data = "datawarehouse.horse.runner_details"
pedigree_horse_data = "datawarehouse.horse.horse_details"
horse_country = "datawarehouse.horse.horse_country_details"
neo_horse_sire_dam_id = "datawarehouse.horse.neo_horse_sire_dam_id"
neo_horse_rp_horse_id = "datawarehouse.horse.neo_horse_rp_horse_id"
neo4j_url = "bolt://internal-qa3-horse.saal.ai:30410"
label_horse = "Horse"
auction_horse = "datawarehouse.horse.horse_auction"
horse_detailed_info = "datawarehouse.horse.horse_detailed_information"
database_name = "race-QA"
database_name_auction = "auction-QA"
mongo_uri = "mongodb://admin:admin@alpha-w3.saal.ai:27020/admin?retryWrites=true&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1"
rprace = "race-QA"

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%y/%m/%d %H:%M:%S')

logging = logging.getLogger()
# ***** ***** ***** ***** ***** ***** Job Properties ***** ***** ***** ***** ***** *****

DEFAULT_ICLOCK_TRANSACTIONS_API = (
    "http://192.168.145.32:8081/iclock/api/transactions/?page_size=300000"
)
DEFAULT_METRICS_TABLE = "dataproducts.raw.employee_attendance_metrics"
DEFAULT_METRICS_HISTORY_TABLE = "dataproducts.raw.employee_attendance_metrics_history"
DEFAULT_ICLOCK_LANDING_PATH = "s3a://landing/employee/iclock_transactions/"
DEFAULT_ONSITE_TABLE = "dataproducts.raw.onsite_employee"
DEFAULT_ONSITE_EXCEL_PATH = "s3a://lakehouse/saal_employee/onsite_employee/"
DEFAULT_ICLOCK_SOURCE = "api"
DEFAULT_HR_TABLE = "dataproducts.raw.employee_hr_data"
DEFAULT_ICLOCK_PAGE_SIZE = 300000
REPORTING_YEAR_COL = "reporting_year"
REPORTING_MONTH_COL = "reporting_month"
MAX_ICLOCK_PAGE_SIZE = 300000
DEFAULT_ICLOCK_CHUNK_DAYS = 7


# Asia/Dubai is fixed UTC+4 (no DST). Spark 3.1 images ship Python 3.8 without zoneinfo.
REPORTING_TIMEZONE = timezone(timedelta(hours=4))
PIPELINE_VERSION = "2026-09-01-runtime-period-v8"


def _parse_period_component(value):
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def resolve_reporting_period(spark_job_args=None, now=None):
    """Return (year, month) for the attendance fetch/report window."""
    from_env = _period_from_env()
    if from_env:
        logging.info(
            "Using Airflow SAAL_REPORTING_* period: %s-%02d",
            from_env[0],
            from_env[1],
        )
        return from_env
    now = _reporting_datetime(now)
    if now.month == 1:
        auto_year, auto_month = now.year - 1, 12
    else:
        auto_year, auto_month = now.year, now.month - 1
    if spark_job_args:
        year = _parse_period_component(spark_job_args.get("reporting_year"))
        month = _parse_period_component(spark_job_args.get("reporting_month"))
        if year is not None and month is not None:
            if month < 1 or month > 12:
                raise ValueError("reporting_month must be 1-12, got %s" % month)
            logging.info(
                "Using reporting_year/month from job config: %s-%02d",
                year,
                month,
            )
            return year, month
    logging.info(
        "Using Dubai auto period: %s-%02d (now=%s)",
        auto_year,
        auto_month,
        now.isoformat(),
    )
    return auto_year, auto_month


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
    """Wall clock in Asia/Dubai (UTC+4) for monthly attendance windows."""
    if now is None:
        return datetime.now(REPORTING_TIMEZONE)
    if now.tzinfo is None:
        return now.replace(tzinfo=REPORTING_TIMEZONE)
    return now.astimezone(REPORTING_TIMEZONE)


def current_iclock_period(now=None, spark_job_args=None):
    """Previous calendar month — config, env, then Dubai auto."""
    logging.info(
        "employee_data pipeline version: %s",
        os.environ.get("SAAL_PIPELINE_VERSION", PIPELINE_VERSION),
    )
    return resolve_reporting_period(spark_job_args=spark_job_args, now=now)
ICLOCK_USERNAME = "saaleng"
ICLOCK_PASSWORD = "S@@L@admin123#"
ICLOCK_FALLBACK_AUTH_TOKEN = (
    "JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo0LCJ1c2VybmFtZSI6InNhYWxlbmci"
    "LCJleHAiOjE3MTQwMjc5MjIsImVtYWlsIjoiIiwib3JpZ19pYXQiOjE3MTM0MjMxMjJ9."
    "RYjIx9ChKG5UEWMhqnkNjPeCnHx2RHtTGXzfY01XGMk"
)

METRICS_COLUMNS = [
    "id",
    "emp",
    "emp_code",
    "first_name",
    "last_name",
    "department",
    "position",
    "punch_time",
    "punch_state",
    "punch_state_display",
    "verify_type",
    "verify_type_display",
    "work_code",
    "gps_location",
    "area_alias",
    "terminal_sn",
    "temperature",
    "is_mask",
    "terminal_alias",
    "upload_time",
]
METRICS_INT_COLUMNS = ("id", "emp", "verify_type")
METRICS_DOUBLE_COLUMNS = ("temperature",)
_EMPTY_CELL = {"", "nan", "none", "nat", "null", "-", "--"}
_PUNCH_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
)
ONSITE_PUNCH_RENAME = {
    "Emp No.": "emp_no",
    "Name": "name",
    "Date": "date",
    "Clock In": "clock_in",
    "Clock Out": "clock_out",
    "Department": "department",
    "Machines Name": "clock_in_machine_name",
    "Machines Name.1": "clock_out_machine_name",
}


def iceberg_ident_parts(table: str):
    """Return (catalog, schema, table) from a fully qualified Iceberg name."""
    parts = [part.strip() for part in table.split(".") if part.strip()]
    if len(parts) < 3:
        raise ValueError(
            "Iceberg table must be catalog.schema.table, got: %s" % table
        )
    return parts[0], parts[1], parts[2]


def iceberg_namespace(table: str):
    """Return catalog.namespace from a fully qualified Iceberg table name."""
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "%s.%s" % (catalog, schema)


def iceberg_schema_location(table: str):
    """S3 warehouse path for catalog.schema (Hive database location)."""
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s" % (catalog, schema)


def iceberg_table_location(table: str):
    """S3 path for an Iceberg table under its schema."""
    catalog, schema, table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s/%s" % (catalog, schema, table_name)


def iceberg_column_ddl(columns):
    """Build a CREATE TABLE column list from (name, spark_type) pairs."""
    return ",\n  ".join("`%s` %s" % (name, dtype) for name, dtype in columns)


def iceberg_create_namespace_sql(table):
    """CREATE NAMESPACE IF NOT EXISTS catalog.schema LOCATION s3a://catalog/schema."""
    return "CREATE NAMESPACE IF NOT EXISTS %s LOCATION '%s'" % (
        iceberg_namespace(table),
        iceberg_schema_location(table),
    )


def iceberg_create_table_sql(table, column_ddl):
    """CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION s3a://..."""
    return (
        "CREATE TABLE IF NOT EXISTS %s (\n  %s\n)\nUSING iceberg\nLOCATION '%s'"
        % (table, column_ddl, iceberg_table_location(table))
    )


def schema_to_column_ddl(schema):
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    return iceberg_column_ddl(columns)


def write_iceberg_overwrite(spark, df, table):
    """Create the Iceberg namespace/table if missing, then overwrite rows.

    Always replace the table schema on overwrite. insertInto writes nulls into
    checkintime/lastcheckintime when the existing column type does not match.
    """
    namespace_sql = iceberg_create_namespace_sql(table)
    logging.info("Ensuring Iceberg schema exists: %s", namespace_sql)
    spark.sql(namespace_sql)
    create_sql = iceberg_create_table_sql(table, schema_to_column_ddl(df.schema))
    logging.info("Ensuring Iceberg table exists:\n%s", create_sql)
    spark.sql(create_sql)
    logging.info("Overwriting Iceberg table %s with current schema", table)
    df.write.format("iceberg").mode("overwrite").option(
        "overwrite-schema", "true"
    ).saveAsTable(table)


def iceberg_history_delete_sql(table, year, month):
    """DELETE rows for one reporting period before append (idempotent re-run)."""
    iceberg_ident_parts(table)
    return (
        "DELETE FROM %s WHERE %s = %s AND %s = %s"
        % (table, REPORTING_YEAR_COL, int(year), REPORTING_MONTH_COL, int(month))
    )


def schema_to_history_column_ddl(schema):
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    names = {name.lower() for name, _dtype in columns}
    if REPORTING_YEAR_COL.lower() not in names:
        columns.append((REPORTING_YEAR_COL, "int"))
    if REPORTING_MONTH_COL.lower() not in names:
        columns.append((REPORTING_MONTH_COL, "int"))
    return iceberg_column_ddl(columns)


def with_reporting_period(df, year, month):
    year_i, month_i = int(year), int(month)
    if month_i < 1 or month_i > 12:
        raise ValueError("reporting_month must be 1-12, got %s" % month_i)
    out = df
    if REPORTING_YEAR_COL not in out.columns:
        out = out.withColumn(REPORTING_YEAR_COL, lit(year_i).cast("int"))
    if REPORTING_MONTH_COL not in out.columns:
        out = out.withColumn(REPORTING_MONTH_COL, lit(month_i).cast("int"))
    return out


def write_iceberg_month_history(spark, df, table, year, month):
    """Create history if missing, delete the period, then append. Never DROP history."""
    if not table:
        raise ValueError("history table is required")
    year_i, month_i = int(year), int(month)
    history_df = with_reporting_period(df, year_i, month_i)

    namespace_sql = iceberg_create_namespace_sql(table)
    logging.info("Ensuring Iceberg schema exists: %s", namespace_sql)
    spark.sql(namespace_sql)
    create_sql = iceberg_create_table_sql(
        table, schema_to_history_column_ddl(history_df.schema)
    )
    logging.info("Ensuring Iceberg history table exists:\n%s", create_sql)
    spark.sql(create_sql)

    table_cols = spark.table(table).columns
    missing = [name for name in table_cols if name not in history_df.columns]
    extra = [name for name in history_df.columns if name not in table_cols]
    if missing or extra:
        raise ValueError(
            "History table %s schema drift; refusing recreate/overwrite. "
            "extra=%s missing=%s"
            % (table, extra, missing)
        )

    delete_sql = iceberg_history_delete_sql(table, year_i, month_i)
    logging.info("Replacing history period: %s", delete_sql)
    spark.sql(delete_sql)

    logging.info(
        "Appending period %s-%02d to history %s", year_i, month_i, table
    )
    history_df.select(*table_cols).write.format("iceberg").mode("append").insertInto(
        table
    )


def resolve_job_arg(spark_job_args, key, default):
    configured = spark_job_args.get(key)
    if configured:
        return str(configured)
    return default


def format_api_unreachable_error(api_url, exc):
    return (
        "iClock transactions API is unreachable from Spark: %s. "
        "Pods in spark-jobs cannot route to this host (connect timeout). "
        "Point spark_job_args.iclock_transactions_api at a URL the cluster "
        "can reach (VPN, ingress, or internal service). Original error: %s"
        % (api_url, exc)
    )


def sources_for_mode(mode):
    normalized = (mode or DEFAULT_ICLOCK_SOURCE).strip().lower()
    if normalized == "auto":
        return ["api", "landing"]
    if normalized == "api":
        return ["api"]
    if normalized == "landing":
        return ["landing"]
    if normalized == "onsite":
        return ["onsite_table", "onsite_excel"]
    raise ValueError(
        "Unknown iclock_source %s. Use api, auto (api then landing), landing, or onsite." % mode
    )


def api_column_names(records):
    """Union of all API keys. Keep known metrics columns first for a stable layout."""
    names = []
    seen = set()
    for name in METRICS_COLUMNS:
        names.append(name)
        seen.add(name)
    for raw in records:
        if not isinstance(raw, dict):
            continue
        for key in raw.keys():
            name = str(key)
            if name not in seen:
                seen.add(name)
                names.append(name)
    return names


def coerce_raw_api_value(value):
    """Keep primitives; JSON-encode objects/arrays so Spark can store them."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return value
    return str(value)


def raw_api_rows(records):
    """One dict per API record, including every key present in the payload."""
    columns = api_column_names(records)
    rows = []
    for raw in records:
        payload = raw if isinstance(raw, dict) else {}
        row = {}
        for name in columns:
            value = coerce_raw_api_value(payload.get(name))
            if name == "punch_time" and value is not None:
                canonical = canonicalize_punch_time(value)
                row[name] = canonical if canonical is not None else (
                    value if isinstance(value, str) else str(value)
                )
            else:
                row[name] = value
        rows.append(row)
    for name in columns:
        seen_types = {
            type(row[name]) for row in rows if row[name] is not None
        }
        if len(seen_types) > 1:
            for row in rows:
                if row[name] is not None and not isinstance(row[name], str):
                    row[name] = str(row[name])
    return columns, rows


def schema_from_raw_rows(columns, rows):
    fields = []
    for name in columns:
        sample = next((row[name] for row in rows if row[name] is not None), None)
        if isinstance(sample, bool):
            dtype = BooleanType()
        elif isinstance(sample, int) and not isinstance(sample, bool):
            dtype = IntegerType()
        elif isinstance(sample, float):
            dtype = DoubleType()
        else:
            dtype = StringType()
        fields.append(StructField(name, dtype, True))
    return StructType(fields)


def spark_dataframe_from_api_records(spark, records):
    """Build a DataFrame with every column returned by the iClock API."""
    if not records:
        return spark.createDataFrame([], attendance_metrics_schema())
    columns, rows = raw_api_rows(records)
    logging.info("Raw API columns (%s): %s", len(columns), columns)
    return spark.createDataFrame(rows, schema_from_raw_rows(columns, rows))


def attendance_metrics_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("emp", IntegerType(), True),
        StructField("emp_code", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("position", StringType(), True),
        StructField("punch_time", StringType(), True),
        StructField("punch_state", StringType(), True),
        StructField("punch_state_display", StringType(), True),
        StructField("verify_type", IntegerType(), True),
        StructField("verify_type_display", StringType(), True),
        StructField("work_code", StringType(), True),
        StructField("gps_location", StringType(), True),
        StructField("area_alias", StringType(), True),
        StructField("terminal_sn", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("is_mask", StringType(), True),
        StructField("terminal_alias", StringType(), True),
        StructField("upload_time", StringType(), True),
    ])


def _cell(value):
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _EMPTY_CELL:
        return None
    return text


def split_person_name(name):
    if name is None:
        return None, None
    parts = str(name).strip().split(None, 1)
    if not parts or parts[0].lower() in _EMPTY_CELL:
        return None, None
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else None
    return first_name, last_name


def normalize_email(value):
    text = _cell(value)
    return text.lower() if text else None


def hr_email_by_employee_id(hr_rows):
    """Map HR employee_id to SAAL business_email."""
    mapping = {}
    for row in hr_rows:
        employee_id = _cell(row.get("employee_id"))
        email = _cell(row.get("business_email"))
        if not employee_id or not email:
            continue
        mapping.setdefault(employee_id, email)
    return mapping


def hr_employee_id_by_email(hr_rows):
    """Map lowercase HR business_email to employee_id."""
    mapping = {}
    for row in hr_rows:
        employee_id = _cell(row.get("employee_id"))
        email = normalize_email(row.get("business_email"))
        if not employee_id or not email:
            continue
        mapping.setdefault(email, employee_id)
    return mapping


def resolve_saal_email(
    employee_id, hr_by_employee_id, source_email=None, hr_by_email=None
):
    """Return HR SAAL email from employee_id, else from source email."""
    key = _cell(employee_id)
    if key:
        email = hr_by_employee_id.get(key)
        if email:
            return email
    if not hr_by_email:
        return None
    matched_id = hr_by_email.get(normalize_email(source_email) or "")
    if not matched_id:
        return None
    return hr_by_employee_id.get(matched_id)


def _strip_timezone_suffix(text):
    trimmed = text.strip()
    if trimmed.endswith("Z"):
        trimmed = trimmed[:-1]
    if len(trimmed) > 6 and trimmed[-3] == ":" and trimmed[-6] in "+-":
        return trimmed[:-6].strip()
    if len(trimmed) > 5 and trimmed[-5] in "+-" and trimmed[-4:].isdigit():
        return trimmed[:-5].strip()
    return trimmed


def canonicalize_punch_time(value):
    """Normalize punch_time to yyyy-MM-dd HH:mm:ss for Spark timestamp parsing."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = _strip_timezone_suffix(str(value).strip())
    if not text or text.lower() in _EMPTY_CELL:
        return None
    candidates = [text]
    if "T" in text:
        candidates.append(text.replace("T", " ", 1))
    for candidate in candidates:
        for fmt in _PUNCH_TIME_FORMATS:
            try:
                return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
    return text


def parsed_punch_timestamp(column="punch_time"):
    """Parse mixed punch_time values without nulling existing timestamps.

    Applying to_timestamp(col, format) to a timestamp column stringifies it
    (often with .0 or a timezone) and the format miss then nulls checkintime
    and lastcheckintime.
    """
    punch = col(column)
    as_string = trim(regexp_replace(punch.cast("string"), "T", " "))
    without_fraction = regexp_replace(as_string, r"\.\d+$", "")
    return coalesce(
        punch.cast("timestamp"),
        to_timestamp(without_fraction, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(as_string, "yyyy-MM-dd HH:mm:ss.SSS"),
        to_timestamp(as_string, "yyyy-MM-dd HH:mm:ss.S"),
        to_timestamp(without_fraction, "yyyy-MM-dd HH:mm"),
        to_timestamp(without_fraction, "yyyy-MM-dd"),
        to_timestamp(as_string),
    )


def combine_date_and_time(date_value, time_value):
    if date_value is None or time_value is None:
        return None
    date_str = str(date_value).strip()
    time_str = str(time_value).strip()
    if date_str.lower() in _EMPTY_CELL or time_str.lower() in _EMPTY_CELL:
        return None
    date_part = date_str.replace("T", " ").split()[0]
    time_part = time_str.replace("T", " ").split()[-1]
    if time_part.count(":") == 1:
        time_part = time_part + ":00"
    if len(time_part) >= 8:
        time_part = time_part[:8]
    return "%s %s" % (date_part, time_part)


def normalize_metrics_record(raw):
    record = {name: None for name in METRICS_COLUMNS}
    if not isinstance(raw, dict):
        return record
    for name in METRICS_COLUMNS:
        if name not in raw or raw[name] is None:
            continue
        value = raw[name]
        if name in METRICS_INT_COLUMNS:
            try:
                record[name] = int(value)
            except (TypeError, ValueError):
                record[name] = None
        elif name in METRICS_DOUBLE_COLUMNS:
            try:
                record[name] = float(value)
            except (TypeError, ValueError):
                record[name] = None
        else:
            if name == "punch_time":
                record[name] = canonicalize_punch_time(value)
            else:
                record[name] = value if isinstance(value, str) else str(value)
    return record


def onsite_row_to_punch_events(row):
    emp_code = row.get("employee_id") or row.get("emp_code") or row.get("emp_no")
    if emp_code is not None:
        emp_code = str(emp_code).strip()
        if emp_code.lower() in _EMPTY_CELL:
            emp_code = None
    first_name, last_name = split_person_name(row.get("name"))
    department = row.get("department")
    punch_date = row.get("date")
    events = []
    for time_value, display, machine_key, state in (
        (row.get("clock_in"), "Check In", "clock_in_machine_name", "0"),
        (row.get("clock_out"), "Check Out", "clock_out_machine_name", "1"),
    ):
        punch_time = combine_date_and_time(punch_date, time_value)
        if not punch_time:
            continue
        record = {name: None for name in METRICS_COLUMNS}
        record["emp_code"] = emp_code
        record["first_name"] = first_name
        record["last_name"] = last_name
        record["department"] = None if department is None else str(department)
        record["punch_time"] = punch_time
        record["punch_state"] = state
        record["punch_state_display"] = display
        machine = row.get(machine_key)
        if machine is not None and str(machine).strip().lower() not in _EMPTY_CELL:
            record["terminal_alias"] = str(machine)
        events.append(record)
    return events


def _hadoop_fs(spark, s3_uri):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(s3_uri)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), jvm


def list_s3_objects(spark, s3_uri, extension):
    trimmed = (s3_uri or "").strip()
    fs, jvm = _hadoop_fs(spark, trimmed if trimmed else "s3a://landing/")
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    ext = extension.lower()
    if trimmed.lower().endswith(ext):
        path = hadoop_path(trimmed)
        if not fs.exists(path):
            raise FileNotFoundError("Object not found: %s" % trimmed)
        return [trimmed]
    prefix = trimmed.rstrip("/") + "/"
    statuses = fs.globStatus(hadoop_path(prefix + "*" + ext))
    paths = sorted(str(status.getPath()) for status in statuses)
    if not paths:
        raise FileNotFoundError("No %s files found under landing path: %s" % (ext, prefix))
    return paths


def _copy_s3_object_to_local(spark, s3_uri, suffix):
    fs, jvm = _hadoop_fs(spark, s3_uri)
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    local_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    local_file.close()
    fs.copyToLocalFile(False, hadoop_path(s3_uri), hadoop_path(local_file.name))
    return local_file.name


def load_landing_json_records(spark, landing_path):
    records = []
    for uri in list_s3_objects(spark, landing_path, ".json"):
        local_path = _copy_s3_object_to_local(spark, uri, ".json")
        try:
            with open(local_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        finally:
            try:
                os.remove(local_path)
            except OSError:
                logging.warning("Could not remove temp file %s", local_path)
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            records.extend(payload["data"])
        elif isinstance(payload, list):
            records.extend(payload)
        else:
            raise ValueError("Landing JSON %s must be a list or {\"data\": [...]} " % uri)
    if not records:
        raise ValueError("Landing JSON at %s did not contain punch records" % landing_path)
    return records


def load_onsite_table_records(spark, onsite_table):
    try:
        onsite_df = spark.table(onsite_table)
    except Exception as exc:
        raise FileNotFoundError("Onsite table not found: %s (%s)" % (onsite_table, exc))
    events = []
    for row in onsite_df.collect():
        events.extend(onsite_row_to_punch_events(row.asDict(recursive=True)))
    if not events:
        raise ValueError("Onsite table %s has no clock-in/out times" % onsite_table)
    return events


def load_onsite_excel_records(spark, excel_s3_path):
    try:
        import pandas as pd
        import openpyxl  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "pandas and openpyxl must be installed on the pyspark image to read "
            "onsite landing Excel files."
        ) from exc
    events = []
    local_paths = []
    try:
        for uri in list_s3_objects(spark, excel_s3_path, ".xlsx"):
            logging.info("Reading onsite landing Excel: %s", uri)
            local_path = _copy_s3_object_to_local(spark, uri, ".xlsx")
            local_paths.append(local_path)
            pdf = pd.read_excel(local_path, sheet_name=0, dtype=str)
            pdf.columns = [ONSITE_PUNCH_RENAME.get(column, column) for column in pdf.columns]
            required = ["emp_no", "name", "date", "clock_in", "clock_out"]
            missing = [column for column in required if column not in pdf.columns]
            if missing:
                raise ValueError("Excel %s is missing columns: %s" % (uri, missing))
            frame = pdf.where(pdf.notna(), None).astype("string")
            for row in frame.to_dict(orient="records"):
                events.extend(onsite_row_to_punch_events(row))
    finally:
        for local_path in local_paths:
            try:
                os.remove(local_path)
            except OSError:
                logging.warning("Could not remove temp file %s", local_path)
    if not events:
        raise ValueError("Onsite Excel at %s has no clock-in/out times" % excel_s3_path)
    return events


def parse_iclock_page(payload, page_url):
    """Return (records, next_url) from one BioTime/iClock transactions page."""
    if isinstance(payload, list):
        return payload, None
    if not isinstance(payload, dict):
        raise ValueError("iClock transactions API response is not JSON object or list")
    records = payload.get("data")
    if not isinstance(records, list):
        records = payload.get("results")
    if not isinstance(records, list):
        raise ValueError("iClock transactions API response is missing a 'data' list")
    next_url = payload.get("next")
    if next_url is None or str(next_url).strip().lower() in {"", "none", "null"}:
        return records, None
    return records, urljoin(page_url, str(next_url).strip())


def dedupe_iclock_records(records):
    """Keep first occurrence when paginated pages overlap on transaction id."""
    seen_ids = set()
    unique = []
    for record in records:
        if isinstance(record, dict) and record.get("id") is not None:
            record_id = record["id"]
            if record_id in seen_ids:
                continue
            seen_ids.add(record_id)
        unique.append(record)
    return unique


def query_params(url):
    return dict(parse_qsl(urlparse(url).query, keep_blank_values=True))


def default_iclock_end_time():
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def resolve_iclock_page_size(api_url, configured=None):
    raw = configured
    if not raw:
        params = query_params(api_url)
        raw = params.get("page_size") or params.get("limit")
    try:
        page_size = int(raw) if raw else DEFAULT_ICLOCK_PAGE_SIZE
    except (TypeError, ValueError):
        page_size = DEFAULT_ICLOCK_PAGE_SIZE
    if page_size < 1:
        page_size = DEFAULT_ICLOCK_PAGE_SIZE
    return page_size


def resolve_iclock_time_filter(configured, default):
    if configured == "":
        return None
    if configured is None:
        return default
    return str(configured)


def build_iclock_page_url(api_url, page, page_size, start_time=None, end_time=None):
    """Build page N with BioTime page, page_size, and limit. Keep existing filters."""
    parsed = urlparse(api_url)
    query = query_params(api_url)
    query["page"] = str(page)
    query["page_size"] = str(page_size)
    query["limit"] = str(page_size)
    if start_time and "start_time" not in query:
        query["start_time"] = start_time
    if end_time and "end_time" not in query:
        query["end_time"] = end_time
    return urlunparse(parsed._replace(query=urlencode(query)))


def iclock_token_url(api_url):
    parsed = urlparse(api_url)
    return urlunparse((parsed.scheme, parsed.netloc, "/jwt-api-token-auth/", "", "", ""))


def obtain_iclock_jwt(api_url, username, password):
    token_url = iclock_token_url(api_url)
    try:
        response = requests.post(
            token_url,
            json={"username": username, "password": password},
            timeout=(10, 30),
        )
    except requests.exceptions.RequestException as exc:
        logging.warning("iClock JWT login could not reach %s: %s", token_url, exc)
        return None
    if response.status_code != 200:
        logging.warning(
            "iClock JWT login %s returned HTTP %s", token_url, response.status_code
        )
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    token = payload.get("token") or payload.get("access")
    if not token:
        return None
    return str(token).replace("JWT ", "").strip()


def iclock_auth_headers(api_url):
    token = obtain_iclock_jwt(api_url, ICLOCK_USERNAME, ICLOCK_PASSWORD)
    if token:
        logging.info("Using freshly issued iClock JWT")
        return {"Authorization": "JWT %s" % token}
    logging.warning("Using fallback iClock JWT; expired tokens often return a truncated page")
    return {"Authorization": ICLOCK_FALLBACK_AUTH_TOKEN}


def strip_page_query(api_url):
    """Drop page, page_size, and limit so BioTime is not paged by query params."""
    drop = {"page", "page_size", "limit"}
    parsed = urlparse(api_url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in drop
    ]
    return urlunparse(parsed._replace(query=urlencode(query)))


def iclock_month_windows(year, month=None):
    """Month windows for a year, or a single month when month is 1-12."""
    if month is None:
        months = range(1, 13)
    else:
        month = int(month)
        if month < 1 or month > 12:
            raise ValueError("iclock_month must be 1-12, got %s" % month)
        months = [month]
    windows = []
    year = int(year)
    for current_month in months:
        start = datetime(year, current_month, 1)
        if current_month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, current_month + 1, 1)
        windows.append(
            (
                start.strftime("%Y-%m-%d %H:%M:%S"),
                end.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    return windows


def split_time_windows(start_time, end_time, chunk_days=DEFAULT_ICLOCK_CHUNK_DAYS):
    """Split [start, end) into short windows. BioTime often caps a query at ~21 days."""
    start = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")
    try:
        chunk_days = int(chunk_days)
    except (TypeError, ValueError):
        chunk_days = DEFAULT_ICLOCK_CHUNK_DAYS
    if chunk_days < 1:
        chunk_days = DEFAULT_ICLOCK_CHUNK_DAYS
    windows = []
    cursor = start
    delta = timedelta(days=chunk_days)
    while cursor < end:
        nxt = cursor + delta
        if nxt > end:
            nxt = end
        windows.append(
            (
                cursor.strftime("%Y-%m-%d %H:%M:%S"),
                nxt.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
        cursor = nxt
    return windows


def iclock_fetch_windows(
    year, month=None, chunk_days=DEFAULT_ICLOCK_CHUNK_DAYS
):
    windows = []
    for start_time, end_time in iclock_month_windows(year, month=month):
        windows.extend(split_time_windows(start_time, end_time, chunk_days=chunk_days))
    return windows


def resolve_iclock_year(configured):
    if configured in (None, "", False):
        return None
    try:
        return int(configured)
    except (TypeError, ValueError):
        return None


def resolve_iclock_month(configured):
    if configured in (None, "", False):
        return None
    try:
        return int(configured)
    except (TypeError, ValueError):
        return None


def resolve_iclock_window(year, month):
    """Return (year, month). Missing values default to the previous calendar month."""
    year = resolve_iclock_year(year)
    month = resolve_iclock_month(month)
    default_year, default_month = current_iclock_period()
    if year is None:
        year = default_year
    if month is None:
        month = default_month
    if month < 1 or month > 12:
        raise ValueError("iclock_month must be 1-12, got %s" % month)
    return year, month


def iclock_period_for_job(spark_job_args=None):
    """Resolve the iClock fetch month from config, env, or Dubai auto."""
    year, month = resolve_reporting_period(spark_job_args=spark_job_args)
    now = _reporting_datetime()
    logging.info(
        "Reporting timezone Asia/Dubai; now=%s; fetch period=%s-%02d",
        now.isoformat(),
        year,
        month,
    )
    return year, month


def build_iclock_month_url(api_url, start_time, end_time, page_size=None):
    """Month window with a large page_size so BioTime does not return one tiny page."""
    page_size = resolve_iclock_page_size(api_url, page_size)
    parsed = urlparse(strip_page_query(api_url))
    query = query_params(urlunparse(parsed))
    query.pop("page", None)
    query["page_size"] = str(page_size)
    query["limit"] = str(page_size)
    query["start_time"] = start_time
    query["end_time"] = end_time
    return urlunparse(parsed._replace(query=urlencode(query)))


def continue_iclock_next_url(next_url, request_url):
    """Keep the date window and page_size when BioTime's next link drops them."""
    current = query_params(request_url)
    parsed = urlparse(next_url)
    nxt_query = query_params(next_url)
    for key in ("start_time", "end_time", "page_size", "limit"):
        if current.get(key) and not nxt_query.get(key):
            nxt_query[key] = current[key]
    return urlunparse(parsed._replace(query=urlencode(nxt_query)))


MAX_ICLOCK_PAGES = 500


def fetch_iclock_api_records(api_url, page_size=None, start_time=None, end_time=None):
    """GET BioTime punches. Month windows keep page_size and follow next pages."""
    username = ICLOCK_USERNAME
    password = ICLOCK_PASSWORD
    auth_token = ICLOCK_FALLBACK_AUTH_TOKEN
    if start_time and end_time:
        request_url = build_iclock_month_url(api_url, start_time, end_time, page_size)
    else:
        request_url = api_url
    collected = []
    seen_urls = set()
    for _page in range(MAX_ICLOCK_PAGES):
        if request_url in seen_urls:
            break
        seen_urls.add(request_url)
        logging.info("Fetching iClock transactions from %s", request_url)
        try:
            response = requests.get(
                request_url,
                headers={"Authorization": auth_token},
                auth=(username, password),
                timeout=(15, 120),
            )
        except requests.exceptions.RequestException as exc:
            raise ConnectionError(format_api_unreachable_error(request_url, exc)) from exc
        if response.status_code != 200:
            logging.warning(
                "iClock HTTP %s for %s: %s",
                response.status_code,
                request_url,
                response.text[:500],
            )
            break
        payload = response.json()
        try:
            records, next_url = parse_iclock_page(payload, request_url)
        except ValueError:
            records, next_url = [], None
            if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                records = payload["data"]
        collected.extend(records)
        logging.info(
            "iClock page records=%s running_total=%s next=%s",
            len(records),
            len(collected),
            bool(next_url),
        )
        if not next_url:
            break
        request_url = continue_iclock_next_url(next_url, request_url)
    collected = dedupe_iclock_records(collected)
    logging.info("iClock fetched %s unique records", len(collected))
    return collected


def fetch_iclock_year_records(
    api_url, year, month=None, chunk_days=DEFAULT_ICLOCK_CHUNK_DAYS
):
    collected = []
    page_size = resolve_iclock_page_size(api_url)
    for start_time, end_time in iclock_fetch_windows(
        year, month=month, chunk_days=chunk_days
    ):
        logging.info("iClock window %s -> %s", start_time, end_time)
        collected.extend(
            fetch_iclock_api_records(
                api_url,
                page_size=page_size,
                start_time=start_time,
                end_time=end_time,
            )
        )
    collected = dedupe_iclock_records(collected)
    if collected:
        punch_times = sorted(
            str(record.get("punch_time"))
            for record in collected
            if record.get("punch_time") not in (None, "")
        )
        if punch_times:
            logging.info(
                "iClock punch_time range for %s-%s: %s -> %s",
                year,
                month if month is not None else "all",
                punch_times[0],
                punch_times[-1],
            )
    logging.info(
        "Fetched %s iClock punch records for year %s month %s",
        len(collected),
        year,
        month if month is not None else "all",
    )
    return collected


def load_punch_records(
    spark,
    source_mode,
    api_url,
    landing_path,
    onsite_table,
    onsite_excel,
    api_page_size=None,
    api_start_time=None,
    api_end_time=None,
):
    errors = []
    for source in sources_for_mode(source_mode):
        try:
            if source == "api":
                records = fetch_iclock_api_records(
                    api_url,
                    page_size=api_page_size,
                    start_time=api_start_time,
                    end_time=api_end_time,
                )
            elif source == "landing":
                logging.info("Loading iClock JSON landing path %s", landing_path)
                records = load_landing_json_records(spark, landing_path)
            elif source == "onsite_table":
                logging.info("Loading punch events from onsite table %s", onsite_table)
                records = load_onsite_table_records(spark, onsite_table)
            else:
                logging.info("Loading punch events from onsite Excel %s", onsite_excel)
                records = load_onsite_excel_records(spark, onsite_excel)
        except Exception as exc:
            logging.warning("Punch source %s failed: %s", source, exc)
            errors.append("%s: %s" % (source, exc))
            continue
        logging.info("Loaded %s punch records from %s", len(records), source)
        return [normalize_metrics_record(record) for record in records], source
    raise ConnectionError(
        "No iClock punch source loaded (mode=%s, tried %s). Failures: %s. "
        "employee_attendance is office iClock only; onsite stays in %s. "
        "Point spark_job_args.iclock_transactions_api at a reachable BioTime URL "
        "or land iClock JSON under %s."
        % (
            source_mode,
            ", ".join(sources_for_mode(source_mode)),
            " | ".join(errors),
            onsite_table,
            landing_path,
        )
    )


# Import Generic Dependencies here
def import_generic_libs(spark, python_dependencies_base_paths=""):
    logging.info(
        "> current_dir: %s",
        os.listdir(os.path.dirname(os.path.abspath(__file__))),
    )
    if not python_dependencies_base_paths:
        return
    for python_dependencies_base_path in python_dependencies_base_paths.split(","):
        python_dependencies_base_path = str(python_dependencies_base_path).strip()
        if not python_dependencies_base_path:
            continue
        logging.info(
            "> Listing dependencies: %s",
            os.listdir(os.path.abspath(python_dependencies_base_path)),
        )

        for py_file in os.listdir(os.path.abspath(python_dependencies_base_path)):
            if py_file == '__init__.py' or py_file == '__pycache__':
                continue
            python_dep_file_path = SparkFiles.get(os.path.join(python_dependencies_base_path, py_file))
            spark.sparkContext.addPyFile(python_dep_file_path)

        # Driver might need some deps (if used --pyFiles this is not needed!!!)
        sys.path.append(python_dependencies_base_path)

def fetching_employeedata_api(
    spark,
    api_url=None,
    table_path=None,
    year=None,
    month=None,
    chunk_days=DEFAULT_ICLOCK_CHUNK_DAYS,
    history_table_path=None,
):
    """Fetch BioTime punches for the previous calendar month.

    Configured iclock_year/iclock_month values are ignored. BioTime is queried
    in short date windows because a full-month start_time/end_time is often
    truncated around day 21.
    """
    api_url = api_url or DEFAULT_ICLOCK_TRANSACTIONS_API
    table_path = table_path or DEFAULT_METRICS_TABLE
    if year is None or month is None:
        raise ValueError("iClock fetch requires year and month")
    year, month = int(year), int(month)
    logging.info("Fetching iClock year %s month %s", year, month)
    data = fetch_iclock_year_records(
        api_url, year, month=month, chunk_days=chunk_days
    )
    df = spark_dataframe_from_api_records(spark, data)
    logging.info("Prepared %s attendance metric rows", len(data))
    write_iceberg_overwrite(spark, df, table_path)
    if history_table_path:
        write_iceberg_month_history(spark, df, history_table_path, year, month)
        logging.info(
            "History period %s-%02d written to %s",
            year,
            month,
            history_table_path,
        )


def fetch_data(api_url, username, password, auth_token):
    headers = {"Authorization": auth_token}
    response = requests.get(api_url, headers=headers, auth=(username, password))
    if response.status_code == 200:
        return response.json()
    return None


def main(
    spark,
    api_url=None,
    metrics_table=None,
    year=None,
    month=None,
    chunk_days=DEFAULT_ICLOCK_CHUNK_DAYS,
    history_table_path=None,
):
    logging.info("---- Starting spark stream -----")
    fetching_employeedata_api(
        spark,
        api_url=api_url,
        table_path=metrics_table,
        year=year,
        month=month,
        chunk_days=chunk_days,
        history_table_path=history_table_path,
    )
    logging.info("Employee attendance metrics loaded successfully")


if __name__ == '__main__':
    spark_app_name = sys.argv[1]

    # Please provide below params accordingly
    # spark.sql.shuffle.partitions , spark.default.parallelism

    spark = SparkSession.builder.appName(spark_app_name) \
        .config("spark.task.maxFailures", "1") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20").getOrCreate()

    # *********************************************************************************
    # Reading Job args

    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    hadoop_custom_arguments = json.loads(sc.getConf().get('spark.hadoop.custom.arguments'))

    spark_job_args = hadoop_custom_arguments["spark_job_args"]
    cluster_details_spark_args = hadoop_custom_arguments["cluster_details_spark_args"]

    minio_s3_credentials = cluster_details_spark_args["s3_credentials"]
    warehouse_catalog = cluster_details_spark_args["catalog"]
    catalog_minio_bucket = cluster_details_spark_args["catalog_minio_bucket"]
    # Reading Job args
    # *********************************************************************************

    python_dependencies_base_paths = spark_job_args.get("python_dependencies_base_paths", "")
    import_generic_libs(spark, python_dependencies_base_paths)

    iclock_transactions_api = resolve_job_arg(
        spark_job_args, "iclock_transactions_api", DEFAULT_ICLOCK_TRANSACTIONS_API
    )
    metrics_table = resolve_job_arg(
        spark_job_args, "output_warehouse_fq_table", DEFAULT_METRICS_TABLE
    )
    history_table = resolve_job_arg(
        spark_job_args,
        "output_warehouse_fq_history_table",
        DEFAULT_METRICS_HISTORY_TABLE,
    )
    iclock_year, iclock_month = iclock_period_for_job(spark_job_args)
    iclock_chunk_days = spark_job_args.get(
        "iclock_chunk_days", DEFAULT_ICLOCK_CHUNK_DAYS
    )
    logging.info(
        "SAAL_REPORTING_YEAR env: %s",
        os.environ.get("SAAL_REPORTING_YEAR"),
    )
    logging.info(
        "SAAL_REPORTING_MONTH env: %s",
        os.environ.get("SAAL_REPORTING_MONTH"),
    )
    logging.info(
        "SAAL_PIPELINE_VERSION env: %s",
        os.environ.get("SAAL_PIPELINE_VERSION", PIPELINE_VERSION),
    )
    logging.info("iClock transactions API: %s", iclock_transactions_api)
    logging.info("iClock year: %s", iclock_year)
    logging.info("iClock month: %s", iclock_month)
    logging.info("iClock chunk days: %s", iclock_chunk_days)
    logging.info("Metrics table: %s", metrics_table)
    logging.info("Metrics history table: %s", history_table)

    try:
        main(
            spark,
            api_url=iclock_transactions_api,
            metrics_table=metrics_table,
            year=iclock_year,
            month=iclock_month,
            chunk_days=iclock_chunk_days,
            history_table_path=history_table,
        )
    except Exception as exc:
        logging.error(exc)
        raise Exception("! Something went wrong with this job") from exc
    finally:
        logging.info("Stopping...")
        spark.stop()
