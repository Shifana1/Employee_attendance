import json
import logging
import os
import sys
import tempfile
import calendar
from datetime import date, datetime, time, timezone, timedelta
from typing import Dict, Iterable, List, Sequence, Tuple

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%y/%m/%d %H:%M:%S",
)

logger = logging.getLogger()

# ZKTeco daily attendance export headers → snake_case Iceberg columns.
# Upload workbooks to MinIO lakehouse by year/month, e.g.:
#   mc cp "DailyAttendance_July.xlsx" lakehouse/saal_employee/onsite_employee/2026/july/
DEFAULT_ONSITE_EXCEL_PATH = "s3a://lakehouse/saal_employee/onsite_employee/"
DEFAULT_ONSITE_TABLE = "raw.employee.onsite_employee"
DEFAULT_ONSITE_HISTORY_TABLE = "raw.employee.onsite_employee_history"
REPORTING_YEAR_COL = "reporting_year"
REPORTING_MONTH_COL = "reporting_month"
_MONTH_FOLDER_NAMES = frozenset(
    calendar.month_name[month_index].lower() for month_index in range(1, 13)
)

ONSITE_COLUMN_RENAME_MAP: Dict[str, str] = {
    "State": "state",
    "Emp No.": "emp_no",
    "AC-No.": "ac_no",
    "No.": "no",
    "Name": "name",
    "Auto-Assign": "auto_assign",
    "Date": "date",
    "Timetable": "timetable",
    "On duty": "on_duty",
    "Off duty": "off_duty",
    "Clock In": "clock_in",
    "Machines Name": "clock_in_machine_name",
    "Clock Out": "clock_out",
    "Machines Name.1": "clock_out_machine_name",
    "Normal": "normal",
    "Real time": "real_time",
    "Late": "late",
    "Early": "early",
    "Absent": "absent",
    "OT Time": "ot_time",
    "Work Time": "work_time",
    "Exception": "exception",
    "Must C/In": "must_c_in",
    "Must C/Out": "must_c_out",
    "Department": "department",
    "NDays": "n_days",
    "WeekEnd": "weekend",
    "Holiday": "holiday",
    "ATT_Time": "att_time",
    "NDays_OT": "n_days_ot",
    "WeekEnd_OT": "weekend_ot",
    "Holiday_OT": "holiday_ot",
    "EmailN": "email",
}

def should_replace_iceberg_schema(load_strategy: str, df_types: Sequence[Tuple[str, str]], table_types: Sequence[Tuple[str, str]]) -> bool:
    """Overwrite recreates the table so timestamp vs string drift cannot null punches."""
    if (load_strategy or "overwrite").strip().lower() == "overwrite":
        return True
    df_by_name = {name.lower(): dtype for name, dtype in df_types}
    table_by_name = {name.lower(): dtype for name, dtype in table_types}
    if set(df_by_name) != set(table_by_name):
        return True
    return any(df_by_name[name] != table_by_name[name] for name in df_by_name)


ONSITE_COLUMN_ORDER: List[str] = [
    "state",
    "emp_no",
    "ac_no",
    "no",
    "name",
    "auto_assign",
    "date",
    "timetable",
    "on_duty",
    "off_duty",
    "clock_in",
    "clock_in_machine_name",
    "clock_out",
    "clock_out_machine_name",
    "normal",
    "real_time",
    "late",
    "early",
    "absent",
    "ot_time",
    "work_time",
    "exception",
    "must_c_in",
    "must_c_out",
    "department",
    "n_days",
    "weekend",
    "holiday",
    "att_time",
    "n_days_ot",
    "weekend_ot",
    "holiday_ot",
    "email",
]


# Asia/Dubai is fixed UTC+4 (no DST). Spark 3.1 images ship Python 3.8 without zoneinfo.
REPORTING_TIMEZONE = timezone(timedelta(hours=4))
PIPELINE_VERSION = "2026-09-02-onsite-clock-cast-v10"


def _parse_period_component(value):
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def resolve_reporting_period(spark_job_args=None, now=None):
    from_env = _period_from_env()
    if from_env:
        logger.info(
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
            logger.info(
                "Using reporting_year/month from job config: %s-%02d",
                year,
                month,
            )
            return year, month
    logger.info(
        "Using Dubai auto period: %s-%02d (now=%s)",
        auto_year,
        auto_month,
        now.isoformat(),
    )
    return auto_year, auto_month


def current_landing_period(now=None, spark_job_args=None):
    logger.info(
        "onsite_employee pipeline version: %s",
        os.environ.get("SAAL_PIPELINE_VERSION", PIPELINE_VERSION),
    )
    return resolve_reporting_period(spark_job_args=spark_job_args, now=now)


def month_folder_name(month):
    month = int(month)
    if month < 1 or month > 12:
        raise ValueError("landing month must be 1-12, got %s" % month)
    return calendar.month_name[month].lower()


def resolve_landing_year(configured):
    if configured in (None, "", False):
        return None
    return int(configured)


def resolve_landing_month(configured):
    if configured in (None, "", False):
        return None
    return int(configured)


def path_looks_like_month_folder(path):
    parts = [part for part in path.rstrip("/").split("/") if part]
    if len(parts) < 2:
        return False
    month_part = parts[-1].lower()
    year_part = parts[-2]
    return (
        month_part in _MONTH_FOLDER_NAMES
        and year_part.isdigit()
        and len(year_part) == 4
    )


def resolve_monthly_excel_path(excel_s3_path, year=None, month=None):
    """Return S3 prefix or .xlsx URI for the landing month folder.

    - Explicit .xlsx object → unchanged.
    - Path already ending in YYYY/month (e.g. .../2026/july/) → unchanged.
    - Base prefix (e.g. s3a://lakehouse/saal_employee/onsite_employee/) → append YYYY/month/.
      Year/month always default to the previous calendar month in Dubai time.
      Configured excel_year/excel_month values are ignored.
    """
    trimmed = (excel_s3_path or "").strip()
    if not trimmed:
        raise ValueError("excel_s3_path is required")
    if trimmed.lower().endswith(".xlsx"):
        return trimmed
    if path_looks_like_month_folder(trimmed):
        return trimmed.rstrip("/") + "/"
    resolved_year = _parse_period_component(year)
    resolved_month = _parse_period_component(month)
    if resolved_year is None or resolved_month is None:
        resolved_year, resolved_month = current_landing_period()
    now = _reporting_datetime()
    logger.info(
        "Reporting timezone Asia/Dubai; now=%s; landing period=%s-%02d",
        now.isoformat(),
        resolved_year,
        resolved_month,
    )
    return "%s/%s/%s/" % (
        trimmed.rstrip("/"),
        resolved_year,
        month_folder_name(resolved_month),
    )


def normalize_onsite_columns(columns: Sequence[str]) -> List[str]:
    """Rename ZKTeco headers to snake_case; unknown headers are left unchanged."""
    return [ONSITE_COLUMN_RENAME_MAP.get(column, column) for column in columns]


_EMPTY_CELL_TEXT = frozenset({"", "nan", "nat", "none", "<na>", "null"})
_TIME_COLUMNS = frozenset({"clock_in", "clock_out", "on_duty", "off_duty"})
_DATE_COLUMNS = frozenset({"date"})
_DATE_TEXT_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%d-%b-%Y",
    "%d-%b-%y",
    "%Y/%m/%d",
)
_EXCEL_TIME_EPOCH_DATES = frozenset(
    {date(1899, 12, 30), date(1899, 12, 31), date(1900, 1, 1)}
)
_PICKLE_LEAK_MARKERS = ("razorvine.pickle", "classdictconstructor")


def is_pickle_leak(value):
    """Spark toString leftover from pandas/Excel objects that failed pickle."""
    if value is None:
        return False
    text = str(value).strip().lower()
    return any(marker in text for marker in _PICKLE_LEAK_MARKERS)


def _is_missing_cell(value):
    if value is None:
        return True
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    try:
        if value != value:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _format_excel_fraction_time(value):
    seconds = int(round(float(value) * 24 * 60 * 60)) % (24 * 60 * 60)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return "%02d:%02d:%02d" % (hours, minutes, secs)


def _normalize_date_text(text):
    """ZKTeco Date is MM/DD/YYYY (e.g. 08/13/2026). Prefer that over DD/MM."""
    for fmt in _DATE_TEXT_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def _stringify_cell(value, column=None):
    """Return a plain Python str or None. Never pd.NA / numpy / Excel objects.

    spark.createDataFrame(pandas StringDtype) pickles pd.NA and datetime.time as
    net.razorvine.pickle.objects.ClassDictConstructor@..., which lands in Iceberg
    as a fake email and makes clock_in/clock_out unparseable.
    """
    if _is_missing_cell(value):
        return None
    if isinstance(value, datetime):
        if value.date() in _EXCEL_TIME_EPOCH_DATES:
            return value.strftime("%H:%M:%S")
        if column in _DATE_COLUMNS or value.time() == time.min:
            return value.strftime("%Y-%m-%d")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, time):
        return value.strftime("%H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if (
        column in _TIME_COLUMNS
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
        and 0 <= float(value) < 1
    ):
        return _format_excel_fraction_time(value)
    text = str(value).strip()
    if not text or text.lower() in _EMPTY_CELL_TEXT or is_pickle_leak(text):
        return None
    if column in _DATE_COLUMNS:
        return _normalize_date_text(text)
    return text


def _stringify_frame(pdf):
    """Keep cell values as landed strings. Do not rewrite emails or clocks."""
    data = {
        column: [_stringify_cell(value, column) for value in pdf[column].tolist()]
        for column in pdf.columns
    }
    return pdf.__class__(data, columns=list(pdf.columns), dtype=object)


def onsite_spark_rows(pdf):
    """Tuples of str|None in ONSITE_COLUMN_ORDER for spark.createDataFrame."""
    ordered = pdf[ONSITE_COLUMN_ORDER]
    return [
        tuple(_stringify_cell(value, column) for column, value in zip(ONSITE_COLUMN_ORDER, row))
        for row in ordered.itertuples(index=False, name=None)
    ]


def clock_text_is_time(value):
    if value is None:
        return False
    text = str(value).strip()
    if not text or is_pickle_leak(text):
        return False
    return ":" in text and any(character.isdigit() for character in text)


def assert_onsite_clocks_landed(rows):
    """Refuse to overwrite Iceberg when Clock In did not survive Excel → Spark."""
    clock_index = ONSITE_COLUMN_ORDER.index("clock_in")
    usable = sum(1 for row in rows if clock_text_is_time(row[clock_index]))
    logger.info("Onsite rows with parseable clock_in: %s / %s", usable, len(rows))
    if rows and usable == 0:
        raise ValueError(
            "Onsite Excel loaded 0 parseable clock_in values. Refusing to overwrite "
            "raw.employee.onsite_employee (stg_onsite_employee.first_punch_in would "
            "stay null). Deploy onsite_employee.py and confirm Clock In in the "
            "landing workbook, e.g. 06:59."
        )
    return usable


def import_generic_libs(spark, python_dependencies_base_paths: str) -> None:
    from pyspark import SparkFiles

    logger.info("> current_dir: %s" % str(os.listdir(os.path.dirname(os.path.abspath(__file__)))))
    if not python_dependencies_base_paths:
        return
    for python_dependencies_base_path in python_dependencies_base_paths.split(","):
        python_dependencies_base_path = str(python_dependencies_base_path).strip()
        logger.info(
            "> Listing dependencies: %s"
            % str(os.listdir(os.path.abspath(python_dependencies_base_path)))
        )
        for py_file in os.listdir(os.path.abspath(python_dependencies_base_path)):
            if py_file == "__init__.py" or py_file == "__pycache__":
                continue
            python_dep_file_path = SparkFiles.get(
                os.path.join(python_dependencies_base_path, py_file)
            )
            spark.sparkContext.addPyFile(python_dep_file_path)
        sys.path.append(python_dependencies_base_path)


def _require_excel_libraries():
    try:
        import pandas as pd  # noqa: F401
        import openpyxl  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "pandas and openpyxl must be installed on the pyspark image to read "
            "landing Excel files. Install both packages or add a spark-excel JAR."
        ) from exc
    return pd


def _hadoop_fs(spark, s3_uri: str):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(s3_uri)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), jvm


def list_excel_s3_paths(spark, excel_s3_path: str) -> List[str]:
    """Return s3a URIs for a single .xlsx object or every .xlsx under a prefix."""
    trimmed = excel_s3_path.strip()
    fs, jvm = _hadoop_fs(spark, trimmed if trimmed else "s3a://lakehouse/")
    hadoop_path = jvm.org.apache.hadoop.fs.Path

    if trimmed.lower().endswith(".xlsx"):
        path = hadoop_path(trimmed)
        if not fs.exists(path):
            raise FileNotFoundError("Excel object not found: %s" % trimmed)
        return [trimmed]

    prefix = trimmed.rstrip("/") + "/"
    statuses = fs.globStatus(hadoop_path(prefix + "*.xlsx"))
    paths = sorted(str(status.getPath()) for status in statuses)
    if not paths:
        raise FileNotFoundError(
            "No .xlsx files found under landing path: %s" % prefix
        )
    return paths


def _copy_s3_object_to_local(spark, s3_uri: str) -> str:
    fs, jvm = _hadoop_fs(spark, s3_uri)
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    local_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    local_file.close()
    fs.copyToLocalFile(False, hadoop_path(s3_uri), hadoop_path(local_file.name))
    return local_file.name


def read_excel_workbooks(spark, excel_s3_paths: Iterable[str]):
    pd = _require_excel_libraries()
    frames = []
    local_paths: List[str] = []
    try:
        for s3_uri in excel_s3_paths:
            logger.info("Reading landing Excel: %s", s3_uri)
            local_path = _copy_s3_object_to_local(spark, s3_uri)
            local_paths.append(local_path)
            pdf = pd.read_excel(local_path, sheet_name=0, dtype=str)
            pdf.columns = normalize_onsite_columns(list(pdf.columns))
            missing = [column for column in ONSITE_COLUMN_ORDER if column not in pdf.columns]
            if missing:
                raise ValueError(
                    "Excel %s is missing expected columns after rename: %s"
                    % (s3_uri, missing)
                )
            frames.append(_stringify_frame(pdf[ONSITE_COLUMN_ORDER]))
    finally:
        for local_path in local_paths:
            try:
                os.remove(local_path)
            except OSError:
                logger.warning("Could not remove temp file %s", local_path)

    combined = pd.concat(frames, ignore_index=True)
    return combined


def onsite_iceberg_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [StructField(column, StringType(), True) for column in ONSITE_COLUMN_ORDER]
    )


def iceberg_ident_parts(table: str) -> Tuple[str, str, str]:
    """Return (catalog, schema, table) from a fully qualified Iceberg name."""
    parts = [part.strip() for part in table.split(".") if part.strip()]
    if len(parts) < 3:
        raise ValueError(
            "Iceberg table must be catalog.schema.table, got: %s" % table
        )
    return parts[0], parts[1], parts[2]


def iceberg_namespace(table: str) -> str:
    """Return catalog.namespace from a fully qualified Iceberg table name."""
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "%s.%s" % (catalog, schema)


def iceberg_schema_location(table: str) -> str:
    """S3 warehouse path for catalog.schema (Hive database location)."""
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s" % (catalog, schema)


def iceberg_table_location(table: str) -> str:
    """S3 path for an Iceberg table under its schema."""
    catalog, schema, table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s/%s" % (catalog, schema, table_name)


def iceberg_column_ddl(columns: Sequence[Tuple[str, str]]) -> str:
    """Build a CREATE TABLE column list from (name, spark_type) pairs."""
    return ",\n  ".join("`%s` %s" % (name, dtype) for name, dtype in columns)


def iceberg_create_namespace_sql(table: str) -> str:
    """CREATE NAMESPACE IF NOT EXISTS catalog.schema LOCATION s3a://catalog/schema."""
    return "CREATE NAMESPACE IF NOT EXISTS %s LOCATION '%s'" % (
        iceberg_namespace(table),
        iceberg_schema_location(table),
    )


def iceberg_drop_table_sql(table: str) -> str:
    """DROP TABLE IF EXISTS catalog.schema.table."""
    iceberg_ident_parts(table)
    return "DROP TABLE IF EXISTS %s" % table


def iceberg_create_table_sql(table: str, column_ddl: str) -> str:
    """CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION s3a://..."""
    return (
        "CREATE TABLE IF NOT EXISTS %s (\n  %s\n)\nUSING iceberg\nLOCATION '%s'"
        % (table, column_ddl, iceberg_table_location(table))
    )


def schema_to_column_ddl(schema) -> str:
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    return iceberg_column_ddl(columns)


def write_iceberg_table(spark, df, table: str, load_strategy: str) -> None:
    """Create the Iceberg namespace/table if missing, then write rows.

    Overwrite drops the table first. overwrite-schema does not convert an
    existing timestamp clock_in column to string, and insertInto then stores
    Excel times as null (or as timestamps staging cannot parse).
    """
    namespace_sql = iceberg_create_namespace_sql(table)
    logger.info("Ensuring Iceberg schema exists: %s", namespace_sql)
    spark.sql(namespace_sql)
    create_sql = iceberg_create_table_sql(table, schema_to_column_ddl(df.schema))
    if (load_strategy or "overwrite").strip().lower() == "overwrite":
        drop_sql = iceberg_drop_table_sql(table)
        logger.info("Dropping Iceberg table so clock columns stay string: %s", drop_sql)
        spark.sql(drop_sql)
        logger.info("Creating Iceberg table:\n%s", create_sql)
        spark.sql(create_sql)
        df.write.format("iceberg").mode("overwrite").saveAsTable(table)
        return
    logger.info("Ensuring Iceberg table exists:\n%s", create_sql)
    spark.sql(create_sql)
    table_df = spark.table(table)
    df_types = [(field.name, field.dataType.simpleString()) for field in df.schema.fields]
    table_types = [
        (field.name, field.dataType.simpleString()) for field in table_df.schema.fields
    ]
    if should_replace_iceberg_schema(load_strategy, df_types, table_types):
        logger.info("Replacing Iceberg schema for %s (load_strategy=%s)", table, load_strategy)
        spark.sql(iceberg_drop_table_sql(table))
        spark.sql(create_sql)
        df.write.format("iceberg").mode("overwrite").option(
            "overwrite-schema", "true"
        ).saveAsTable(table)
        return
    df.select(*table_df.columns).write.format("iceberg").mode(load_strategy).insertInto(table)


def iceberg_history_delete_sql(table: str, year: int, month: int) -> str:
    """DELETE rows for one reporting period before append (idempotent re-run)."""
    iceberg_ident_parts(table)
    return (
        "DELETE FROM %s WHERE %s = %s AND %s = %s"
        % (table, REPORTING_YEAR_COL, int(year), REPORTING_MONTH_COL, int(month))
    )


def schema_to_history_column_ddl(schema) -> str:
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    names = {name.lower() for name, _dtype in columns}
    if REPORTING_YEAR_COL.lower() not in names:
        columns.append((REPORTING_YEAR_COL, "int"))
    if REPORTING_MONTH_COL.lower() not in names:
        columns.append((REPORTING_MONTH_COL, "int"))
    return iceberg_column_ddl(columns)


def with_reporting_period(df, year: int, month: int):
    from pyspark.sql.functions import lit

    year_i, month_i = int(year), int(month)
    if month_i < 1 or month_i > 12:
        raise ValueError("reporting_month must be 1-12, got %s" % month_i)
    out = df
    if REPORTING_YEAR_COL not in out.columns:
        out = out.withColumn(REPORTING_YEAR_COL, lit(year_i).cast("int"))
    if REPORTING_MONTH_COL not in out.columns:
        out = out.withColumn(REPORTING_MONTH_COL, lit(month_i).cast("int"))
    return out


def write_iceberg_month_history(
    spark, df, table: str, year: int, month: int
) -> None:
    """Create history if missing, delete the period, then append. Never DROP history."""
    if not table:
        raise ValueError("history table is required")
    year_i, month_i = int(year), int(month)
    history_df = with_reporting_period(df, year_i, month_i)

    namespace_sql = iceberg_create_namespace_sql(table)
    logger.info("Ensuring Iceberg schema exists: %s", namespace_sql)
    spark.sql(namespace_sql)
    create_sql = iceberg_create_table_sql(
        table, schema_to_history_column_ddl(history_df.schema)
    )
    logger.info("Ensuring Iceberg history table exists:\n%s", create_sql)
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
    logger.info("Replacing history period: %s", delete_sql)
    spark.sql(delete_sql)

    logger.info(
        "Appending period %s-%02d to history %s", year_i, month_i, table
    )
    history_df.select(*table_cols).write.format("iceberg").mode("append").insertInto(
        table
    )


def load_onsite_employee(
    spark,
    excel_s3_path: str,
    table_path: str,
    load_strategy: str,
    history_table_path: str = None,
    reporting_year: int = None,
    reporting_month: int = None,
) -> None:
    excel_paths = list_excel_s3_paths(spark, excel_s3_path)
    logger.info("Found %s landing workbook(s)", len(excel_paths))
    pandas_df = read_excel_workbooks(spark, excel_paths)
    rows = onsite_spark_rows(pandas_df)
    assert_onsite_clocks_landed(rows)
    df = spark.createDataFrame(rows, schema=onsite_iceberg_schema())
    write_iceberg_table(spark, df, table_path, load_strategy)
    logger.info("Data successfully written to %s", table_path)
    if history_table_path:
        if reporting_year is None or reporting_month is None:
            reporting_year, reporting_month = current_landing_period()
        write_iceberg_month_history(
            spark, df, history_table_path, reporting_year, reporting_month
        )
        logger.info(
            "History period %s-%02d written to %s",
            reporting_year,
            reporting_month,
            history_table_path,
        )


def main(
    spark,
    excel_s3_path: str,
    table_path: str,
    load_strategy: str,
    history_table_path: str = None,
    reporting_year: int = None,
    reporting_month: int = None,
) -> None:
    logger.info("---- Starting Main() -----")
    load_onsite_employee(
        spark,
        excel_s3_path,
        table_path,
        load_strategy,
        history_table_path=history_table_path,
        reporting_year=reporting_year,
        reporting_month=reporting_month,
    )
    logger.info("Session is created and data is loaded.")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark_app_name = sys.argv[1]

    spark = (
        SparkSession.builder.appName(spark_app_name)
        .config("spark.task.maxFailures", "1")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.default.parallelism", "20")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    hadoop_custom_arguments = json.loads(sc.getConf().get("spark.hadoop.custom.arguments"))

    spark_job_args = hadoop_custom_arguments["spark_job_args"]
    cluster_details_spark_args = hadoop_custom_arguments["cluster_details_spark_args"]

    python_dependencies_base_paths = spark_job_args.get("python_dependencies_base_paths", "")
    import_generic_libs(spark, python_dependencies_base_paths)

    reporting_year, reporting_month = resolve_reporting_period(spark_job_args)
    excel_s3_path = resolve_monthly_excel_path(
        spark_job_args.get("excel_s3_path", DEFAULT_ONSITE_EXCEL_PATH),
        reporting_year,
        reporting_month,
    )
    logger.info("Resolved Excel landing path: %s", excel_s3_path)
    table_path = spark_job_args.get("output_warehouse_fq_table", DEFAULT_ONSITE_TABLE)
    history_table_path = spark_job_args.get(
        "output_warehouse_fq_history_table", DEFAULT_ONSITE_HISTORY_TABLE
    )
    load_strategy = spark_job_args.get("output_wh_table_load_strategy", "overwrite")

    try:
        main(
            spark,
            excel_s3_path,
            table_path,
            load_strategy,
            history_table_path=history_table_path,
            reporting_year=reporting_year,
            reporting_month=reporting_month,
        )
    except Exception as exc:
        logger.error(exc)
        raise Exception("! Something went wrong with this job") from exc
    finally:
        logger.info("Stopping...")
        spark.stop()
