import csv
import sys
import json
import os
from typing import Iterable, List, Mapping, Sequence, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkFiles
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%y/%m/%d %H:%M:%S')

logger = logging.getLogger()
# ***** ***** ***** ***** ***** ***** Job Properties ***** ***** ***** ***** ***** *****

DEFAULT_HR_CSV_PATH = (
    "s3a://workflows/employee_data/dependencies/resources/"
    "employee_hr_master - employee_master.csv"
)
DEFAULT_HR_TABLE = "raw.employee.employee_hr_data"

# Warehouse names. Source CSV uses Employee ID as the real emp id (no badge emp_code).
EMPLOYEE_HR_COLUMNS = [
    "employee_id",
    "employee_name",
    "business_email",
    "department",
    "job_role",
    "company",
    "line_manager",
    "joining_date",
    "current_status",
    "gender",
    "nationality",
]

HR_HEADER_MAP = {
    "employee id": "employee_id",
    "full name": "employee_name",
    "email": "business_email",
    "department": "department",
    "designation": "job_role",
    "business unit": "company",
    "direct manager": "line_manager",
    "date of joining": "joining_date",
    "status": "current_status",
    "gender": "gender",
    "nationality": "nationality",
}

_EMPTY_CELL = {"", "nan", "none", "nat", "null", "-", "--"}


def _normalize_header(name: object) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _cell(value: object) -> str:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _EMPTY_CELL:
        return None
    return text


def parse_hr_csv_lines(lines: Iterable[str]) -> List[dict]:
    """Map the current HR export (Employee ID, Full Name, Email, Department)."""
    rows = list(csv.reader(lines))
    header_idx = None
    header_keys = None
    for index, row in enumerate(rows):
        keys = [_normalize_header(cell) for cell in row]
        if "employee id" in keys:
            header_idx = index
            header_keys = keys
            break
    if header_keys is None:
        raise ValueError("HR CSV is missing an Employee ID header row")

    records = []
    for row in rows[header_idx + 1 :]:
        record = {column: None for column in EMPLOYEE_HR_COLUMNS}
        for index, key in enumerate(header_keys):
            dest = HR_HEADER_MAP.get(key)
            if dest is None or dest not in record:
                continue
            value = row[index] if index < len(row) else None
            record[dest] = _cell(value)
        if record["employee_id"] is None and record["employee_name"] is None:
            continue
        records.append(record)
    return records


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


def iceberg_create_table_sql(table: str, column_ddl: str) -> str:
    """CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION s3a://..."""
    return (
        "CREATE TABLE IF NOT EXISTS %s (\n  %s\n)\nUSING iceberg\nLOCATION '%s'"
        % (table, column_ddl, iceberg_table_location(table))
    )


def schema_to_column_ddl(schema: StructType) -> str:
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    return iceberg_column_ddl(columns)


def employee_hr_schema() -> StructType:
    return StructType(
        [StructField(column, StringType(), True) for column in EMPLOYEE_HR_COLUMNS]
    )


def write_iceberg_overwrite(spark, df, table: str) -> None:
    """Create the Iceberg namespace/table if missing, then overwrite rows.

    insertInto resolves columns by position. Align by name so schema drift
    (new columns, reordered Hive fields) does not write nulls into the
    wrong columns.
    """
    namespace_sql = iceberg_create_namespace_sql(table)
    logger.info("Ensuring Iceberg schema exists: %s", namespace_sql)
    spark.sql(namespace_sql)
    create_sql = iceberg_create_table_sql(table, schema_to_column_ddl(df.schema))
    logger.info("Ensuring Iceberg table exists:\n%s", create_sql)
    spark.sql(create_sql)
    table_cols = spark.table(table).columns
    extra = [name for name in df.columns if name not in table_cols]
    missing = [name for name in table_cols if name not in df.columns]
    if extra or missing:
        logger.warning(
            "Iceberg schema drift for %s extra=%s missing=%s; overwriting schema",
            table,
            extra,
            missing,
        )
        df.write.format("iceberg").mode("overwrite").option(
            "overwrite-schema", "true"
        ).saveAsTable(table)
        return
    df.select(*table_cols).write.format("iceberg").mode("overwrite").insertInto(table)


# Import Generic Dependencies here
def import_generic_libs():
    logger.info('> current_dir: %s' % str(os.listdir(os.path.dirname(os.path.abspath(__file__)))))
    for python_dependencies_base_path in python_dependencies_base_paths.split(","):
        python_dependencies_base_path = str(python_dependencies_base_path).strip()
        logger.info('> Listing dependencies: %s' % str(os.listdir(os.path.abspath(python_dependencies_base_path))))
        for py_file in os.listdir(os.path.abspath(python_dependencies_base_path)):
            if py_file == '__init__.py' or py_file == '__pycache__':
                continue
            python_dep_file_path = SparkFiles.get(os.path.join(python_dependencies_base_path, py_file))
            spark.sparkContext.addPyFile(python_dep_file_path)
        # Driver might need some deps (if used --pyFiles this is not needed!!!)
        sys.path.append(python_dependencies_base_path)
        
def load_hr_csv_records(spark, csv_path: str) -> List[Mapping[str, str]]:
    """Read the HR CSV from S3/HDFS as text and map Employee ID as employee_id."""
    lines = [row[0] for row in spark.read.text(csv_path).collect()]
    records = parse_hr_csv_lines(lines)
    if not records:
        raise ValueError("HR CSV %s is empty after parsing" % csv_path)
    return records


# Function to load employee metadata
def load_emp_metadata(spark, csv_path, table_path):
    schema = employee_hr_schema()
    try:
        records = load_hr_csv_records(spark, csv_path)
        df = spark.createDataFrame(records, schema=schema)
        write_iceberg_overwrite(spark, df, table_path)
        logger.info("Data successfully written to the table.")
    except Exception as e:
        logger.error(f"Error loading employee metadata: {e}")
        raise

# Main function
def main(spark,csv_path,table_path):
    try:
        logger.info("---- Starting Main() -----")
        load_emp_metadata(spark, csv_path, table_path)
        logger.info("Session is created and data is loaded.")
        spark.stop()
    except Exception as e:
        logger.error(e)
        sc.stop() 
        raise Exception('! Something went wrong with this job')
    finally:
        logger.info('Stopping...')
        sc.stop()


if __name__ == '__main__':

    # Read arguments
    spark_app_name = sys.argv[1]
    
    # Initialize Spark session
    spark = SparkSession.builder.appName(spark_app_name) \
        .config("spark.task.maxFailures", "1") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    hadoop_custom_arguments = json.loads(sc.getConf().get('spark.hadoop.custom.arguments'))

    # Extract job arguments
    spark_job_args = hadoop_custom_arguments["spark_job_args"]
    cluster_details_spark_args = hadoop_custom_arguments["cluster_details_spark_args"]

    # Extract configurations
    minio_s3_credentials = cluster_details_spark_args["s3_credentials"]
    warehouse_catalog = cluster_details_spark_args["catalog"]
    catalog_minio_bucket = cluster_details_spark_args["catalog_minio_bucket"]
    python_dependencies_base_paths = spark_job_args.get("python_dependencies_base_paths", "")

    # Import generic libraries
    import_generic_libs()


    # Paths for data loading
    csv_path = spark_job_args.get("csv_s3_path", DEFAULT_HR_CSV_PATH)
    table_path = spark_job_args.get("output_warehouse_fq_table", DEFAULT_HR_TABLE)

    # Run main function
    main(spark,csv_path,table_path)
    exit()