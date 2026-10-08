"""Replace-month Iceberg history writer (delete year/month, then append).

Companion *_history tables keep prior months. Never DROP or overwrite-schema
a history table — that would wipe older periods.
"""

from __future__ import annotations

import logging
from typing import List, Sequence, Tuple

logger = logging.getLogger(__name__)

REPORTING_YEAR_COL = "reporting_year"
REPORTING_MONTH_COL = "reporting_month"
HISTORY_SUFFIX = "_history"


def default_history_table_name(current_table: str) -> str:
    """Derive catalog.schema.table_history from a fully qualified current table."""
    trimmed = (current_table or "").strip()
    if not trimmed:
        raise ValueError("current_table is required")
    if trimmed.endswith(HISTORY_SUFFIX):
        return trimmed
    return trimmed + HISTORY_SUFFIX


def iceberg_ident_parts(table: str) -> Tuple[str, str, str]:
    parts = [part.strip() for part in table.split(".") if part.strip()]
    if len(parts) < 3:
        raise ValueError(
            "Iceberg table must be catalog.schema.table, got: %s" % table
        )
    return parts[0], parts[1], parts[2]


def iceberg_namespace(table: str) -> str:
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "%s.%s" % (catalog, schema)


def iceberg_schema_location(table: str) -> str:
    catalog, schema, _table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s" % (catalog, schema)


def iceberg_table_location(table: str) -> str:
    catalog, schema, table_name = iceberg_ident_parts(table)
    return "s3a://%s/%s/%s" % (catalog, schema, table_name)


def iceberg_column_ddl(columns: Sequence[Tuple[str, str]]) -> str:
    return ",\n  ".join("`%s` %s" % (name, dtype) for name, dtype in columns)


def iceberg_create_namespace_sql(table: str) -> str:
    return "CREATE NAMESPACE IF NOT EXISTS %s LOCATION '%s'" % (
        iceberg_namespace(table),
        iceberg_schema_location(table),
    )


def iceberg_create_table_sql(table: str, column_ddl: str) -> str:
    return (
        "CREATE TABLE IF NOT EXISTS %s (\n  %s\n)\nUSING iceberg\nLOCATION '%s'"
        % (table, column_ddl, iceberg_table_location(table))
    )


def iceberg_history_delete_sql(table: str, year: int, month: int) -> str:
    """DELETE rows for one reporting period before append (idempotent re-run)."""
    iceberg_ident_parts(table)
    return (
        "DELETE FROM %s WHERE %s = %s AND %s = %s"
        % (
            table,
            REPORTING_YEAR_COL,
            int(year),
            REPORTING_MONTH_COL,
            int(month),
        )
    )


def iceberg_table_exists_sql(table: str) -> str:
    """Spark SQL probe used when catalog.tableExists is unavailable."""
    iceberg_ident_parts(table)
    return "SELECT 1 FROM %s LIMIT 1" % table


def history_column_pairs_from_schema(schema) -> List[Tuple[str, str]]:
    """DataFrame columns plus reporting_year/month ints when missing."""
    columns = [
        (field.name, field.dataType.simpleString()) for field in schema.fields
    ]
    names = {name.lower() for name, _dtype in columns}
    if REPORTING_YEAR_COL.lower() not in names:
        columns.append((REPORTING_YEAR_COL, "int"))
    if REPORTING_MONTH_COL.lower() not in names:
        columns.append((REPORTING_MONTH_COL, "int"))
    return columns


def schema_to_history_column_ddl(schema) -> str:
    return iceberg_column_ddl(history_column_pairs_from_schema(schema))


def with_reporting_period(df, year: int, month: int):
    """Attach reporting_year / reporting_month to a Spark DataFrame."""
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


def _history_table_exists(spark, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except Exception:
        return False


def assert_history_schema_compatible(spark, df, table: str) -> None:
    """Refuse silent schema drift that would null or mis-align history appends."""
    if not _history_table_exists(spark, table):
        return
    table_cols = spark.table(table).columns
    df_cols = list(df.columns)
    missing = [name for name in table_cols if name not in df_cols]
    extra = [name for name in df_cols if name not in table_cols]
    if missing or extra:
        raise ValueError(
            "History table %s schema drift; refusing recreate/overwrite. "
            "extra=%s missing=%s. Add columns manually or migrate the history "
            "table without DROP."
            % (table, extra, missing)
        )


def write_iceberg_month_history(
    spark, df, table: str, year: int, month: int
) -> None:
    """Create history table if missing, delete the period, then append rows.

    Never DROP or overwrite-schema the history table.
    """
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

    assert_history_schema_compatible(spark, history_df, table)

    delete_sql = iceberg_history_delete_sql(table, year_i, month_i)
    logger.info("Replacing history period: %s", delete_sql)
    spark.sql(delete_sql)

    table_cols = spark.table(table).columns
    aligned = history_df.select(*table_cols)
    logger.info(
        "Appending period %s-%02d to %s",
        year_i,
        month_i,
        table,
    )
    aligned.write.format("iceberg").mode("append").insertInto(table)
