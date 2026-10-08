"""Resolve Hive metastore URIs and Trino connections from Airflow GLOBALS."""

from typing import Any, Mapping, Optional, Sequence

# This Airflow env keys the warehouse catalog as datawarehouse (see working emp.py).
# Older clusters used datalake. Employee Spark jobs still name the Iceberg catalog
# dataproducts.
DATAPRODUCTS_METASTORE_ALIASES: Sequence[str] = (
    "dataproducts",
    "datalake",
    "datawarehouse",
    "transformed",
)
DATAPRODUCTS_TRINO_CONNECTION_ALIASES: Sequence[str] = (
    "datalake",
    "datawarehouse",
    "dataproducts",
)


def resolve_metastore_uri(global_vars: Mapping[str, Any], catalog: str) -> str:
    """Return the Hive metastore thrift URI for a Spark catalog."""
    metastores = global_vars.get("metastores") or {}
    if not isinstance(metastores, Mapping):
        metastores = {}

    keys: Sequence[str] = (
        DATAPRODUCTS_METASTORE_ALIASES if catalog == "dataproducts" else (catalog,)
    )
    for key in keys:
        uri = metastores.get(key)
        if uri:
            if key != catalog:
                print(
                    "WARNING: GLOBALS.metastores[%r] missing; "
                    "using metastores[%r] for Spark catalog %r"
                    % (catalog, key, catalog)
                )
            return str(uri)

    available = sorted(str(k) for k in metastores.keys())
    raise KeyError(
        "GLOBALS.metastores has no URI for catalog %r (tried %s). Available keys: %s"
        % (catalog, list(keys), available)
    )


def resolve_trino_connection_id(
    global_vars: Mapping[str, Any], catalog: str
) -> Optional[str]:
    """Return the Airflow Trino connection id for a catalog."""
    connections = global_vars.get("trino_airflow_connections") or {}
    if not isinstance(connections, Mapping):
        connections = {}

    keys: Sequence[str] = (
        DATAPRODUCTS_TRINO_CONNECTION_ALIASES if catalog == "dataproducts" else (catalog,)
    )
    for key in keys:
        conn_id = connections.get(key)
        if conn_id:
            return conn_id
    return None
