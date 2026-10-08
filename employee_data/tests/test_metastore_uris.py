import sys
from pathlib import Path

FLOWS_DIR = Path(__file__).resolve().parents[1] / "flows"
sys.path.insert(0, str(FLOWS_DIR))

from metastore_uris import (  # noqa: E402
    resolve_metastore_uri,
    resolve_trino_connection_id,
)


def test_dataproducts_uses_explicit_metastore_uri() -> None:
    uri = resolve_metastore_uri(
        {"metastores": {"dataproducts": "thrift://dp:9083", "datalake": "thrift://dl:9083"}},
        "dataproducts",
    )
    assert uri == "thrift://dp:9083"


def test_dataproducts_falls_back_to_datalake_alias() -> None:
    uri = resolve_metastore_uri(
        {"metastores": {"raw": "thrift://raw:9083", "datalake": "thrift://dl:9083"}},
        "dataproducts",
    )
    assert uri == "thrift://dl:9083"


def test_dataproducts_falls_back_to_datawarehouse_alias() -> None:
    uri = resolve_metastore_uri(
        {
            "metastores": {
                "raw": "thrift://raw:9083",
                "transformed": "thrift://tf:9083",
                "datawarehouse": "thrift://dw:9083",
            }
        },
        "dataproducts",
    )
    assert uri == "thrift://dw:9083"


def test_dataproducts_falls_back_to_transformed_when_warehouse_keys_missing() -> None:
    uri = resolve_metastore_uri(
        {"metastores": {"raw": "thrift://raw:9083", "transformed": "thrift://tf:9083"}},
        "dataproducts",
    )
    assert uri == "thrift://tf:9083"


def test_raw_and_transformed_use_exact_keys() -> None:
    global_vars = {
        "metastores": {
            "raw": "thrift://raw:9083",
            "transformed": "thrift://tf:9083",
            "datalake": "thrift://dl:9083",
        }
    }
    assert resolve_metastore_uri(global_vars, "raw") == "thrift://raw:9083"
    assert resolve_metastore_uri(global_vars, "transformed") == "thrift://tf:9083"


def test_missing_all_metastore_aliases_lists_available_keys() -> None:
    try:
        resolve_metastore_uri({"metastores": {"raw": "thrift://raw:9083"}}, "dataproducts")
    except KeyError as exc:
        message = str(exc)
        assert "dataproducts" in message
        assert "datawarehouse" in message
        assert "raw" in message
    else:
        raise AssertionError("expected KeyError when no warehouse metastore URI exists")


def test_trino_connection_prefers_datalake_then_datawarehouse() -> None:
    assert (
        resolve_trino_connection_id(
            {"trino_airflow_connections": {"datalake": "trino-datalake"}},
            "dataproducts",
        )
        == "trino-datalake"
    )
    assert (
        resolve_trino_connection_id(
            {"trino_airflow_connections": {"datawarehouse": "trino-demo"}},
            "dataproducts",
        )
        == "trino-demo"
    )
