"""
Fixtures compartilhadas:
- Spark local para DataFrames
- DataFrame de exemplo (id, created_at, amount)
"""

from __future__ import annotations
import pytest
import json
import sys
import types
from pyspark.sql import SparkSession



@pytest.fixture(scope="session")
def spark():
    """
    SparkSession local para testes.
    """
    spark = (
        SparkSession.builder
        .appName("onedata-customs-unit")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture()
def sample_contract_csv():
    """
    Contrato mínimo CSV (NDJSON-like não, apenas csv) com duas colunas e partição.
    """
    return {
        "version": "1.0",
        "catalog": "bronze",
        "schema": "sales",
        "table": "orders",
        "columns": [
            {"name": "id", "dtype": "int", "comment": "PK"},
            {"name": "created_at", "dtype": "timestamp"},
        ],
        "partitions": ["created_at"],
        "source": {
            "format": "csv",
            "options": {"header": True, "delimiter": ","}
        }
    }


@pytest.fixture()
def sample_contract_txt():
    """
    Contrato TXT delimitado (mapeado para CSV).
    """
    return {
        "version": "1.0",
        "catalog": "bronze",
        "schema": "ops",
        "table": "logs",
        "columns": [
            {"name": "msg", "dtype": "string"},
            {"name": "ts", "dtype": "timestamp"},
        ],
        "partitions": [],
        "source": {
            "format": "txt",
            "options": {"delimiter": "|"}
        }
    }


@pytest.fixture()
def contract_json_csv(sample_contract_csv):
    return json.dumps(sample_contract_csv)

@pytest.fixture
def make_module():
    """
    Propósito: criar/injetar dinamicamente um módulo Python (em sys.modules) para
    que o loader/runner possa importá-lo por nome (string), como no runtime real.
    Uso:
        mod = make_module("custom_example", {"fn": callable})
    """
    created = []

    def _factory(name: str, attrs: dict):
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
        created.append(name)
        return mod

    yield _factory

    # limpeza
    for name in created:
        sys.modules.pop(name, None)
