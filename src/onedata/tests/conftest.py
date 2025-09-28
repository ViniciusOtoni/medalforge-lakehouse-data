"""
Fixtures compartilhadas:
- Spark local para DataFrames
- DataFrame de exemplo (id, created_at, amount)
"""

from __future__ import annotations
import pytest
import json
from pathlib import Path
from pyspark.sql import SparkSession



@pytest.fixture(scope="session")
def spark():
    """
    SparkSession local para testes.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("bronze-tests")
        .config("spark.ui.enabled", "false")
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