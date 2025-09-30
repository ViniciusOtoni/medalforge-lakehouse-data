"""
Fixtures compartilhadas:
- Spark local para DataFrames
- Contratos de exemplo (CSV/TXT)
- Factory para criar módulos dinâmicos (make_module)
- STUBS para módulos Databricks (labs.dqx e sdk), evitando ModuleNotFoundError em CI
"""

from __future__ import annotations
import pytest
import json
import sys
import types
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# STUBS Databricks (labs.dqx / sdk) — evitam ModuleNotFoundError durante imports
# ---------------------------------------------------------------------------
def _install_databricks_stubs() -> None:
    """
    Propósito: criar módulos mínimos 'databricks.labs.dqx.engine' e 'databricks.sdk'
    para permitir import dos drivers/stages em ambiente sem Databricks instalado.
    Os testes podem monkeypatchar DQEngine/WorkspaceClient normalmente.
    """
    def ensure_module(name: str) -> types.ModuleType:
        if name in sys.modules:
            return sys.modules[name]
        mod = types.ModuleType(name)
        sys.modules[name] = mod
        return mod

    # Pacotes base
    m_dbx = ensure_module("databricks")
    m_labs = ensure_module("databricks.labs"); m_labs.__path__ = []
    m_dqx = ensure_module("databricks.labs.dqx"); m_dqx.__path__ = []
    m_engine = ensure_module("databricks.labs.dqx.engine")
    m_sdk = ensure_module("databricks.sdk")

    # Stubs básicos
    class _StubWorkspaceClient:
        def __init__(self, *a, **k):
            pass

    class _StubDQEngine:
        def __init__(self, ws_client, *a, **k):
            self._ws = ws_client
        def apply_checks_by_metadata_and_split(self, df, checks, globs):
            # Split inócuo: tudo válido; quarentena vazia
            return df, df.limit(0)

    # Atribuições
    m_engine.DQEngine = _StubDQEngine
    m_sdk.WorkspaceClient = _StubWorkspaceClient


# Instala stubs imediatamente (antes de qualquer import dos módulos alvo)
_install_databricks_stubs()


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """
    SparkSession local para testes.
    """
    spark = (
        SparkSession.builder
        .appName("onedata-tests")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# Contratos de exemplo (Bronze) 
# ---------------------------------------------------------------------------
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
    que o código possa importá-lo por nome (string), como no runtime real.

    Uso:
        mod = make_module("custom_example", {"fn": callable})
    """
    created: list[str] = []

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
