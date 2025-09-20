"""
Testes unitários de helpers de escrita/merge Delta (utils.merge).

Intuito
-------
- Validar construção de LOCATION externo.
- Exercitar caminhos de criação em branco e early-return no df vazio (merge_upsert)
  com Spark e DF mockados – sem IO real.
"""

from types import SimpleNamespace
from typing import Any, List, Optional
import pytest
from silver.utils import merge as M


def test_external_path_for_ok():
    """
    _external_path_for: monta <base>/<schema>/<table>.
    """
    path = M._external_path_for("silver.sales.orders", "abfss://silver@acct.dfs.core.windows.net/base")
    assert path.endswith("/sales/orders")


def test_merge_upsert_with_mocks(monkeypatch):
    """
    merge_upsert: garante tabela, evita MERGE com df vazio e invoca MERGE quando há linhas.
    """

    # --- Spark mock (no escopo do módulo utils.merge)
    class _SparkCatalog:
        def __init__(self) -> None:
            self.exists = False

        def tableExists(self, dbName=None, tableName=None) -> bool:
            # usa flag para simular existência
            return self.exists

    class _Spark:
        def __init__(self):
            self.catalog = _SparkCatalog()
            self.sql_calls: List[str] = []

        def sql(self, sqlQuery: str) -> None:
            self.sql_calls.append(sqlQuery)

    spark_mock = _Spark()
    monkeypatch.setattr(M, "spark", spark_mock)

    # --- DataFrame mock compatível com o que o módulo usa
    class _Writer:
        def __init__(self): self._partitions: Optional[List[str]] = None
        def format(self, *_): return self
        def partitionBy(self, *cols: str): self._partitions = list(cols); return self
        def mode(self, *_): return self
        def save(self, *_): return None
        def saveAsTable(self, *_): return None

    class _DF:
        def __init__(self, cols: List[str], has_rows: bool):
            self._cols = cols
            self._has_rows = has_rows
        @property
        def columns(self) -> List[str]: return self._cols
        def limit(self, *_): return self
        @property
        def write(self): return _Writer()
        def createOrReplaceTempView(self, name: str): pass
        def head(self, n=1): return [SimpleNamespace()] if self._has_rows else []
        # usado em append_external
        def write(self): return _Writer()

    # 1) DF vazio ⇒ não faz MERGE
    df_empty = _DF(["id", "v"], has_rows=False)
    M.merge_upsert(
        df=df_empty,
        tgt_fqn="silver.sales.t",
        keys=["id"],
        zorder_by=None,
        partition_by=["dt"],
        external_base="abfss://silver@acct.dfs.core.windows.net/base",
    )
    # deve ter rodado CREATE TABLE LOCATION (pelo menos 1 SQL)
    assert any("CREATE TABLE" in s for s in spark_mock.sql_calls)
    calls_before = len(spark_mock.sql_calls)

    # 2) DF com linha ⇒ deve disparar MERGE
    df_rows = _DF(["id", "v"], has_rows=True)
    spark_mock.sql_calls.clear()
    M.merge_upsert(
        df=df_rows,
        tgt_fqn="silver.sales.t",
        keys=["id"],
        zorder_by=["id"],
        partition_by=["dt"],
        external_base="abfss://silver@acct.dfs.core.windows.net/base",
    )
    assert any("MERGE INTO silver.sales.t t" in s for s in spark_mock.sql_calls)
    assert any("OPTIMIZE silver.sales.t ZORDER BY" in s for s in spark_mock.sql_calls)
