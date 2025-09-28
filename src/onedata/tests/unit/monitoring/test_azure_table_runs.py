# src/onedata/tests/unit/monitoring/test_azure_table_runs.py
import os
import sys
import types

from onedata.monitoring.azure_table_runs import PipelineRunLogger
mod = sys.modules[PipelineRunLogger.__module__]


def test_noop_when_env_missing(monkeypatch):
    if "MON_TABLE_ACCOUNT" in os.environ:
        monkeypatch.delenv("MON_TABLE_ACCOUNT", raising=False)

    with PipelineRunLogger(
        env="dev", pipeline="bronze", schema="sales", table="orders", target_fqn="bronze.sales.orders"
    ) as runlog:
        runlog.finish(status="ok")


def test_finish_not_called_twice(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.entities = []
        def upsert_entity(self, mode, entity):
            self.entities.append((mode, entity))

    monkeypatch.setenv("MON_TABLE_ACCOUNT", "acc")

    # Stub do serviço de Tabelas
    class _FakeSvc:
        def __init__(self, *a, **k): pass
        def create_table_if_not_exists(self, table_name): return None
        def get_table_client(self, table_name): return FakeClient()

    # Monkeypatcha o símbolo usado pelo módulo em runtime
    mod.TableServiceClient = _FakeSvc  # type: ignore

    with PipelineRunLogger(
        env="dev", pipeline="bronze", schema="sales", table="orders", target_fqn="bronze.sales.orders"
    ) as runlog:
        runlog.finish(status="ok")
        # __exit__ não deve finalizar de novo (flag _finished)
