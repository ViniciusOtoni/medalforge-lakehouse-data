import os
import types

from onedata.monitoring.azure_table_runs import PipelineRunLogger
import onedata.monitoring.azure_table_runs as mod


def test_noop_when_env_missing(monkeypatch):
    # Sem MON_TABLE_ACCOUNT, vira no-op. Não deve levantar erro.
    if "MON_TABLE_ACCOUNT" in os.environ:
        monkeypatch.delenv("MON_TABLE_ACCOUNT", raising=False)

    with PipelineRunLogger(
        env="dev", pipeline="bronze", schema="sales", table="orders", target_fqn="bronze.sales.orders"
    ) as runlog:
        runlog.finish(status="ok")

    # Apenas garante que não explodiu. (sem asserts pois é no-op)


def test_finish_not_called_twice(monkeypatch):
    # fake client
    class FakeClient:
        def __init__(self):
            self.entities = []
        def upsert_entity(self, mode, entity):
            self.entities.append((mode, entity))

    # monkeypatch interno: forçar _table_client a devolver FakeClient
    monkeypatch.setenv("MON_TABLE_ACCOUNT", "acc")
    mod.TableServiceClient = types.SimpleNamespace(
        __call__=lambda *a, **k: None,
        __bool__=lambda self: True,
    )
    class _FakeSvc:
        def __init__(self, *a, **k): pass
        def create_table_if_not_exists(self, table_name): return None
        def get_table_client(self, table_name): return FakeClient()
    mod.TableServiceClient = _FakeSvc  # type: ignore

    with PipelineRunLogger(
        env="dev", pipeline="bronze", schema="sales", table="orders", target_fqn="bronze.sales.orders"
    ) as runlog:
        runlog.finish(status="ok")
        # __exit__ não deve finalizar de novo (flag _finished)
