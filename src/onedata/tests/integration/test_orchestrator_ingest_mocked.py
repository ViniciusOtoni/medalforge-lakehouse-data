import bronze.orchestrator as orch_mod
from bronze.models import BaseLocations, RunnerConfig
from bronze.orchestrator import BronzeOrchestrator

class _FakeIngestor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        _FakeIngestor._last = self
    def ingest(self, include_existing_files=False):
        _FakeIngestor._ingest_called = True
        _FakeIngestor._ingest_include_existing = include_existing_files

def test_orchestrator_ingest_calls_factory(monkeypatch, spark, contract_json_csv):
    # Patch dos símbolos realmente usados pelo orquestrador
    monkeypatch.setattr(orch_mod.TableManager, "ensure_external_table", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod.TableManager, "ensure_schema", lambda *a, **k: None)
    monkeypatch.setitem(orch_mod.IngestorFactory._FORMAT_REGISTRY, "csv", _FakeIngestor)

    base = BaseLocations(
        raw_root="abfss://raw@acc.dfs.core.windows.net",
        bronze_root="abfss://bronze@acc.dfs.core.windows.net",
    )
    cfg = RunnerConfig(mode="validate+plan+ingest", include_existing_files=True, env="dev")
    orch = BronzeOrchestrator(spark=spark, base_locations=base, env="dev")

    outputs = orch.run(contract_json=contract_json_csv, configRunner=cfg)

    assert "ingested" in outputs["status"]
    assert getattr(_FakeIngestor, "_ingest_called", False) is True
    assert getattr(_FakeIngestor, "_ingest_include_existing", None) is True
    assert _FakeIngestor._last.kwargs["target_table"].endswith(".sales.orders")
