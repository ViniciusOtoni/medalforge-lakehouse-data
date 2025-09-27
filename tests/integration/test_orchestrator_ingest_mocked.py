from bronze.models import BaseLocations, RunnerConfig
from bronze.orchestrator import BronzeOrchestrator
from bronze.ingestors.factory import IngestorFactory


class _FakeIngestor:
    """
    Fake que captura a chamada do orchestrator.ingest sem abrir stream real.
    """
    def __init__(self, **kwargs):
        # guardamos tudo pra inspeção
        self.kwargs = kwargs
        _FakeIngestor._last = self

    def ingest(self, include_existing_files=False):
        _FakeIngestor._ingest_called = True
        _FakeIngestor._ingest_include_existing = include_existing_files


def test_orchestrator_ingest_calls_factory(monkeypatch, spark, contract_json_csv):
    # Registrar fake na factory para todos os formatos
    monkeypatch.setitem(IngestorFactory._FORMAT_REGISTRY, "csv", _FakeIngestor)

    base = BaseLocations(
        raw_root="abfss://raw@acc.dfs.core.windows.net",
        bronze_root="abfss://bronze@acc.dfs.core.windows.net",
    )
    cfg = RunnerConfig(mode="validate+plan+ingest", include_existing_files=True, env="dev")
    orch = BronzeOrchestrator(spark=spark, base_locations=base, env="dev")

    outputs = orch.run(contract_json=contract_json_csv, configRunner=cfg)

    # Assertions de integração “leve”: ingest foi chamado
    assert "ingested" in outputs["status"]
    assert getattr(_FakeIngestor, "_ingest_called", False) is True
    assert getattr(_FakeIngestor, "_ingest_include_existing", None) is True
    # A fábrica recebeu target_table correto
    assert _FakeIngestor._last.kwargs["target_table"].endswith(".sales.orders")
