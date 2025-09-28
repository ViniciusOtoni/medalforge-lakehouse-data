import bronze.orchestrator as orch_mod
from bronze.models import BaseLocations, RunnerConfig
from bronze.orchestrator import BronzeOrchestrator

def test_orchestrator_validate_and_plan_only(monkeypatch, spark, contract_json_csv):
    monkeypatch.setattr(orch_mod.TableManager, "ensure_external_table", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod.TableManager, "ensure_schema", lambda *a, **k: None)

    base = BaseLocations(
        raw_root="abfss://raw@acc.dfs.core.windows.net",
        bronze_root="abfss://bronze@acc.dfs.core.windows.net",
    )
    cfg = RunnerConfig(mode="validate+plan", include_existing_files=True, env="dev")
    orch = BronzeOrchestrator(spark=spark, base_locations=base, env="dev")

    outputs = orch.run(contract_json=contract_json_csv, configRunner=cfg)

    assert "validated" in outputs["status"]
    assert "planned" in outputs["status"]
    plan = outputs["details"]["plan"]
    assert plan["format"] == "csv"
    assert plan["checkpointLocation"].startswith("abfss://bronze")
    assert plan["source_directory"].startswith("abfss://raw")
