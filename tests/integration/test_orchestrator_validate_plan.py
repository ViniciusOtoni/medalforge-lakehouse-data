from bronze.models import BaseLocations, RunnerConfig
from bronze.orchestrator import BronzeOrchestrator


def test_orchestrator_validate_and_plan_only(spark, contract_json_csv):
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
