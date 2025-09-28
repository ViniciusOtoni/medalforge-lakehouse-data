from onedata.bronze.models import RunnerConfig, BaseLocations, Paths, IngestionPlan


def test_runner_config_defaults():
    cfg = RunnerConfig()
    assert isinstance(cfg.mode, str)
    assert cfg.env in {"dev", "hml", "prd"} or cfg.env == "dev"


def test_base_locations():
    base = BaseLocations(raw_root="abfss://raw@acc.dfs.core.windows.net",
                         bronze_root="abfss://bronze@acc.dfs.core.windows.net")
    assert base.raw_root.startswith("abfss://")
    assert base.bronze_root.startswith("abfss://")


def test_paths_model():
    p = Paths(source_directory="/raw", table_location="/bronze/t", checkpoint_location="/bronze/_cp/t")
    assert p.source_directory and p.table_location and p.checkpoint_location


def test_ingestion_plan_as_dict():
    plan = IngestionPlan(
        contract_fqn="bronze.sales.orders",
        data_format="csv",
        reader_options={"header": "true", "delimiter": ","},
        partitions=["created_at", "ingestion_date"],
        schema_json="{}",
        paths={"source_directory": "/raw", "checkpointLocation": "/cp", "table_location": "/tbl"},
        trigger="availableNow",
        include_existing_files=True,
    )
    d = plan.as_dict()
    assert d["contract_fqn"] == "bronze.sales.orders"
    assert d["format"] == "csv"
    assert d["includeExistingFiles"] is True
