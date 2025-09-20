"""
Funções auxiliares de main.py: _normalize_mode, _build_paths, _plan_dict.
"""

import json
import pytest

from bronze.main import _normalize_mode, _build_paths, _plan_dict
from bronze.managers.data_contract_manager import DataContractManager

def test_normalize_mode_variantes():
    """
    Diferentes combinações devem mapear corretamente para (validate, plan, ingest).
    """
   
    assert _normalize_mode("validate+plan") == (True, True, False)
    assert _normalize_mode("ingest") == (False, False, True)
    assert _normalize_mode("validate+ingest") == (True, False, True)


def test_build_paths_ok():
    """
    _build_paths deve compor source_directory, table_location e checkpointLocation.
    """
 
    out = _build_paths(catalog="bronze", schema="sales", table="orders", reprocess_label="r1")
    assert "source_directory" in out and "table_location" in out and "checkpointLocation" in out
    assert out["checkpointLocation"].endswith("_r1")


def test_plan_dict_schema_json_serializable(spark):
    """
    _plan_dict deve serializar schema_struct.jsonValue() ou cair no str(schema).
    """
    
   

    contract = {
        "schema": "sales",
        "table": "orders",
        "columns": [{"name": "id", "dtype": "string"}],
        "source": {"format": "csv", "options": {"header": True}},
    }
    mgr = DataContractManager(contract)
    payload = {
        "format": "csv",
        "reader_options": {"header": True},
        "partitions": ["ingestion_date"],
        "schema_struct": mgr.spark_schema_typed(),
    }
    paths = {"source_directory": "/raw", "checkpointLocation": "/chk", "table_location": "/loc"}
    plan = _plan_dict(mgr=mgr, payload=payload, paths=paths, include_existing=True)
    # deve ser um JSON imprimível
    json.dumps(plan)
