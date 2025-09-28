"""
DataContractManager: validações e helpers (sem tocar Delta/UC).
"""

import pytest
from bronze.managers.data_contract_manager import DataContractManager


def _mk_contract(fmt="csv", options=None):
    return {
        "schema": "sales",
        "table": "orders",
        "columns": [
            {"name": "id", "dtype": "string", "comment": "pk"},
            {"name": "created_at", "dtype": "string"},
        ],
        "partitions": ["created_at"],
        "source": {"format": fmt, "options": options or {}},
    }


def test_reader_kind_txt_mapeia_para_csv():
    """
    'txt' deve virar 'csv' em reader_kind.
    """
    
    mgr = DataContractManager(_mk_contract(fmt="txt", options={"delimiter": "|"}))
    assert mgr.reader_kind() == "csv"


def test_reader_options_txt_exige_delimiter():
    """
    reader_options() deve falhar quando 'txt' sem 'delimiter'.
    """
    
    with pytest.raises(ValueError):
        DataContractManager(_mk_contract(fmt="txt", options={})).reader_options()


def test_effective_partitions_inclui_ingestion_date():
    """
    effective_partitions deve garantir 'ingestion_date'.
    """
    
    mgr = DataContractManager(_mk_contract())
    assert "ingestion_date" in mgr.effective_partitions
