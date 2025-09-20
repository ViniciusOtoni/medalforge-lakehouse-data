"""
Testes unitários do driver DQX.

Intuito
-------
- Validar a coleta/validação de checks (_collect_checks).
- Exercitar apply_checks_split com um DQEngine mockado (sem chamar DQX real).
"""

from types import SimpleNamespace
from typing import Any, Dict, List, Tuple
import pytest
from pyspark.sql import DataFrame
from silver.dqx import driver as dqx_driver


def test_collect_checks_valid_ok():
    """
    _collect_checks: aceita 'checks' e 'custom' e preserva estrutura.
    """
    cfg = {
        "checks": [
            {"name": "c1", "criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        ],
        "custom": [
            {"name": "c2", "criticality": "warning", "check": {"function": "is_unique", "arguments": {"columns": ["id"]}}}
        ],
    }
    out = dqx_driver._collect_checks(cfg)
    assert isinstance(out, list) and len(out) == 2
    assert out[0]["check"]["function"] == "is_not_null"


def test_collect_checks_invalid_item_raises():
    """
    _collect_checks: item sem 'check.function' deve levantar ValueError.
    """
    cfg = {"checks": [{"name": "bad", "criticality": "error", "check": {}}]}
    with pytest.raises(ValueError):
        dqx_driver._collect_checks(cfg)


def test_apply_checks_split_with_mock(monkeypatch, spark, sample_df):
    """
    apply_checks_split: usa DQEngine mockado para retornar (valid, quarantine) previsíveis.

    Estratégia
    ----------
    - Mocka DQEngine(...) e seu método apply_checks_by_metadata_and_split para
      retornar (sample_df, sample_df.limit(0)).
    - Verifica que a função repassa o que o mock produz.
    """

    class _DummyEngine:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def apply_checks_by_metadata_and_split(
            self, df: DataFrame, checks: List[Dict[str, Any]], _globals: Dict[str, Any]
        ) -> Tuple[DataFrame, DataFrame]:
            return df, df.limit(0)

    monkeypatch.setattr(dqx_driver, "DQEngine", lambda *_a, **_k: _DummyEngine())
    valid, quar = dqx_driver.apply_checks_split(sample_df, {"checks": [], "custom": []})
    assert isinstance(valid, DataFrame) and isinstance(quar, DataFrame)
    assert quar.count() == 0
