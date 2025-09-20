"""
Testes unitários dos modelos Pydantic do contrato Silver.

Intuito
-------
- Validar normalizações e regras em DQCheck/DQXCfg/SilverYaml.
- Garantir `target_fqn` e alias de schema.
"""

import pytest
from silver.framework.contract_model import (
    DQCheck, DQXCfg, TargetWriteCfg, TargetCfg, SilverYaml, Step
)


def test_dqcheck_aliases_and_arg_normalization():
    """
    DQCheck: deve normalizar function aliases e argumentos (column/columns, nulls_distinct).
    """
    raw = {
        "name": "uniq",
        "criticality": "error",
        "function": "unique",  # alias -> is_unique
        "arguments": {"column": "id", "nulls_distinct": "true"},
    }
    c = DQCheck(**raw)
    assert c.check.function == "is_unique"
    assert c.check.arguments["columns"] == ["id"]
    assert c.check.arguments["nulls_distinct"] is True


def test_silveryaml_version_guard_and_target_fqn():
    """
    SilverYaml: aceita versão 1.x e monta `target_fqn` corretamente.
    """
    y = SilverYaml(
        version="1.0",
        source={"bronze_table": "bronze.sales.t_raw"},
        target=TargetCfg(
            catalog="silver",
            schema="sales",
            table="t_clean",
            write=TargetWriteCfg(mode="merge", merge_keys=["id"]),
        ),
        dqx=DQXCfg(),
        etl={"standard": [Step(method="trim_columns", args={"columns": ["id"]})]},
        quarantine={"remediate": [], "sink": None},
        customs={"allow": False, "registry": [], "use_in": []},
    )
    assert y.target_fqn == "silver.sales.t_clean"


def test_silveryaml_version_unsupported_raises():
    """
    SilverYaml: versão não-1.x deve falhar.
    """
    with pytest.raises(ValueError):
        SilverYaml(
            version="2.0",
            source={"bronze_table": "bronze.sales.t"},
            target=TargetCfg(
                catalog="silver", schema="sales", table="t", write=TargetWriteCfg()
            ),
            dqx=DQXCfg(),
            etl={"standard": []},
            quarantine={"remediate": [], "sink": None},
            customs={"allow": False, "registry": [], "use_in": []},
        )
