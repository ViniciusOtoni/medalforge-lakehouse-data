"""
Módulo: test_silver
Finalidade: validar o contrato SilverYaml: regra de versão (1.x), extra='forbid',
e helper target_fqn.
"""

import pytest
from onedata.silver.domain.silver import SilverYaml
from onedata.silver.domain.target import TargetCfg, TargetWriteCfg
from onedata.silver.domain.dqx import DQXCfg
from onedata.silver.domain.etl import Step, QuarantineCfg, CustomsCfg


def _minimal_silver():
    target = TargetCfg(catalog="silver", schema="sales", table="orders", write=TargetWriteCfg())
    dqx = DQXCfg()  # defaults vazios
    etl = {"standard": [Step(method="cast_types")]}
    quarantine = QuarantineCfg()
    customs = CustomsCfg()
    return target, dqx, etl, quarantine, customs


def test_silveryaml_aceita_somente_versao_1x():
    """Propósito: garantir que apenas versões 1.x sejam aceitas."""
    target, dqx, etl, quarantine, customs = _minimal_silver()
    s = SilverYaml(
        version="1.0",
        source={"bronze_table": "bronze.sales.orders"},
        target=target,
        dqx=dqx,
        etl=etl,
        quarantine=quarantine,
        customs=customs,
    )
    assert s.version.startswith("1.")

    with pytest.raises(ValueError):
        SilverYaml(
            version="2.0",
            source={},
            target=target,
            dqx=dqx,
            etl=etl,
            quarantine=quarantine,
            customs=customs,
        )


def test_silveryaml_target_fqn_e_extra_forbid():
    """Propósito: validar target_fqn e que campos extras sejam rejeitados."""
    target, dqx, etl, quarantine, customs = _minimal_silver()
    s = SilverYaml(
        version="1.1",
        source={"bronze_table": "bronze.sales.orders"},
        target=target,
        dqx=dqx,
        etl=etl,
        quarantine=quarantine,
        customs=customs,
    )
    assert s.target_fqn == "silver.sales.orders"

    # extra='forbid' — tentar passar um campo desconhecido deve falhar
    with pytest.raises(Exception):
        SilverYaml(
            version="1.1",
            source={"bronze_table": "bronze.sales.orders"},
            target=target,
            dqx=dqx,
            etl=etl,
            quarantine=quarantine,
            customs=customs,
            foo="bar",  # type: ignore[arg-type]
        )
