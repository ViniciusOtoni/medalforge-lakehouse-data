"""
Módulo: test_etl
Finalidade: validar modelos de ETL/quarentena/customs — tipos, defaults e shapes.
"""

from onedata.silver.domain.etl import Step, QuarantineCfg, CustomDecl, CustomsCfg


def test_step_defaults_e_literais_de_stage():
    """Propósito: Step deve aceitar stage None ou literais ('standard'|'quarantine')."""
    s1 = Step(method="trim", args={"cols": ["name"]})
    assert s1.stage is None
    s2 = Step(method="fix", args={}, stage="standard")
    assert s2.stage == "standard"


def test_quarantinecfg_defaults_and_sink_shape():
    """Propósito: QuarantineCfg deve iniciar com lista vazia e sink opcional."""
    q = QuarantineCfg()
    assert q.remediate == []
    assert q.sink is None
    q2 = QuarantineCfg(remediate=[Step(method="clean")], sink={"table": "monitoring.quarantine.foo"})
    assert q2.remediate[0].method == "clean"
    assert q2.sink["table"].endswith("foo")


def test_customdecl_defaults_args_schema_vazio():
    """Propósito: CustomDecl.args_schema deve defaultar para dict vazio."""
    c = CustomDecl(name="cap", module="custom_x", method="cap_amount")
    assert c.args_schema == {}


def test_customscfg_defaults_e_flags():
    """Propósito: CustomsCfg deve iniciar desabilitado, com registries e use_in vazios."""
    cfg = CustomsCfg()
    assert cfg.allow is False
    assert cfg.registry == []
    assert cfg.use_in == []
