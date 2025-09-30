"""
Módulo: test_target
Finalidade: validar a política de escrita (TargetWriteCfg) e o alias 'schema' em TargetCfg.
"""

from onedata.silver.domain.target import TargetWriteCfg, TargetCfg


def test_targetwritecfg_defaults():
    """Propósito: confirmar defaults de escrita ('merge', listas vazias)."""
    w = TargetWriteCfg()
    assert w.mode == "merge"
    assert w.merge_keys == []
    assert w.partition_by == []
    assert w.zorder_by == []


def test_targetcfg_alias_schema_e_dump_por_alias():
    """
    Propósito: aceitar 'schema' no input (alias de schema_name) e conseguir dump por alias.
    """
    t = TargetCfg(catalog="silver", schema="sales_new", table="orders", write=TargetWriteCfg())
    assert t.schema_name == "sales_new"

    dumped = t.model_dump(by_alias=True)
    # no dump por alias deve sair como 'schema', não 'schema_name'
    assert "schema" in dumped and "schema_name" not in dumped
    assert dumped["schema"] == "sales_new"
