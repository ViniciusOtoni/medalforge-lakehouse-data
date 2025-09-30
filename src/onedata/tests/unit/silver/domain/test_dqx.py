"""
Módulo: test_dqx
Finalidade: garantir que os modelos DQX (DQInnerCheck, DQCheck, DQXCfg) apliquem
aliases e normalizações conforme especificado, e que extras não quebrem (ignorados).
"""

import pytest
from onedata.silver.domain.dqx import DQInnerCheck, DQCheck, DQXCfg


def test_innercheck_arguments_none_vira_dict_vazio():
    """Propósito: quando arguments=None, o validador deve normalizar para {}."""
    chk = DQInnerCheck(function="is_not_null", arguments=None)
    assert chk.arguments == {}


def test_dqcheck_formato_achatado_e_alias_de_funcao():
    """
    Propósito: aceitar o formato achatado e aplicar alias 'unique'->'is_unique'.
    """
    model = DQCheck(
        name="u1",
        criticality="error",
        function="unique",              # achatado + alias
        arguments={"column": "id"}
    )
    assert model.check.function == "is_unique"
    assert "columns" in model.check.arguments
    assert model.check.arguments["columns"] == ["id"]


def test_dqcheck_normaliza_is_unique_columns_variantes():
    """
    Propósito: normalizar 'column' vs 'columns' (string, lista, lista de listas) e 'nulls_distinct' string->bool.
    """
    # string simples
    m1 = DQCheck(name="u", function="is_unique", arguments={"column": "a"})
    assert m1.check.arguments["columns"] == ["a"]

    # columns como string
    m2 = DQCheck(name="u2", function="is_unique", arguments={"columns": "a"})
    assert m2.check.arguments["columns"] == ["a"]

    # columns aninhado
    m3 = DQCheck(name="u3", function="is_unique", arguments={"columns": [["a", "b"], "c"]})
    assert m3.check.arguments["columns"] == ["a", "b", "c"]

    # nulls_distinct coerção
    m4 = DQCheck(name="u4", function="is_unique", arguments={"column": "x", "nulls_distinct": "TrUe"})
    assert m4.check.arguments["nulls_distinct"] is True


def test_dqcheck_normaliza_is_in_range_min_max():
    """
    Propósito: em is_in_range, valores float integrais viram int; floats não-integrais viram string.
    Evita problemas de binário para limites decimais.
    """
    m1 = DQCheck(
        name="r1",
        function="is_in_range",
        arguments={"column": "a", "min_limit": 10.0, "max_limit": 20.0},
    )
    assert m1.check.arguments["min_limit"] == 10
    assert m1.check.arguments["max_limit"] == 20

    m2 = DQCheck(
        name="r2",
        function="is_in_range",
        arguments={"column": "a", "min_limit": 0.1, "max_limit": 0.3},
    )
    assert m2.check.arguments["min_limit"] == "0.1"
    assert m2.check.arguments["max_limit"] == "0.3"


def test_dqcheck_extras_sao_ignorados():
    """Propósito: extras não explodem; garantimos que não aparecem no dump do modelo."""
    model = DQCheck(name="x", function="is_not_null", arguments={}, foo="x")  # type: ignore[arg-type]
    dumped = model.model_dump()
    assert "foo" not in dumped
    assert "foo" not in dumped.get("check", {})
