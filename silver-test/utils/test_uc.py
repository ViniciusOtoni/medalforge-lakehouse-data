"""
Testes unitários de utilitários do Unity Catalog.
"""

import types
import pytest
from silver.utils import uc as UC


def test_split_fqn_ok_and_invalid():
    """
    split_fqn: decompõe corretamente e falha para formatos inválidos.
    """
    assert UC.split_fqn("a.b.c") == ("a", "b", "c")
    with pytest.raises(ValueError):
        UC.split_fqn("a.b")


def test_ensure_catalog_schema_permission_message(monkeypatch):
    """
    ensure_catalog_schema: ao detectar erro de permissão, levanta RuntimeError mais amigável.
    """
    def _boom(*_a, **_k):
        raise Exception("User is not authorized to perform this action")

    monkeypatch.setattr(UC, "ensure_schema", _boom)
    with pytest.raises(RuntimeError) as exc:
        UC.ensure_catalog_schema("cat", "sch")
    assert "Sem permissão para criar o schema" in str(exc.value)
