"""
Módulo: test_uc
Finalidade: validar utilitários de Unity Catalog (utils/uc.py):
 - split_fqn: formato correto e mensagem em caso de erro
 - ensure_schema: execução do SQL idempotente
 - ensure_catalog_schema: mapeia erros de permissão para RuntimeError com mensagem amigável
"""

import pytest
from onedata.silver.utils.uc import split_fqn, ensure_schema, ensure_catalog_schema


def test_split_fqn_formato_valido_e_invalido():
    """Propósito: aceitar 'catalog.schema.table' e rejeitar strings fora do padrão com mensagem clara."""
    cat, sch, tbl = split_fqn("silver.sales.orders")
    assert (cat, sch, tbl) == ("silver", "sales", "orders")

    with pytest.raises(ValueError) as e:
        split_fqn("sales.orders")  # faltando catálogo
    assert "catalog.schema.table" in str(e.value)


def test_ensure_schema_executa_sql(monkeypatch):
    """Propósito: garantir que ensure_schema emita o SQL de criação idempotente."""
    seen = {}

    class FakeSpark:
        def sql(self, q):
            seen["sql"] = q

    import onedata.silver.utils.uc as mod
    monkeypatch.setattr(mod, "spark", FakeSpark(), raising=True)

    ensure_schema("silver", "sales")
    assert seen["sql"] == "CREATE SCHEMA IF NOT EXISTS silver.sales"


def test_ensure_catalog_schema_mapeia_erro_de_permissao(monkeypatch):
    """Propósito: quando ensure_schema dispara erro de permissão, converter para RuntimeError com mensagem amigável."""
    class FakeErr(Exception):
        pass

    def fake_ensure_schema(catalog, schema):
        raise FakeErr("Not authorized to create schema")  # simula falta de permissão

    import onedata.silver.utils.uc as mod
    monkeypatch.setattr(mod, "ensure_schema", fake_ensure_schema, raising=True)

    with pytest.raises(RuntimeError) as e:
        ensure_catalog_schema("silver", "sales")
    assert "Sem permissão para criar o schema 'silver.sales'" in str(e.value)


def test_ensure_catalog_schema_repropaga_erros_nao_mapeados(monkeypatch):
    """Propósito: erros que não são de permissão devem ser repropagados (sem mascarar)."""
    class Weird(Exception): ...

    def fake_ensure_schema(catalog, schema):
        raise Weird("driver offline")

    import onedata.silver.utils.uc as mod
    monkeypatch.setattr(mod, "ensure_schema", fake_ensure_schema, raising=True)

    with pytest.raises(Weird):
        ensure_catalog_schema("silver", "sales")


def test_ensure_catalog_schema_sucesso_chama_ensure_schema(monkeypatch):
    """Propósito: caminho feliz deve simplesmente chamar ensure_schema sem erros."""
    calls = {"n": 0}

    def fake_ensure_schema(catalog, schema):
        calls["n"] += 1

    import onedata.silver.utils.uc as mod
    monkeypatch.setattr(mod, "ensure_schema", fake_ensure_schema, raising=True)

    ensure_catalog_schema("silver", "sales")
    assert calls["n"] == 1
