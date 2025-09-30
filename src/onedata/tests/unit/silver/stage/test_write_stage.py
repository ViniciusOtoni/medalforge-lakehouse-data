"""
Módulo: test_write_stage
Finalidade: validar:
 - normalização de FQN com catálogo atual (_normalize_fqn_with_current_catalog)
 - ensure_uc_and_merge chama split/ensure/merge com FQN normalizado
 - persist_df_append_external chama split/ensure/append com FQN normalizado

Todos os efeitos externos (UC/merge/append) são mockados com monkeypatch.
"""

import pytest
from pyspark.sql import Row, types as T
from onedata.silver.stages.write_stage import (
    _normalize_fqn_with_current_catalog,
    ensure_uc_and_merge,
    persist_df_append_external,
)


def test_normalize_fqn_retorna_3_partes_ou_erra(monkeypatch, spark):
    """Propósito: garantir que schema.table receba o current_catalog e 3-partes retornem inalteradas."""
    # Mock current_catalog() -> 'silver'
    def fake_sql(query):
        class RowObj:  # .first()[0] retorna 'silver'
            def __init__(self): self._v = "silver"
            def first(self): return [self._v]
        return RowObj()

    monkeypatch.setattr(spark, "sql", fake_sql, raising=True)

    # 2-partes vira 3-partes
    assert _normalize_fqn_with_current_catalog(spark, "sales.orders") == "silver.sales.orders"
    # 3-partes fica intacto
    assert _normalize_fqn_with_current_catalog(spark, "lake.sales.orders") == "lake.sales.orders"
    # 1-parte deve falhar
    with pytest.raises(ValueError):
        _normalize_fqn_with_current_catalog(spark, "orders")


def test_ensure_uc_and_merge_chama_utilitarios_com_fqn_normalizado(monkeypatch, spark):
    """
    Propósito: verificar sequência:
      1) normaliza FQN com catálogo atual
      2) split_fqn -> (cat, sch, tbl)
      3) ensure_catalog_schema(cat, sch)
      4) merge_upsert(...) com parâmetros repassados
    """
    # mock current_catalog = silver
    def fake_sql(query):
        class R: 
            def first(self): return ["silver"]
        return R()
    monkeypatch.setattr(spark, "sql", fake_sql, raising=True)

    seen = {}

    # monkeypatch no módulo alvo
    import onedata.silver.stages.write_stage as mod

    def fake_split_fqn(fqn):
        seen["split_in"] = fqn
        return ("silver", "sales", "orders")

    def fake_ensure(cat, sch):
        seen["ensure"] = (cat, sch)

    def fake_merge(df, fqn, keys, zorder_by, partition_by=None, external_base=None):
        seen["merge"] = {
            "fqn": fqn, "keys": keys, "zorder": zorder_by,
            "partition_by": partition_by, "external_base": external_base,
            "rows": df.count(),
        }

    monkeypatch.setattr(mod, "split_fqn", fake_split_fqn, raising=True)
    monkeypatch.setattr(mod, "ensure_catalog_schema", fake_ensure, raising=True)
    monkeypatch.setattr(mod, "merge_upsert", fake_merge, raising=True)

    df = spark.createDataFrame([Row(id=1)], T.StructType([T.StructField("id", T.IntegerType())]))

    ensure_uc_and_merge(
        spark,
        final_df=df,
        target_fqn="sales.orders",  # sem catálogo → deve prefixar com current_catalog='silver'
        merge_keys=["id"],
        zorder_by=["id"],
        partition_by=["id"],
        external_base="abfss://silver@st.dfs.core.windows.net/tables",
    )

    # Asserts
    assert seen["split_in"] == "silver.sales.orders"
    assert seen["ensure"] == ("silver", "sales")
    assert seen["merge"]["fqn"] == "silver.sales.orders"
    assert seen["merge"]["keys"] == ["id"]
    assert seen["merge"]["zorder"] == ["id"]
    assert seen["merge"]["partition_by"] == ["id"]
    assert seen["merge"]["external_base"].startswith("abfss://")
    assert seen["merge"]["rows"] == 1


def test_persist_df_append_external_chama_utilitarios(monkeypatch, spark):
    """
    Propósito: validar que persist_df_append_external normaliza FQN, garante UC e chama append_external.
    """
    # mock current_catalog = silver
    def fake_sql(query):
        class R: 
            def first(self): return ["silver"]
        return R()
    monkeypatch.setattr(spark, "sql", fake_sql, raising=True)

    import onedata.silver.stages.write_stage as mod

    seen = {}

    def fake_split_fqn(fqn):
        seen["split_in"] = fqn
        return ("silver", "sales", "orders")

    def fake_ensure(cat, sch):
        seen["ensure"] = (cat, sch)

    def fake_append(df, fqn, external_base=None, partition_by=None):
        seen["append"] = {
            "fqn": fqn, "external_base": external_base, "partition_by": partition_by,
            "rows": df.count(),
        }

    monkeypatch.setattr(mod, "split_fqn", fake_split_fqn, raising=True)
    monkeypatch.setattr(mod, "ensure_catalog_schema", fake_ensure, raising=True)
    monkeypatch.setattr(mod, "append_external", fake_append, raising=True)

    df = spark.createDataFrame([Row(id=2)], T.StructType([T.StructField("id", T.IntegerType())]))

    persist_df_append_external(
        df,
        table_fqn="sales.orders",
        external_base="abfss://silver@st/tables",
        partition_by=["id"],
    )

    assert seen["split_in"] == "silver.sales.orders"
    assert seen["ensure"] == ("silver", "sales")
    assert seen["append"]["fqn"] == "silver.sales.orders"
    assert seen["append"]["external_base"].startswith("abfss://")
    assert seen["append"]["partition_by"] == ["id"]
    assert seen["append"]["rows"] == 1
