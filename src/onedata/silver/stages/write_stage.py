"""
Estágio de escrita: garante catálogo/esquema no UC, faz merge_upsert
e persiste rejeitos/quarentena quando configurado.
"""

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from onedata.silver.utils.uc import split_fqn, ensure_catalog_schema
from onedata.silver.utils.merge import merge_upsert, append_external

def ensure_uc_and_merge(
    spark: SparkSession,
    final_df: DataFrame,
    target_fqn: str,
    merge_keys: list[str],
    zorder_by: list[str] | None,
    partition_by: list[str] | None,
    external_base: str,
) -> None:
    """
    Garante UC no destino e executa o merge_upsert.
    """
    cat, sch, _ = split_fqn(target_fqn)
    ensure_catalog_schema(cat, sch)
    merge_upsert(
        final_df,
        target_fqn,
        merge_keys,
        zorder_by,
        partition_by=partition_by,
        external_base=external_base,
    )

def persist_df_append_external(
    df: DataFrame,
    table_fqn: str,
    *,
    external_base: str,
    partition_by: list[str] | None = None,
) -> None:
    """
    Persiste DataFrame com append em tabela externa (Delta).
    """
    append_external(df, table_fqn, external_base=external_base, partition_by=partition_by)
