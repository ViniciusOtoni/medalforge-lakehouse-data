# src/onedata/silver/stages/write_stage.py

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from onedata.silver.utils.uc import split_fqn, ensure_catalog_schema
from onedata.silver.utils.merge import merge_upsert, append_external

def _normalize_fqn_with_current_catalog(spark: SparkSession, table_fqn: str) -> str:
    """
    Normaliza FQN para 3 partes (catalog.schema.table).
    - Se vier com 2 partes (schema.table), prefixa com current_catalog().
    - Se vier com 3 partes, retorna como está.
    """
    parts = table_fqn.split(".")
    if len(parts) == 3:
        return table_fqn
    if len(parts) == 2:
        current_catalog = spark.sql("select current_catalog()").first()[0]
        return f"{current_catalog}.{parts[0]}.{parts[1]}"
    raise ValueError(f"Nome de tabela inválido: '{table_fqn}'. Esperado schema.table ou catalog.schema.table.")

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
    target_fqn = _normalize_fqn_with_current_catalog(spark, target_fqn)
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
    Garante catálogo/esquema antes do primeiro CREATE TABLE.
    """
    spark = df.sparkSession
    table_fqn = _normalize_fqn_with_current_catalog(spark, table_fqn)
    cat, sch, _ = split_fqn(table_fqn)
    ensure_catalog_schema(cat, sch) 
    append_external(df, table_fqn, external_base=external_base, partition_by=partition_by)
