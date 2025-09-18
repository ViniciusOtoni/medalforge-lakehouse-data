# utils/merge.py
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from uuid import uuid4

spark = SparkSession.builder.getOrCreate()

def _external_path_for(tgt_fqn: str, external_base: str) -> str:
    # tgt_fqn = catalog.schema.table
    _, schema, table = tgt_fqn.split(".")
    base = external_base.rstrip("/")
    return f"{base}/{schema}/{table}"

def _ensure_external_table_from_df(
    df: DataFrame,
    tgt_fqn: str,
    external_base: Optional[str],
    partition_by: Optional[List[str]] = None,
):
    """
    Se external_base for fornecido, cria a Tabela Delta EXTERNA com LOCATION,
    escrevendo um Delta "vazio" no path para materializar o schema/particionamento.
    Se external_base for None, cria tabela managed a partir do df.limit(0).
    """
    if spark.catalog.tableExists(tgt_fqn):
        return

    if external_base:
        path = _external_path_for(tgt_fqn, external_base)
        writer = df.limit(0).write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        # materializa um delta vazio no path externo
        writer.mode("overwrite").save(path)
        # registra a tabela EXTERNA apontando para o path
        spark.sql(f"CREATE TABLE {tgt_fqn} USING DELTA LOCATION '{path}'")
    else:
        # managed table (vai exigir Root Storage Credential no metastore!)
        writer = df.limit(0).write
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(tgt_fqn)

def merge_upsert(
    df: DataFrame,
    tgt_fqn: str,
    keys: List[str],
    zorder_by: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    external_base: Optional[str] = None,
):
    """
    MERGE dinâmico suportando External Tables:
      - Cria tabela (EXTERNAL se external_base informado) com schema do df.
      - Se df estiver vazio, apenas garante existência.
      - Faz MERGE upsert e, opcionalmente, OPTIMIZE ZORDER BY.
    """
    _ensure_external_table_from_df(
        df, tgt_fqn, external_base=external_base, partition_by=partition_by
    )

    # sem linhas? nada a upsertar
    if len(df.head(1)) == 0:
        return

    staging_view = f"_staging_merge_{uuid4().hex}"
    df.createOrReplaceTempView(staging_view)
    cols = df.columns

    on_expr = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    set_expr = ", ".join([f"{c} = s.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])

    spark.sql(f"""
        MERGE INTO {tgt_fqn} t
        USING {staging_view} s
          ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    if zorder_by:
        spark.sql(f"OPTIMIZE {tgt_fqn} ZORDER BY ({', '.join(zorder_by)})")
