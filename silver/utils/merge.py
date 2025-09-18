# utils/merge.py
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from uuid import uuid4

spark = SparkSession.builder.getOrCreate()

def _create_empty_table_from_df(df: DataFrame, tgt: str, partition_by: Optional[List[str]] = None):
    """
    Cria a tabela Delta 'tgt' (3-part name UC) com o *schema do df*,
    mesmo que df esteja vazio. Suporta partition_by.
    """
    writer = df.limit(0).write  # usa schema do df
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(tgt)

def merge_upsert(
    df: DataFrame,
    tgt: str,
    keys: List[str],
    zorder_by: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
):
    """
    MERGE dinâmico:
      - Se a tabela não existir, cria com o schema do df (respeitando partition_by).
      - Se df estiver vazio, apenas garante a existência e retorna.
      - Se houver dados, faz MERGE (upsert).
      - Opcionalmente faz OPTIMIZE ZORDER BY.
    """
    # garante existência com schema correto
    if not spark.catalog.tableExists(tgt):
        _create_empty_table_from_df(df, tgt, partition_by=partition_by)

    # sem linhas? nada a upsertar, mas tabela já foi criada
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
        MERGE INTO {tgt} t
        USING {staging_view} s
          ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    if zorder_by:
        spark.sql(f"OPTIMIZE {tgt} ZORDER BY ({', '.join(zorder_by)})")
