from typing import List
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()

def ensure_table(tgt: str):
    """
    Garante que a tabela Delta exista (vazia) para receber MERGE.
    """
    if not spark.catalog.tableExists(tgt):
        spark.sql(f"CREATE TABLE {tgt} USING DELTA AS SELECT * FROM (SELECT 1 AS _dummy) WHERE 1=0")

def merge_upsert(df: DataFrame, tgt: str, keys: List[str], zorder_by: List[str] | None = None):
    """
    MERGE dinâmico com base nas colunas do DataFrame. Atualiza se bater chave; insere se não existir.
    """
    ensure_table(tgt)
    df.createOrReplaceTempView("_staging_merge_")
    cols = df.columns
    on_expr = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    set_expr = ", ".join([f"{c} = s.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])

    spark.sql(f"""
        MERGE INTO {tgt} t
        USING _staging_merge_ s
          ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    if zorder_by:
        spark.sql(f"OPTIMIZE {tgt} ZORDER BY ({', '.join(zorder_by)})")
