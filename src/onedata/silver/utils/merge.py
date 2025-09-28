"""
Helpers de escrita para tabelas Delta (UC), incluindo suporte a tabelas EXTERNAS:
- _external_path_for: deriva o LOCATION externo a partir do FQN e base.
- _ensure_external_table_from_df: garante a existência da tabela (EXTERNAL ou managed).
- append_external: garante e faz append no mesmo LOCATION externo.
- merge_upsert: executa MERGE (upsert) dinâmico; opcionalmente aplica OPTIMIZE ZORDER BY.

Observações
- Mantém a API original (nomes/assinaturas).
- Usa DataFrame → escrita Delta; MERGE via Spark SQL.
"""

from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from uuid import uuid4

spark = SparkSession.builder.getOrCreate()


def _external_path_for(tgt_fqn: str, external_base: str) -> str:
    """
    Constrói o caminho (LOCATION) externo para uma tabela, a partir do FQN e da base.

    Parâmetros
    - tgt_fqn: nome totalmente qualificado da tabela (catalog.schema.table).
    - external_base: prefixo base do storage (ex.: "abfss://silver@.../").

    Retorno
    - string no formato "<external_base>/<schema>/<table>".

    Exceptions
    - ValueError: se o FQN não puder ser decomposto em 3 partes.
    """
    # tgt_fqn = catalog.schema.table
    _, schema, table = tgt_fqn.split(".")
    base = external_base.rstrip("/")
    return f"{base}/{schema}/{table}"


def _ensure_external_table_from_df(
    df: DataFrame,
    tgt_fqn: str,
    external_base: Optional[str],
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Garante a existência da tabela Delta (EXTERNA se `external_base` for fornecido).

    Comportamento
    - Se a tabela já existe, retorna imediatamente.
    - EXTERNAL: escreve Delta "vazio" (limit(0)) no LOCATION para materializar schema/partições e cria a tabela via DDL.
    - Managed: usa saveAsTable com o schema do DataFrame (requer permissões de managed/Root Credential).

    Parâmetros
    - df: DataFrame de referência (schema e partições).
    - tgt_fqn: catalog.schema.table.
    - external_base: base do storage; se None, cria managed table.
    - partition_by: lista de colunas de particionamento físico.

    Retorno
    - None
    """
    if spark.catalog.tableExists(dbName=None, tableName=tgt_fqn):
        return

    if external_base:
        path = _external_path_for(tgt_fqn=tgt_fqn, external_base=external_base)
        writer = df.limit(0).write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        # materializa delta vazio no path, registrando schema/particionamento
        writer.mode("overwrite").save(path)
        # registra a tabela EXTERNA apontando para o LOCATION
        spark.sql(sqlQuery=f"CREATE TABLE {tgt_fqn} USING DELTA LOCATION '{path}'")
    else:
        writer = df.limit(0).write
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(name=tgt_fqn)


def append_external(
    df: DataFrame,
    tgt_fqn: str,
    external_base: str,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Garante a tabela EXTERNA e faz append dos dados no mesmo LOCATION externo.

    Parâmetros
    - df: DataFrame a ser acrescentado.
    - tgt_fqn: catalog.schema.table.
    - external_base: base do storage (ex.: "abfss://silver@...").
    - partition_by: colunas de particionamento físico (usadas apenas na 1ª criação).

    Retorno
    - None
    """
    # cria/garante external table (schema do df se for primeira vez)
    _ensure_external_table_from_df(
        df=df,
        tgt_fqn=tgt_fqn,
        external_base=external_base,
        partition_by=partition_by,
    )

    # escreve no mesmo path externo
    path = _external_path_for(tgt_fqn=tgt_fqn, external_base=external_base)
    df.write.format("delta").mode("append").save(path)


def merge_upsert(
    df: DataFrame,
    tgt_fqn: str,
    keys: List[str],
    zorder_by: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    external_base: Optional[str] = None,
) -> None:
    """
    Executa MERGE (upsert) dinâmico suportando tabelas EXTERNAS.

    Fluxo
    - Garante a tabela (EXTERNAL se `external_base` informado) com o schema do df.
    - Se o df estiver vazio, apenas garante a existência e retorna.
    - Cria uma view temporária e executa MERGE INTO (update + insert).
    - Opcionalmente roda OPTIMIZE ZORDER BY.

    Parâmetros
    - df: DataFrame fonte dos dados.
    - tgt_fqn: catalog.schema.table (alvo do MERGE).
    - keys: chaves de junção para o MERGE.
    - zorder_by: colunas para OPTIMIZE ZORDER BY (opcional).
    - partition_by: partições físicas (usadas apenas na criação).
    - external_base: base externa para criar como EXTERNAL; se None, cria managed.

    Retorno
    - None

    Observações
    - O MERGE usa todas as colunas do DataFrame no SET/INSERT.
    """
    _ensure_external_table_from_df(
        df=df,
        tgt_fqn=tgt_fqn,
        external_base=external_base,
        partition_by=partition_by,
    )

    # sem linhas? nada a upsertar (ação leve p/ inspecionar 1 linha)
    if len(df.head(n=1)) == 0:
        return

    staging_view = f"_staging_merge_{uuid4().hex}"
    df.createOrReplaceTempView(name=staging_view)
    cols = df.columns

    on_expr = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    set_expr = ", ".join([f"{c} = s.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])

    spark.sql(sqlQuery=f"""
        MERGE INTO {tgt_fqn} t
        USING {staging_view} s
          ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    if zorder_by:
        spark.sql(sqlQuery=f"OPTIMIZE {tgt_fqn} ZORDER BY ({', '.join(zorder_by)})")
