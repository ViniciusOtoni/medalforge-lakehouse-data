"""
Gerencia schemas e tabelas EXTERNAS no Unity Catalog (Delta):
- Cria schema (idempotente).
- Cria/garante tabela externa com LOCATION, PARTITIONED BY e TBLPROPERTIES.
- Aplica comentários em colunas (CREATE/ALTER idempotente).
"""

from typing import Optional, Sequence, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, DataType
)


class TableManager:
    """
    Orquestra DDL no UC para tabelas Delta externas.

    Propósito
    - Garantir a existência de schemas e tabelas externas com metadados corretos.
    - Incluir colunas de auditoria (ingestion_ts/ingestion_date) se ausentes.
    - Aplicar comentários por coluna.

    Notas
    - Operações são idempotentes (CREATE IF NOT EXISTS / ALTER COLUMN COMMENT).
    """

    def __init__(self, spark: SparkSession):
        """
        Inicializa o gerenciador.

        Parâmetros
        - spark: SparkSession ativa usada para emitir comandos SQL.

        Retorno
        - None
        """
        self.spark = spark

    @staticmethod
    def _q(name: str) -> str:
        """
        Quota identificadores para SQL (catalog/schema/table/col).

        Parâmetros
        - name: nome do identificador.

        Retorno
        - string com crases: `nome`
        """
        return f"`{name}`"

    @staticmethod
    def _esc(text: str) -> str:
        """
        Escapa aspas simples para uso em literais SQL.

        Parâmetros
        - text: conteúdo a ser inserido em '...'.

        Retorno
        - string com aspas simples duplicadas.
        """
        return text.replace("'", "''")

    def ensure_schema(self, catalog: str, schema: str) -> None:
        """
        Cria o schema no UC se não existir.

        Parâmetros
        - catalog: nome do catálogo.
        - schema: nome do schema.

        Retorno
        - None
        """
        self.spark.sql(
            sqlQuery=f"CREATE SCHEMA IF NOT EXISTS {self._q(name=catalog)}.{self._q(name=schema)}"
        )

    # ---------- helpers para DDL ----------

    def _to_sql_type(self, dt: DataType) -> str:
        """
        Converte DataType Spark para string DDL.

        Parâmetros
        - dt: tipo Spark.

        Retorno
        - representação DDL (ex.: STRING, INT, DECIMAL(10,2), array<string>, ...).
        """
        # atômicos
        if isinstance(dt, StringType):    return "STRING"
        if isinstance(dt, IntegerType):   return "INT"
        if isinstance(dt, LongType):      return "BIGINT"
        if isinstance(dt, DoubleType):    return "DOUBLE"
        if isinstance(dt, BooleanType):   return "BOOLEAN"
        if isinstance(dt, DateType):      return "DATE"
        if isinstance(dt, TimestampType): return "TIMESTAMP"
        if isinstance(dt, DecimalType):   return f"DECIMAL({dt.precision},{dt.scale})"

        # complexos (array/map/struct) em DDL Spark nativa
        return dt.simpleString()  # ex.: array<string>, map<string,int>, struct<a:string,b:int>

    def _ddl_columns_with_audit(
        self,
        schema_struct: StructType,
        col_comments: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Monta a lista de colunas para o CREATE TABLE:
        - Usa tipos do schema fornecido.
        - Adiciona COMMENT por coluna quando houver.
        - Garante colunas de auditoria ingestion_ts (TIMESTAMP) e ingestion_date (DATE) se ausentes.

        Parâmetros
        - schema_struct: StructType com campos base.
        - col_comments: {coluna: comentário} opcional.

        Retorno
        - string com a cláusula de colunas para o DDL (separada por vírgulas/linhas).
        """
        col_comments = col_comments or {}
        fields: list[StructField] = list(schema_struct) if isinstance(schema_struct, StructType) else []
        names = {f.name for f in fields}

        ddl_parts = []
        for field in fields:
            base = f"{self._q(name=field.name)} {self._to_sql_type(dt=field.dataType)}"
            if field.name in col_comments:
                base += f" COMMENT '{self._esc(text=col_comments[field.name])}'"
            ddl_parts.append(base)

       

        return ",\n  ".join(ddl_parts)

    def _apply_column_comments(
        self,
        catalog: str,
        schema: str,
        table: str,
        col_comments: Dict[str, str]
    ) -> None:
        """
        Aplica/atualiza comentários de colunas via ALTER TABLE ... ALTER COLUMN ... COMMENT.

        Parâmetros
        - catalog, schema, table: identificadores do alvo.
        - col_comments: {coluna: comentário}.

        Retorno
        - None
        """
        if not col_comments:
            return
        target_table = f"{self._q(name=catalog)}.{self._q(name=schema)}.{self._q(name=table)}"
        for col, comment in col_comments.items():
            self.spark.sql(
                sqlQuery=(
                    f"ALTER TABLE {target_table} "
                    f"ALTER COLUMN {self._q(name=col)} "
                    f"COMMENT '{self._esc(text=comment)}'"
                )
            )

    def ensure_external_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        location: str,
        schema_struct: StructType,
        partitions: Optional[Sequence[str]] = None,
        comment: Optional[str] = None,
        column_comments: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Cria/garante uma tabela EXTERNA Delta no UC com:
        - SCHEMA (CREATE SCHEMA IF NOT EXISTS),
        - colunas (com comentários),
        - PARTITIONED BY,
        - LOCATION,
        - TBLPROPERTIES (appendOnly para Bronze, timestampNtz).

        Parâmetros
        - catalog, schema, table: destino UC.
        - location: URI/path externo (abfss://...).
        - schema_struct: StructType base da tabela.
        - partitions: colunas para particionamento físico.
        - comment: comentário da tabela.
        - column_comments: {coluna: comentário}.

        Retorno
        - None

        Exceptions
        - Pode propagar erros do Spark SQL/Delta/UC.
        """
        # 1) Garante schema
        self.ensure_schema(catalog=catalog, schema=schema)

        # 2) Monta seções do DDL
        cols_clause = self._ddl_columns_with_audit(
            schema_struct=schema_struct,
            col_comments=column_comments or {}
        )

        part_clause = ""
        if partitions:
            cols = ", ".join(self._q(name=c) for c in partitions)
            part_clause = f"PARTITIONED BY ({cols})"

        comment_clause = f"COMMENT '{self._esc(text=comment)}'" if comment else ""

        props = {
            "delta.feature.timestampNtz": "supported",
            "delta.appendOnly": "true",  # Bronze: somente append
        }
        props_clause = "TBLPROPERTIES (" + ", ".join([f"'{key_prop}' = '{value_prop}'" for key_prop, value_prop in props.items()]) + ")"

        # 3) CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...
        sql = f"""
        CREATE TABLE IF NOT EXISTS
          {self._q(name=catalog)}.{self._q(name=schema)}.{self._q(name=table)}
        (
          {cols_clause}
        )
        USING DELTA
        {part_clause}
        {comment_clause}
        LOCATION '{self._esc(text=location)}'
        {props_clause}
        """
        self.spark.sql(sqlQuery="\n".join(line for line in sql.splitlines() if line.strip()))

        # 4) Aplica comentários de coluna mesmo se a tabela já existia
        self._apply_column_comments(
            catalog=catalog,
            schema=schema,
            table=table,
            col_comments=column_comments or {}
        )
