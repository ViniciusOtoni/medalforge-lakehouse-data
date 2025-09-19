# managers/table_manager.py
from typing import Optional, Sequence, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, DataType
)

class TableManager:
    """
    Garante schemas e tabelas externas no UC, com LOCATION + PARTITIONED BY + TBLPROPERTIES.
    Também aplica comentários por coluna (CREATE e ALTER idempotente).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def _q(name: str) -> str:
        return f"`{name}`"

    @staticmethod
    def _esc(text: str) -> str:
        """Escapa aspas simples para literais SQL."""
        return text.replace("'", "''")

    def ensure_schema(self, catalog: str, schema: str) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._q(catalog)}.{self._q(schema)}")

    # ---------- helpers para DDL ----------
    def _to_sql_type(self, dt: DataType) -> str:
        # atômicos com nomes "bonitos"
        if isinstance(dt, StringType):    return "STRING"
        if isinstance(dt, IntegerType):   return "INT"
        if isinstance(dt, LongType):      return "BIGINT"
        if isinstance(dt, DoubleType):    return "DOUBLE"
        if isinstance(dt, BooleanType):   return "BOOLEAN"
        if isinstance(dt, DateType):      return "DATE"
        if isinstance(dt, TimestampType): return "TIMESTAMP"
        if isinstance(dt, DecimalType):   return f"DECIMAL({dt.precision},{dt.scale})"
        # complexos (array/map/struct) em DDL Spark nativa
        return dt.simpleString()  # ex.: "array<string>", "map<string,int>", "struct<a:string,b:int>"

    def _ddl_columns_with_audit(self, schema_struct: StructType, col_comments: Optional[Dict[str, str]] = None) -> str:
        """
        Constrói a lista de colunas para o DDL, acrescentando colunas de auditoria se ausentes,
        e adiciona COMMENT por coluna quando fornecido.
        """
        col_comments = col_comments or {}
        fields: list[StructField] = list(schema_struct) if isinstance(schema_struct, StructType) else []
        names = {f.name for f in fields}

        ddl_parts = []
        for f in fields:
            base = f"{self._q(f.name)} {self._to_sql_type(f.dataType)}"
            if f.name in col_comments:
                base += f" COMMENT '{self._esc(col_comments[f.name])}'"
            ddl_parts.append(base)

        # colunas de auditoria (se não vieram do contrato)
        if "ingestion_ts" not in names:
            cmt = col_comments.get("ingestion_ts")
            base = f"{self._q('ingestion_ts')} TIMESTAMP"
            if cmt:
                base += f" COMMENT '{self._esc(cmt)}'"
            ddl_parts.append(base)

        if "ingestion_date" not in names:
            cmt = col_comments.get("ingestion_date")
            base = f"{self._q('ingestion_date')} DATE"
            if cmt:
                base += f" COMMENT '{self._esc(cmt)}'"
            ddl_parts.append(base)

        return ",\n  ".join(ddl_parts)

    def _apply_column_comments(self, catalog: str, schema: str, table: str, col_comments: Dict[str, str]) -> None:
        """
        Aplica/atualiza comentários via ALTER TABLE … ALTER COLUMN … COMMENT.
        Executa mesmo que a tabela já exista.
        """
        if not col_comments:
            return
        fq = f"{self._q(catalog)}.{self._q(schema)}.{self._q(table)}"
        for col, comment in col_comments.items():
            self.spark.sql(
                f"ALTER TABLE {fq} ALTER COLUMN {self._q(col)} COMMENT '{self._esc(comment)}'"
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
        Cria/garante uma tabela EXTERNA Delta no UC com SCHEMA, PARTITIONED BY, LOCATION e comentários.
        """
        self.ensure_schema(catalog, schema)

        cols_clause = self._ddl_columns_with_audit(schema_struct, column_comments or {})

        part_clause = ""
        if partitions:
            cols = ", ".join(self._q(c) for c in partitions)
            part_clause = f"PARTITIONED BY ({cols})"

        comment_clause = f"COMMENT '{self._esc(comment)}'" if comment else ""

        props = {
            "delta.feature.timestampNtz": "supported",
            "delta.appendOnly": "true"   # Bronze: somente append
        }
        props_clause = "TBLPROPERTIES (" + ", ".join([f"'{k}' = '{v}'" for k, v in props.items()]) + ")"

        sql = f"""
        CREATE TABLE IF NOT EXISTS
          {self._q(catalog)}.{self._q(schema)}.{self._q(table)}
        (
          {cols_clause}
        )
        USING DELTA
        {part_clause}
        {comment_clause}
        LOCATION '{self._esc(location)}'
        {props_clause}
        """
        self.spark.sql("\n".join(line for line in sql.splitlines() if line.strip()))

        # garante/atualiza comentários mesmo se a tabela já existia
        self._apply_column_comments(catalog, schema, table, column_comments or {})
