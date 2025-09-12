from typing import Optional, Sequence
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, DataType
)

class TableManager:
    """
    Garante schemas e tabelas externas no UC, com LOCATION + PARTITIONED BY + TBLPROPERTIES.
    Agora: cria a tabela com SCHEMA explícito (colunas e tipos), permitindo PARTITIONED BY.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def _q(name: str) -> str:
        return f"`{name}`"

    def ensure_schema(self, catalog: str, schema: str) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._q(catalog)}.{self._q(schema)}")

    # ---------- helpers para DDL ----------
    def _to_sql_type(self, dt: DataType) -> str:
        if isinstance(dt, StringType):    return "STRING"
        if isinstance(dt, IntegerType):   return "INT"
        if isinstance(dt, LongType):      return "BIGINT"
        if isinstance(dt, DoubleType):    return "DOUBLE"
        if isinstance(dt, BooleanType):   return "BOOLEAN"
        if isinstance(dt, DateType):      return "DATE"
        if isinstance(dt, TimestampType): return "TIMESTAMP"
        if isinstance(dt, DecimalType):   return f"DECIMAL({dt.precision},{dt.scale})"
        # Para Bronze estamos assumindo tipos atômicos; ajuste aqui se quiser suportar STRUCT/ARRAY/MAP.
        raise ValueError(f"Tipo Spark não suportado no DDL: {dt.simpleString()}")

    def _ddl_columns_with_audit(self, schema_struct: StructType) -> str:
        """
        Constrói a lista de colunas para o DDL, acrescentando colunas de auditoria se ausentes.
        """
        fields: list[StructField] = list(schema_struct) if isinstance(schema_struct, StructType) else []
        names = {f.name for f in fields}

        ddl_parts = []
        # colunas do contrato
        for f in fields:
            ddl_parts.append(f"{self._q(f.name)} {self._to_sql_type(f.dataType)}")

        # colunas de auditoria (se não vieram do contrato)
        if "ingestion_ts" not in names:
            ddl_parts.append(f"{self._q('ingestion_ts')} TIMESTAMP")
        if "ingestion_date" not in names:
            ddl_parts.append(f"{self._q('ingestion_date')} DATE")

        return ",\n  ".join(ddl_parts)

    def ensure_external_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        location: str,
        schema_struct: StructType,
        partitions: Optional[Sequence[str]] = None,
        comment: Optional[str] = None,
    ) -> None:
        """
        Cria/garante uma tabela EXTERNA Delta no UC com SCHEMA, PARTITIONED BY e LOCATION.
        """
        self.ensure_schema(catalog, schema)

        cols_clause = self._ddl_columns_with_audit(schema_struct)

        part_clause = ""
        if partitions:
            cols = ", ".join(self._q(c) for c in partitions)
            part_clause = f"PARTITIONED BY ({cols})"

        comment_clause = f"COMMENT '{comment}'" if comment else ""

        # Propriedades padrão (ajuste conforme política)
        props = {
            "delta.feature.timestampNtz": "supported",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "7",
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
        LOCATION '{location}'
        {props_clause}
        """
        # compacta linhas
        self.spark.sql("\n".join(line for line in sql.splitlines() if line.strip()))
