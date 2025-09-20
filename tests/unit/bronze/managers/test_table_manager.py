"""
TableManager: geração de DDL e aplicação de comentários (mock de spark.sql).
"""

from pyspark.sql.types import StructType, StructField, StringType
from bronze.managers.table_manager import TableManager


def test_ensure_external_table_emite_sql(monkeypatch, spark):
    """
    Verifica que o método emite CREATE TABLE e ALTER COLUMN COMMENT.
    """
    
    captured = []

    def fake_sql(sqlQuery):
        captured.append(sqlQuery)

    monkeypatch.setattr(spark, "sql", fake_sql)

    tm = TableManager(spark)
    schema = StructType([StructField("id", StringType(), True)])
    tm.ensure_external_table(
        catalog="bronze",
        schema="sales",
        table="orders",
        location="abfss://bronze@acct.dfs.core.windows.net/datasets/sales/orders",
        schema_struct=schema,
        partitions=["ingestion_date"],
        comment="Bronze table",
        column_comments={"id": "Primary key", "ingestion_date": "Partition"},
    )

    assert any("CREATE TABLE IF NOT EXISTS" in q for q in captured)
    assert any("ALTER TABLE" in q and "ALTER COLUMN" in q for q in captured)
