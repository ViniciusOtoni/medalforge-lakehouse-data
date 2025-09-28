from onedata.bronze.managers.table_manager import TableManager
from pyspark.sql.types import StructType, StructField, StringType


class FakeSpark:
    def __init__(self):
        self.sql_calls = []

    class _SQL:
        def __init__(self, parent):
            self.parent = parent
        def __call__(self, sqlQuery):
            self.parent.sql_calls.append(sqlQuery)

    @property
    def sql(self):
        return TableManager.sql.__get__(self, TableManager)  # not used

    def sql(self, sqlQuery):
        # registra a query
        self.sql_calls.append(sqlQuery)


def test_ensure_external_table_builds_expected_sql():
    fs = FakeSpark()
    tm = TableManager(spark=fs)
    schema = StructType([StructField("id", StringType(), True)])
    tm.ensure_external_table(
        catalog="bronze",
        schema="sales",
        table="orders",
        location="abfss://bronze@acc/datasets/bronze/sales/orders",
        schema_struct=schema,
        partitions=["ingestion_date"],
        comment="Bronze table (contract-managed)",
        column_comments={"id": "identifier"},
    )
    # Pelo menos um CREATE TABLE IF NOT EXISTS deve ter sido emitido
    assert any("CREATE TABLE IF NOT EXISTS" in q for q in fs.sql_calls)
    assert any("USING DELTA" in q for q in fs.sql_calls)
