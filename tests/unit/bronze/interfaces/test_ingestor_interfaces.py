from pyspark.sql.types import StructType, StructField, StringType
from bronze.interfaces.ingestor_interfaces import StructuredDataIngestor


class _CSVIngestorForTest(StructuredDataIngestor):
    """Classe concreta mínima para testar hooks sem dado real."""
 

def test_default_reader_options_and_validate(spark, monkeypatch):
    ing = _CSVIngestorForTest(
        spark=spark,
        target_table="bronze.sales.orders",
        schema_struct=StructType([StructField("id", StringType())]),
        partitions=[],
        reader_options={},  # sem overrides
        source_directory="/raw",
        checkpoint_location="/cp",
    )
    # defaults têm delimiter e header
    opts = ing.default_reader_options()
    assert "delimiter" in opts and "header" in opts
    # valida não lança
    ing.validate_reader_options({"delimiter": ","})
