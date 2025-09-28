import pytest
from pyspark.sql.types import StructType, StructField, StringType
from bronze.ingestors.factory import IngestorFactory
from bronze.ingestors.ingestors import CSVIngestor, JSONIngestor, DelimitedTextIngestor


def test_factory_registry_defaults():
    # formatos padrão disponíveis
    for fmt, cls in [("csv", CSVIngestor), ("json", JSONIngestor), ("txt", DelimitedTextIngestor)]:
        assert IngestorFactory._FORMAT_REGISTRY[fmt] is cls


def test_factory_register_custom_class(monkeypatch):
    class FakeIngestor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    IngestorFactory.register("custom", FakeIngestor)
    assert IngestorFactory._FORMAT_REGISTRY["custom"] is FakeIngestor


def test_factory_create_builds_instance(spark):
    inst = IngestorFactory.create(
        data_format="csv",
        spark=spark,
        target_table="bronze.sales.orders",
        schema=StructType([StructField("id", StringType())]),
        partitions=[],
        reader_options={"header": "true"},
        source_directory="/raw",
        checkpoint_location="/cp",
    )
    # Deve ser CSVIngestor
 
    assert isinstance(inst, CSVIngestor)
    assert inst.target_table == "bronze.sales.orders"


def test_factory_raises_for_unknown_format(spark):
    with pytest.raises(TypeError):
        IngestorFactory.create(
            data_format="parquet",
            spark=spark,
            target_table="bronze.x.y",
            schema=StructType([]),
            partitions=[],
            reader_options={},
            source_directory="/raw",
            checkpoint_location="/cp",
        )
