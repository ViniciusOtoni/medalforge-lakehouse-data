from importlib import import_module
from pyspark.sql.types import StructType, StructField, StringType
from bronze.ingestors.factory import IngestorFactory


_ing_mod_name = next(iter(IngestorFactory._FORMAT_REGISTRY.values())).__module__
_ing_mod = import_module(_ing_mod_name)
CSVIngestor = getattr(_ing_mod, "CSVIngestor")
JSONIngestor = getattr(_ing_mod, "JSONIngestor")
DelimitedTextIngestor = getattr(_ing_mod, "DelimitedTextIngestor")


def test_factory_registry_defaults():
    expected = {
        "csv": CSVIngestor,
        "json": JSONIngestor,
        "txt": DelimitedTextIngestor,
    }
    for fmt, cls in expected.items():
        assert IngestorFactory._FORMAT_REGISTRY[fmt] is cls


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
    assert isinstance(inst, CSVIngestor)
