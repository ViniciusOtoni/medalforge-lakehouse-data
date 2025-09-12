from __future__ import annotations
from typing import Any, Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestors.ingestors import CSVIngestor, JSONIngestor, TextIngestor

class IngestorFactory:
    """
    Factory para criação de instâncias concretas de ingestor com base no formato.
    """

    @staticmethod
    def create(
        fmt: str,
        spark: SparkSession,
        fqn: str,
        schema_struct: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ):
        fmt = (fmt or "").lower()
        if fmt == "csv":
            cls = CSVIngestor
        elif fmt == "json":
            cls = JSONIngestor
        elif fmt == "txt":
            # TXT delimitado é tratado como CSV sob o capô
            cls = TextIngestor
        else:
            raise ValueError(f"Formato não suportado para ingestor: '{fmt}'")
        return cls(
            spark=spark,
            fqn=fqn,
            schema_struct=schema_struct,
            partitions=partitions,
            reader_options=reader_options,
            source_directory=source_directory,
            checkpoint_location=checkpoint_location,
        )
