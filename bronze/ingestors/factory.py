# ingestors/factory.py
from __future__ import annotations
from typing import Any, Dict, List, Type
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestors.ingestors import CSVIngestor, JSONIngestor, TextIngestor

class IngestorFactory:
    """
    Cria instâncias de ingestors a partir de um formato, via mapa de registro.
    - Case-insensitive
    - Extensível via register()
    """

    # formato -> classe
    _REGISTRY: Dict[str, Type] = {
        "csv": CSVIngestor,
        "json": JSONIngestor,
        "txt": TextIngestor,
        "text": TextIngestor,
    }

    @classmethod
    def register(cls, fmt: str, ingestor_cls: Type) -> None:
        """
        Registra/override de um formato para uma classe de ingestor.
        Ex.: IngestorFactory.register("parquet", ParquetIngestor)
        """
        if not isinstance(fmt, str) or not fmt.strip():
            raise ValueError("fmt deve ser string não vazia")
        cls._REGISTRY[fmt.strip().lower()] = ingestor_cls

    @classmethod
    def create(
        cls,
        fmt: str,
        spark: SparkSession,
        fqn: str,
        schema_struct: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ):
        key = (fmt or "").strip().lower()
        ingestor_cls = cls._REGISTRY.get(key)
        if ingestor_cls is None:
            known = ", ".join(sorted(cls._REGISTRY.keys()))
            raise TypeError(f"Formato inválido '{fmt}'. Suportados: {known}")

        return ingestor_cls(
            spark=spark,
            fqn=fqn,
            schema_struct=schema_struct,
            partitions=partitions,
            reader_options=reader_options,
            source_directory=source_directory,
            checkpoint_location=checkpoint_location,
        )
