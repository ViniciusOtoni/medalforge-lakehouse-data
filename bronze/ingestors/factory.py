"""
Fábrica de ingestors (CSV/JSON/TEXT). Resolve o formato e cria a instância.
Extensível via registro dinâmico.
"""

from __future__ import annotations
from typing import Any, Dict, List, Type
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestors.ingestors import CSVIngestor, JSONIngestor, TextIngestor


class IngestorFactory:
    """
    Resolve ingestors por formato (case-insensitive) e instancia com argumentos nomeados.
    Use `register()` para adicionar/sobrescrever formatos.
    """

    _REGISTRY: Dict[str, Type] = {
        "csv": CSVIngestor,
        "json": JSONIngestor,
        "txt": TextIngestor,
        "text": TextIngestor,
    }

    @classmethod
    def register(cls, data_format: str, ingestor_cls: Type) -> None:
        """
        Registra uma classe de ingestor para um formato.

        Parâmetros
        - data_format: identificador do formato (ex.: "csv").
        - ingestor_cls: classe do ingestor.

        Raises
        - ValueError: se o formato for vazio/não-string.
        """
        if not isinstance(data_format, str) or not data_format.strip():
            raise ValueError("data_format deve ser string não vazia")
        cls._REGISTRY[data_format.strip().lower()] = ingestor_cls

    @classmethod
    def create(
        cls,
        data_format: str,
        spark: SparkSession,
        target_table_fqn: str,
        schema: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ) -> Any:
        """
        Cria o ingestor para `data_format`.

        Parâmetros
        - data_format: "csv" | "json" | "txt" | etc.
        - spark: sessão Spark.
        - target_table_fqn: tabela alvo totalmente qualificada (catalog.schema.table).
        - schema: StructType esperado.
        - partitions: colunas de particionamento.
        - reader_options: opções do leitor (ex.: header, delimiter).
        - source_directory: diretório de origem.
        - checkpoint_location: caminho de checkpoint (stream/Auto Loader).

        Retorna
        - Instância do ingestor correspondente.

        Raises
        - TypeError: formato não registrado.
        """
        key = (data_format or "").strip().lower()
        ingestor_cls = cls._REGISTRY.get(key)
        if ingestor_cls is None:
            known = ", ".join(sorted(cls._REGISTRY.keys()))
            raise TypeError(f"Formato inválido '{data_format}'. Suportados: {known}")

        return ingestor_cls(
            spark=spark,
            fqn=target_table_fqn,
            schema_struct=schema,
            partitions=partitions,
            reader_options=reader_options,
            source_directory=source_directory,
            checkpoint_location=checkpoint_location,
        )
