"""
Fábrica de ingestors (CSV/JSON/TXT). Resolve o formato e cria a instância.
Extensível via registro dinâmico.
"""

from __future__ import annotations
from typing import Any, Dict, List, Type
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bronze.ingestors.ingestors import CSVIngestor, JSONIngestor, DelimitedTextIngestor


class IngestorFactory:
    """
    Resolve ingestors por formato (case-insensitive) e instancia com argumentos nomeados.

    Objetivo
    --------
    Padronizar a criação de ingestors (CSV, JSON, TXT) a partir de um formato informado,
    permitindo extensão dinâmica via `register()`.
    """

    _FORMAT_REGISTRY: Dict[str, Type] = {
        "csv": CSVIngestor,
        "json": JSONIngestor,
        "txt": DelimitedTextIngestor,   # TXT delimitado tratado como CSV
        "text": DelimitedTextIngestor,  # alias para TXT delimitado
    }

    @classmethod
    def register(cls, data_format: str, ingestor_cls: Type) -> None:
        """
        Registra uma classe de ingestor para um formato.

        Parâmetros
        ----------
        data_format : str
            Identificador do formato (ex.: "csv").
        ingestor_cls : Type
            Classe concreta de ingestor a ser registrada.

        Retorno
        -------
        None

        Exceções
        --------
        ValueError
            Se o formato for vazio ou não-string.
        """
        if not isinstance(data_format, str) or not data_format.strip():
            raise ValueError("data_format deve ser string não vazia")
        cls._FORMAT_REGISTRY[data_format.strip().lower()] = ingestor_cls

    @classmethod
    def create(
        cls,
        data_format: str,
        spark: SparkSession,
        target_table: str,
        schema: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ) -> Any:
        """
        Cria o ingestor apropriado para o `data_format`.

        Parâmetros
        ----------
        data_format : str
            Identificador do formato ("csv" | "json" | "txt" | etc.).
        spark : SparkSession
            Sessão Spark ativa.
        target_table : str
            Nome da tabela alvo totalmente qualificada (catalog.schema.table).
        schema : StructType
            Estrutura de schema esperado.
        partitions : list[str]
            Colunas de particionamento físico (opcional).
        reader_options : dict[str, Any]
            Opções específicas do leitor (ex.: header, delimiter).
        source_directory : str
            Diretório/prefixo de origem (RAW).
        checkpoint_location : str
            Caminho do checkpoint do stream (Auto Loader).

        Retorno
        -------
        Any
            Instância do ingestor correspondente.

        Exceções
        --------
        TypeError
            Se o formato informado não estiver registrado.
        """
        key = (data_format or "").strip().lower()
        ingestor_cls = cls._FORMAT_REGISTRY.get(key)
        if ingestor_cls is None:
            known_classes = ", ".join(sorted(cls._FORMAT_REGISTRY.keys()))
            raise TypeError(f"Formato inválido '{data_format}'. Suportados: {known_classes}")

        return ingestor_cls(
            spark=spark,
            target_table=target_table,  
            schema_struct=schema,
            partitions=partitions,
            reader_options=reader_options,
            source_directory=source_directory,
            checkpoint_location=checkpoint_location,
        )
