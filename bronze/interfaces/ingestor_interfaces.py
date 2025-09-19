"""
Interfaces base para ingestors (Bronze / Auto Loader + UC).
Define o contrato mínimo e marcadores por tipo de dado.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class DataIngestor(ABC):
    """
    Contrato base para ingestão em tabela Delta do Unity Catalog.

    Propósito
    - Padronizar leitura via Auto Loader a partir de um diretório fonte
      e escrita em tabela UC (com particionamento físico opcional).

    Atributos esperados
    - spark: SparkSession ativa.
    - fqn: nome da tabela alvo totalmente qualificado (catalog.schema.table).
    - schema_struct: StructType esperado na leitura.
    - partitions: lista de colunas de particionamento.
    - reader_options: opções do leitor (header, delimiter, multiline, nullValue...).
    - source_directory: diretório/prefixo de origem (RAW).
    - checkpoint_location: caminho de checkpoint do stream.
    """

    def __init__(
        self,
        spark: SparkSession,
        fqn: str,
        schema_struct: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ) -> None:
        """
        Inicializa o ingestor com dependências e parâmetros de execução.

        Parâmetros
        - spark: sessão Spark.
        - fqn: tabela alvo (catalog.schema.table).
        - schema_struct: schema esperado na leitura.
        - partitions: colunas de particionamento físico.
        - reader_options: opções do leitor por formato.
        - source_directory: diretório/prefixo de origem (RAW).
        - checkpoint_location: caminho do checkpoint do stream.

        Retorno
        - None
        """
        self.spark: SparkSession = spark
        self.fqn: str = fqn
        self.schema_struct: StructType = schema_struct
        self.partitions: List[str] = partitions or []
        self.reader_options: Dict[str, Any] = reader_options or {}
        self.source_directory: str = source_directory
        self.checkpoint_location: str = checkpoint_location

    @abstractmethod
    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = True,
    ) -> None:
        """
        Executa a ingestão via Auto Loader.

        Parâmetros
        - trigger: "availableNow" (processa backlog e finaliza)
                   ou "processingTime:<intervalo>" (ex.: "processingTime=5 minutes").
        - include_existing_files: se True, processa arquivos já existentes + novos.

        Retorno
        - None

        Exceptions
        - Pode propagar exceções do Spark (leitura/escrita/stream).
        """
        raise NotImplementedError


class StructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados estruturados (ex.: CSV)."""
    pass


class SemiStructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados semiestruturados (ex.: JSON)."""
    pass


class UnstructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados não estruturados (ex.: texto livre, binários)."""
    pass
