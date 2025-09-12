from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

class DataIngestor(ABC):
    """
    Interface base para ingestão na Bronze (Databricks + UC).

    Intuito:
        Padronizar a ingestão com Auto Loader lendo do RAW e gravando em
        uma tabela Delta (Unity Catalog), com particionamento físico.

    SOLID:
        - SRP: define apenas o contrato; sem DQ/transformações/criação de tabela.
        - ISP: implementações concretas expõem só o que precisam.
        - DIP: orquestrador depende desta abstração, não de classes concretas.

    Args:
        spark: SparkSession ativa.
        fqn: nome totalmente qualificado da tabela UC (ex.: "bronze.sales.orders_raw").
        schema_struct: StructType tipado a partir do contrato (dtype faltante => string).
        partitions: lista de colunas para particionamento físico (sempre inclui 'ingestion_date').
        reader_options: opções específicas do leitor (header, delimiter, multiline, nullValue…).
        source_directory: diretório RAW a ser monitorado pelo Auto Loader (derivado no runner).
        checkpoint_location: caminho do checkpoint (no BRONZE) para esta tabela.
    """

    def __init__(
        self,
        spark: SparkSession,
        fqn: str,
        schema_struct: StructType,
        partitions: List[str],
        reader_options: Dict[str, object],
        source_directory: str,
        checkpoint_location: str,
    ):
        self.spark = spark
        self.fqn = fqn
        self.schema_struct = schema_struct
        self.partitions = partitions or []
        self.reader_options = reader_options or {}
        self.source_directory = source_directory
        self.checkpoint_location = checkpoint_location

    @abstractmethod
    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = True,
    ) -> None:
        """
        Executa a ingestão com Auto Loader.

        Args:
            trigger: 'availableNow' (default D-1) ou 'processingTime:<intervalo>'.
            include_existing_files: se True, processa backlog + novos arquivos.
        """
        raise NotImplementedError


class StructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados estruturados (ex.: CSV)."""
    pass


class SemiStructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados semiestruturados (ex.: JSON)."""
    pass


class UnstructuredDataIngestor(DataIngestor, ABC):
    """Marcador para dados não estruturados (ex.: binaryFile / texto livre)."""
    pass
