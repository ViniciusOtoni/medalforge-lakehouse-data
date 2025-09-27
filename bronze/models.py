"""
Modelos tipados usados pelo runner da Bronze (config, paths e planos).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class RunnerConfig:
    """
    Configuração de execução do runner.

    Parâmetros
    ----------
    mode : str
        Modo de execução ("validate", "plan", "ingest" ou combinações concatenadas por '+').
    include_existing_files : bool
        Se True, processa backlog + novos arquivos (Auto Loader).
    reprocess_label : str | None
        Sufixo opcional para isolar o checkpoint (ex.: "replay2024").
    env : str
        Ambiente lógico (ex.: "dev", "hml", "prd").
    """
    mode: str = "validate+plan+ingest"
    include_existing_files: bool = False
    reprocess_label: str | None = None
    env: str = "dev"


@dataclass(frozen=True)
class BaseLocations:
    """
    Raízes físicas usadas pelo runner.

    Parâmetros
    ----------
    raw_root : str
        Raiz do contêiner RAW (ex.: abfss://raw@...).
    bronze_root : str
        Raiz do contêiner BRONZE (ex.: abfss://bronze@...).
    """
    raw_root: str
    bronze_root: str


@dataclass(frozen=True)
class Paths:
    """
    Conjunto de caminhos derivados para uma tabela alvo.

    Parâmetros
    ----------
    source_directory : str
        Diretório/prefixo monitorado pelo Auto Loader (RAW).
    table_location : str
        LOCATION externo para a tabela BRONZE (abfss://...).
    checkpoint_location : str
        Caminho do checkpoint do stream na BRONZE.
    """
    source_directory: str
    table_location: str
    checkpoint_location: str


@dataclass(frozen=True)
class IngestionPlan:
    """
    Plano da ingestão gerado a partir do contrato.

    Parâmetros
    ----------
    contract_fqn : str
        FQN do contrato (catalog.schema.table).
    data_format : str
        Formato de leitura (ex.: "csv", "json").
    reader_options : Dict[str, Any]
        Opções finais do leitor (defaults da família ⊕ contrato).
    partitions : List[str]
        Partições físicas efetivas (ex.: ["created_at", "ingestion_date"]).
    schema_json : str
        Representação do schema em JSON (jsonValue do StructType).
    paths : Dict[str, str]
        Caminhos (source_directory, table_location, checkpointLocation).
    trigger : str
        Modo de disparo (ex.: "availableNow").
    include_existing_files : bool
        Flag para consumo de backlog.
    """
    contract_fqn: str
    data_format: str
    reader_options: Dict[str, Any]
    partitions: List[str]
    schema_json: str
    paths: Dict[str, str]
    trigger: str
    include_existing_files: bool

    def as_dict(self) -> Dict[str, Any]:
        """Serializa o plano em dicionário simples (útil para logs e outputs)."""
        return {
            "contract_fqn": self.contract_fqn,
            "format": self.data_format,
            "reader_options": self.reader_options,
            "partitions": self.partitions,
            "schema_struct": self.schema_json,
            "trigger": self.trigger,
            "includeExistingFiles": self.include_existing_files,
            **self.paths,
        }
