"""
Interfaces base para ingestors (Bronze / Auto Loader + UC).
Define o contrato mínimo, hooks (Template Method) e marcadores por tipo de dado.
Inclui colunas de auditoria (ingestion_ts, ingestion_date).
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


class AuditColumnsMixin:
    """
    Mixin para enriquecer DataFrames com colunas de auditoria.

    Objetivo
    --------
    Padronizar a inclusão das colunas:
      - ingestion_ts: timestamp atual no momento da ingestão
      - ingestion_date: data derivada do ingestion_ts

    Métodos
    -------
    add_ingestion_columns(df: DataFrame) -> DataFrame
        Retorna um novo DataFrame com as colunas de auditoria adicionadas.
    """

    def add_ingestion_columns(self, df: DataFrame) -> DataFrame:
        """
        Adiciona colunas de auditoria.

        Parâmetros
        ----------
        df : DataFrame
            DataFrame lido pelo Auto Loader.

        Retorno
        -------
        DataFrame
            DataFrame com as colunas 'ingestion_ts' e 'ingestion_date'.

        Exceções
        --------
        N/A
        """
        df = df.withColumn("ingestion_ts", F.current_timestamp())
        df = df.withColumn("ingestion_date", F.to_date(F.col("ingestion_ts")))
        return df


class DataIngestor(AuditColumnsMixin, ABC):
    """
    Contrato base para ingestão em tabela Delta do Unity Catalog.

    Objetivo
    --------
    Padronizar a leitura via Auto Loader a partir de um diretório fonte
    e a escrita em tabela UC (com particionamento físico opcional).

    Atributos esperados
    -------------------
    spark : SparkSession
        Sessão Spark ativa.
    target_table : str
        Nome da tabela alvo totalmente qualificado (catalog.schema.table).
    schema_struct : StructType
        Schema esperado na leitura (aplicado ao reader).
    partitions : List[str]
        Lista de colunas de particionamento físico (se houver).
    reader_options : Dict[str, Any]
        Opções do leitor por formato (ex.: header, delimiter, multiline, nullValue).
    source_directory : str
        Diretório/prefixo de origem (camada RAW).
    checkpoint_location : str
        Caminho de checkpoint do stream.

    Métodos abstratos (hooks)
    -------------------------
    format_name() -> str
        Retorna o nome do formato (csv|json|text...), usado em cloudFiles.format.
    default_reader_options() -> Dict[str, Any]
        Retorna as opções default do leitor para a família de dados.
    validate_reader_options(opts: Dict[str, Any]) -> None
        Lança exceção se houver inconsistências nas opções.
    write_options() -> Dict[str, Any]
        Opções de escrita adicionais (se necessário). Default: {}.

    Método final
    ------------
    ingest(trigger="availableNow", include_existing_files=False) -> None
        Implementa o Template Method chamando os hooks acima.
    """

    def __init__(
        self,
        spark: SparkSession,
        target_table: str,
        schema_struct: StructType,
        partitions: List[str],
        reader_options: Dict[str, Any],
        source_directory: str,
        checkpoint_location: str,
    ) -> None:
        """
        Inicializa o ingestor com dependências e parâmetros de execução.

        Parâmetros
        ----------
        spark : SparkSession
            Sessão Spark ativa.
        target_table : str
            Tabela alvo no formato FQN (catalog.schema.table).
        schema_struct : StructType
            Schema a ser aplicado na leitura (reader.schema()).
        partitions : List[str]
            Colunas de particionamento físico (opcional).
        reader_options : Dict[str, Any]
            Opções específicas do leitor (sobrepõem os defaults).
        source_directory : str
            Diretório/prefixo de origem (RAW).
        checkpoint_location : str
            Caminho do checkpoint do stream.

        Retorno
        -------
        None

        Exceções
        --------
        N/A (validações específicas ocorrem em validate_reader_options()).
        """
        self.spark: SparkSession = spark
        self.target_table: str = target_table
        self.schema_struct: StructType = schema_struct
        self.partitions: List[str] = partitions or []
        self.reader_options: Dict[str, Any] = reader_options or {}
        self.source_directory: str = source_directory
        self.checkpoint_location: str = checkpoint_location

    # -------------------- hooks (Template Method) -------------------- #

    @abstractmethod
    def format_name(self) -> str:
        """
        Retorna o nome do formato a ser usado em `cloudFiles.format`.

        Retorno
        -------
        str
            Ex.: "csv", "json", "text".
        """
        ...

    @abstractmethod
    def default_reader_options(self) -> Dict[str, Any]:
        """
        Opções default do leitor para a família de dados.

        Retorno
        -------
        Dict[str, Any]
            Dicionário de opções (ex.: {"header":"true", "delimiter":","}).
        """
        ...

    def validate_reader_options(self, opts: Dict[str, Any]) -> None:
        """
        Valida coerência das opções do leitor.

        Parâmetros
        ----------
        opts : Dict[str, Any]
            Opções finais aplicadas (defaults + overrides).

        Retorno
        -------
        None

        Exceções
        --------
        ValueError
            Caso exista alguma incoerência (ex.: delimiter vazio).
        """
        return None 


    def write_options(self) -> Dict[str, Any]:
        """
        Opções de escrita adicionais (hook).

        Retorno
        -------
        Dict[str, Any]
            Propriedades adicionais de escrita (ex.: {"mergeSchema":"true"}).
        """
        return {}

    # -------------------- Template Method -------------------- #

    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = False,
    ) -> None:
        """
        Executa a ingestão via Auto Loader (Template Method).

        Parâmetros
        ----------
        trigger : str, default "availableNow"
            Modo de disparo do streaming ("availableNow" processa backlog e finaliza).
        include_existing_files : bool, default False
            Se True, processa arquivos já existentes + novos.

        Retorno
        -------
        None

        Exceções
        --------
        Pode propagar exceções do Spark durante leitura/escrita/stream.
        """
        file_format = self.format_name()
        # concatenar options padrões + informadas no contrato
        opts = {**self.default_reader_options(), **self.reader_options}
        self.validate_reader_options(opts)

      
        reader = (
            self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("cloudFiles.schemaLocation", self.checkpoint_location)
                .option(
                    "cloudFiles.includeExistingFiles",
                    "true" if include_existing_files else "false",
                )
        )
        for key, val in opts.items():
            reader = reader.option(key, val)

        df = reader.schema(self.schema_struct).load(self.source_directory)

        # Enriquecimento padrão (auditoria) + hook pós-leitura
        df = self.add_ingestion_columns(df)

        # Escrita Delta/UC (toTable)
        writer = (
            df.writeStream
              .option("checkpointLocation", self.checkpoint_location)
        )

        # Trigger
        if trigger == "availableNow":
            writer = writer.trigger(availableNow=True)

        # Opções de escrita adicionais (se necessário)
        for key, value in self.write_options().items():
            writer = writer.option(key, value)

        # Particionamento físico (se houver)
        if self.partitions:
            writer = writer.partitionBy(*self.partitions)

        writer.toTable(self.target_table)


# -------------------- Especializações por tipo de dado -------------------- #
# Aplicando padrões de SOLID... Descentralizar responsabilidades e segregar entre as interfaces

class StructuredDataIngestor(DataIngestor, ABC):
    """
    Base para dados estruturados (CSV).

    Objetivo
    --------
    Fornecer defaults e validações típicas de CSV.
    """
    def format_name(self) -> str:
        """Retorna 'csv' para Auto Loader."""
        return "csv"

    def default_reader_options(self) -> Dict[str, Any]:
        """
        Opções default para CSV.

        Retorno
        -------
        Dict[str, Any]
        """
        return {
            "header": "true",
            "delimiter": ",",
            "quote": '"',
            "escape": '"',
            "nullValue": "",
            "badRecordsPath": f"{self.checkpoint_location}/bad_records",
            "rescuedDataColumn": "_rescued",
        }

    def validate_reader_options(self, opts: Dict[str, Any]) -> None:
        """Valida opções específicas de CSV."""
        if not opts.get("delimiter"):
            raise ValueError("CSV: 'delimiter' não pode ser vazio.")


class SemiStructuredDataIngestor(DataIngestor, ABC):
    """
    Base para dados semiestruturados (JSON-like).

    Objetivo
    --------
    Fornecer defaults/validações típicas de JSON (NDJSON vs multiline).
    """
    def format_name(self) -> str:
        """Retorna 'json' para Auto Loader."""
        return "json"

    def default_reader_options(self) -> Dict[str, Any]:
        """
        Opções default para JSON.

        Retorno
        -------
        Dict[str, Any]
        """
        return {
            "multiline": "false",
            "badRecordsPath": f"{self.checkpoint_location}/bad_records",
            "rescuedDataColumn": "_rescued",
        }

    def validate_reader_options(self, opts: Dict[str, Any]) -> None:
        return None


class UnstructuredDataIngestor(DataIngestor, ABC):
    """
    Base para dados não estruturados (texto/binários).

    Observação
    ----------
    Se for TXT DELIMITADO (ou seja, 'CSV disfarçado'), considere:
      - herdar de StructuredDataIngestor, ou
      - sobrescrever format_name() retornando 'csv' aqui.
    """
    def format_name(self) -> str:
        """Retorna 'text' para Auto Loader (para texto simples)."""
        return "text"

    def default_reader_options(self) -> Dict[str, Any]:
        """
        Opções default para TEXT.

        Retorno
        -------
        Dict[str, Any]
        """
        return {
            "wholetext": "false",
            "badRecordsPath": f"{self.checkpoint_location}/bad_records",
            "rescuedDataColumn": "_rescued",
        }
