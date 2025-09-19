"""
Ingestors baseados em Auto Loader (cloudFiles) para CSV, JSON e TXT (delimitado).
Cada ingestor lê do diretório de origem, enriquece com colunas de ingestão
e escreve em Delta com checkpoint e schema evolution habilitado.
"""

from __future__ import annotations
from pyspark.sql import functions as F
from interfaces.ingestor_interfaces import (
    StructuredDataIngestor,
    SemiStructuredDataIngestor,
    UnstructuredDataIngestor,
)


class CSVIngestor(StructuredDataIngestor):
    """
    Ingestor para arquivos CSV via Auto Loader.

    Parâmetros esperados (via atributos herdados):
    - self.spark: SparkSession
    - self.schema_struct: StructType do arquivo
    - self.partitions: list[str] para particionamento da tabela destino
    - self.reader_options: dict (ex.: header, delimiter, nullValue)
    - self.source_directory: str (pasta de origem)
    - self.checkpoint_location: str (caminho de checkpoint)
    - self.fqn: str (tabela alvo: catalog.schema.table)

    Método
    -------
    ingest(include_existing_files: bool = True) -> None
        Lê CSV (batch), adiciona colunas de ingestão e grava em Delta.

    Exceptions
    ----------
    Propaga exceções de leitura/escrita do Spark.
    """

    def ingest(self, include_existing_files: bool = True) -> None:
        source_dir = self.source_directory
        checkpoint_path = self.checkpoint_location

        try:
            reader = (
                self.spark.readStream
                .format(source="cloudFiles")
                .option(key="cloudFiles.format", value="csv")
                .option(
                    key="cloudFiles.includeExistingFiles",
                    value="true" if include_existing_files else "false",
                )
                .option(key="cloudFiles.schemaLocation", value=checkpoint_path)
                .schema(schema=self.schema_struct)
            )

            # Opções específicas do leitor CSV
            for opt_key in ("header", "delimiter", "nullValue"):
                if opt_key in self.reader_options:
                    reader = reader.option(key=opt_key, value=self.reader_options[opt_key])

            df = reader.load(path=source_dir)

            # Carimbos de ingestão
            df = (
                df.withColumn(colName="ingestion_ts", col=F.current_timestamp())
                  .withColumn(colName="ingestion_date", col=F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                .format(source="delta")
                .option(key="checkpointLocation", value=checkpoint_path)
                .option(key="mergeSchema", value="true")
                .outputMode(outputMode="append")
                .trigger(availableNow=True)
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(tableName=self.fqn)
            print(f"[CSVIngestor] Ingestão concluída em table={self.fqn}")
        except Exception as e:
            print(f"[CSVIngestor] Erro ao ingerir (table={self.fqn}, src={source_dir}): {e}")
            raise


class JSONIngestor(SemiStructuredDataIngestor):
    """
    Ingestor para arquivos JSON via Auto Loader.

    Ver campos herdados em CSVIngestor.

    Método
    -------
    ingest(include_existing_files: bool = True) -> None

    Exceptions
    ----------
    Propaga exceções de leitura/escrita do Spark.
    """

    def ingest(self, include_existing_files: bool = True) -> None:
        source_dir = self.source_directory
        checkpoint_path = self.checkpoint_location

        try:
            reader = (
                self.spark.readStream
                .format(source="cloudFiles")
                .option(key="cloudFiles.format", value="json")
                .option(
                    key="cloudFiles.includeExistingFiles",
                    value="true" if include_existing_files else "false",
                )
                .option(key="cloudFiles.schemaLocation", value=checkpoint_path)
                .schema(schema=self.schema_struct)
            )

            # Opção específica para JSON multi-linha
            if "multiline" in self.reader_options:
                reader = reader.option(key="multiline", value=self.reader_options["multiline"])

            df = reader.load(path=source_dir)

            df = (
                df.withColumn(colName="ingestion_ts", col=F.current_timestamp())
                  .withColumn(colName="ingestion_date", col=F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                .format(source="delta")
                .option(key="checkpointLocation", value=checkpoint_path)
                .option(key="mergeSchema", value="true")
                .outputMode(outputMode="append")
                .trigger(availableNow=True)
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(tableName=self.fqn)
            print(f"[JSONIngestor] Ingestão concluída em table={self.fqn}")
        except Exception as e:
            print(f"[JSONIngestor] Erro ao ingerir (table={self.fqn}, src={source_dir}): {e}")
            raise


class TextIngestor(UnstructuredDataIngestor):
    """
    Ingestor para arquivos TXT delimitados (tratados como CSV) via Auto Loader.

    Requer em reader_options:
    - delimiter: str

    Método
    -------
    ingest(include_existing_files: bool = True) -> None

    Raises
    ------
    ValueError: quando 'delimiter' não é fornecido.
    Outras exceções do Spark podem ser propagadas.
    """

    def ingest(self, include_existing_files: bool = True) -> None:
        source_dir = self.source_directory
        checkpoint_path = self.checkpoint_location

        try:
            if "delimiter" not in self.reader_options:
                raise ValueError(
                    "Para TXT delimitado, 'reader_options[\"delimiter\"]' é obrigatório."
                )

            reader = (
                self.spark.readStream
                .format(source="cloudFiles")
                .option(key="cloudFiles.format", value="csv")  # TXT como CSV
                .option(
                    key="cloudFiles.includeExistingFiles",
                    value="true" if include_existing_files else "false",
                )
                .option(key="cloudFiles.schemaLocation", value=checkpoint_path)
                .schema(schema=self.schema_struct)
                .option(key="header", value=self.reader_options.get("header", False))
                .option(key="delimiter", value=self.reader_options["delimiter"])
            )

            if "nullValue" in self.reader_options:
                reader = reader.option(key="nullValue", value=self.reader_options["nullValue"])

            df = reader.load(path=source_dir)

            df = (
                df.withColumn(colName="ingestion_ts", col=F.current_timestamp())
                  .withColumn(colName="ingestion_date", col=F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                .format(source="delta")
                .option(key="checkpointLocation", value=checkpoint_path)
                .option(key="mergeSchema", value="true")
                .outputMode(outputMode="append")
                .trigger(availableNow=True)
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(tableName=self.fqn)
            print(f"[TextIngestor] Ingestão concluída em table={self.fqn}")
        except Exception as e:
            print(f"[TextIngestor] Erro ao ingerir (table={self.fqn}, src={source_dir}): {e}")
            raise
