from __future__ import annotations
from pyspark.sql import functions as F
from interfaces.ingestor_interfaces import (
    StructuredDataIngestor,
    SemiStructuredDataIngestor,
    UnstructuredDataIngestor,
)

def _apply_trigger(writer, trigger: str):
    """
    Aplica o gatilho no DataStreamWriter:
      - 'availableNow' => modo D-1
      - 'processingTime:5 minutes' => contínuo com janela
    """
    if trigger == "availableNow":
        return writer.trigger(availableNow=True)
    if trigger.startswith("processingTime:"):
        interval = trigger.split("processingTime:", 1)[1].strip()
        return writer.trigger(processingTime=interval)
    # fallback seguro
    return writer.trigger(availableNow=True)


class CSVIngestor(StructuredDataIngestor):
    """
    Ingestão de arquivos CSV com Databricks Auto Loader + Delta/UC.

    - 'cloudFiles.schemaLocation' aponta para o mesmo caminho do checkpoint (no BRONZE).
    - Adiciona 'ingestion_ts' e 'ingestion_date' antes de escrever.
    - Particiona conforme self.partitions (já inclui 'ingestion_date').
    """

    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = True,
    ) -> None:
        try:
            reader = (
                self.spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                    .option("cloudFiles.includeExistingFiles", "true" if include_existing_files else "false")
                    .option("cloudFiles.schemaLocation", self.checkpoint_location)
                    .schema(self.schema_struct)
            )

            # Opções específicas de CSV
            for k in ("header", "delimiter", "nullValue"):
                if k in self.reader_options:
                    reader = reader.option(k, self.reader_options[k])

            df = reader.load(self.source_directory)

            # Auditoria
            df = (
                df.withColumn("ingestion_ts", F.current_timestamp())
                  .withColumn("ingestion_date", F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                  .format("delta")
                  .option("checkpointLocation", self.checkpoint_location)
                  .option("mergeSchema", "true")
                  .outputMode("append")
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            _apply_trigger(writer, trigger).toTable(self.fqn)
            print(f"[CSVIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[CSVIngestor] Erro: {e}")
            raise


class JSONIngestor(SemiStructuredDataIngestor):
    """
    Ingestão de arquivos JSON com Databricks Auto Loader + Delta/UC.
    """

    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = True,
    ) -> None:
        try:
            reader = (
                self.spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.includeExistingFiles", "true" if include_existing_files else "false")
                    .option("cloudFiles.schemaLocation", self.checkpoint_location)
                    .schema(self.schema_struct)
            )

            if "multiline" in self.reader_options:
                reader = reader.option("multiline", self.reader_options["multiline"])

            df = reader.load(self.source_directory)

            df = (
                df.withColumn("ingestion_ts", F.current_timestamp())
                  .withColumn("ingestion_date", F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                  .format("delta")
                  .option("checkpointLocation", self.checkpoint_location)
                  .option("mergeSchema", "true")
                  .outputMode("append")
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            _apply_trigger(writer, trigger).toTable(self.fqn)
            print(f"[JSONIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[JSONIngestor] Erro: {e}")
            raise


class TextIngestor(UnstructuredDataIngestor):
    """
    Ingestão de TXT **delimitado** (tratado como CSV com outro delimitador).
    """

    def ingest(
        self,
        trigger: str = "availableNow",
        include_existing_files: bool = True,
    ) -> None:
        try:
            if "delimiter" not in self.reader_options:
                raise ValueError("Para TXT delimitado, 'reader_options[\"delimiter\"]' é obrigatório.")

            reader = (
                self.spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")  # TXT como CSV
                    .option("cloudFiles.includeExistingFiles", "true" if include_existing_files else "false")
                    .option("cloudFiles.schemaLocation", self.checkpoint_location)
                    .schema(self.schema_struct)
                    .option("header", self.reader_options.get("header", False))
                    .option("delimiter", self.reader_options["delimiter"])
            )

            if "nullValue" in self.reader_options:
                reader = reader.option("nullValue", self.reader_options["nullValue"])

            df = reader.load(self.source_directory)

            df = (
                df.withColumn("ingestion_ts", F.current_timestamp())
                  .withColumn("ingestion_date", F.to_date(F.col("ingestion_ts")))
            )

            writer = (
                df.writeStream
                  .format("delta")
                  .option("checkpointLocation", self.checkpoint_location)
                  .option("mergeSchema", "true")
                  .outputMode("append")
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            _apply_trigger(writer, trigger).toTable(self.fqn)
            print(f"[TextIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[TextIngestor] Erro: {e}")
            raise
