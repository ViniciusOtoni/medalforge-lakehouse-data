# ingestors/autoloader_ingestors.py
from __future__ import annotations
from pyspark.sql import functions as F
from interfaces.ingestor_interfaces import (
    StructuredDataIngestor,
    SemiStructuredDataIngestor,
    UnstructuredDataIngestor,
)

class CSVIngestor(StructuredDataIngestor):
    def ingest(
        self,
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

            for k in ("header", "delimiter", "nullValue"):
                if k in self.reader_options:
                    reader = reader.option(k, self.reader_options[k])

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
                  .trigger(availableNow=True)               
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(self.fqn)
            print(f"[CSVIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[CSVIngestor] Erro: {e}")
            raise


class JSONIngestor(SemiStructuredDataIngestor):
    def ingest(
        self,
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
                  .trigger(availableNow=True)               
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(self.fqn)
            print(f"[JSONIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[JSONIngestor] Erro: {e}")
            raise


class TextIngestor(UnstructuredDataIngestor):
    def ingest(
        self,
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
                  .trigger(availableNow=True)               
            )

            if self.partitions:
                writer = writer.partitionBy(*self.partitions)

            writer.toTable(self.fqn)
            print(f"[TextIngestor] Ingestão concluída em {self.fqn}")
        except Exception as e:
            print(f"[TextIngestor] Erro: {e}")
            raise
