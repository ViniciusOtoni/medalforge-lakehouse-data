"""
Orquestrador OO do runner Bronze.
Separa responsabilidades e facilita testes (DI de Spark, Managers e Factory).
"""

from __future__ import annotations
import json
from typing import Any, Dict, Tuple

from pyspark.sql import SparkSession

from .models import RunnerConfig, BaseLocations, Paths, IngestionPlan
from managers.data_contract_manager import DataContractManager
from managers.table_manager import TableManager
from ingestors.factory import IngestorFactory
from monitoring.azure_table_runs import PipelineRunLogger


class BronzeOrchestrator:
    """
    Orquestrador da Bronze (validate/plan/ingest).

    Objetivo
    --------
    Encapsular o fluxo de validação, planejamento e ingestão usando:
      - DataContractManager (contratos + schema tipado)
      - TableManager (DDL idempotente UC)
      - IngestorFactory (Auto Loader por formato)

    Parâmetros do construtor
    ------------------------
    spark : SparkSession
        Sessão Spark ativa.
    base_locations : BaseLocations
        Raízes RAW/BRONZE usadas para derivar paths.
    env : str
        Ambiente lógico (ex.: "dev", "hml", "prd").
    """

    def __init__(self, spark: SparkSession, base_locations: BaseLocations, env: str = "dev") -> None:
        self.spark = spark
        self.base = base_locations
        self.env = env

    # ---------------- utilidades internas ---------------- #

    @staticmethod
    def _normalize_mode(mode: str) -> Tuple[bool, bool, bool]:
        """
        Converte a string do modo em flags de execução.

        Parâmetros
        ----------
        mode : str
            Ex.: "validate+plan", "ingest", "validate+ingest"

        Retorno
        -------
        tuple[bool, bool, bool]
            (do_validate, do_plan, do_ingest)
        """
        tokens = {m.strip().lower() for m in mode.replace(",", "+").split("+") if m.strip()}
        do_validate = "validate" in tokens or not tokens
        do_plan = "plan" in tokens or not tokens
        do_ingest = "ingest" in tokens
        return do_validate, do_plan, do_ingest

    def _build_paths(self, catalog: str, schema: str, table: str, reprocess_label: str | None) -> Paths:
        """
        Deriva diretórios de origem (RAW), LOCATION e checkpoint (BRONZE).

        Retorno
        -------
        Paths
        """
        raw_root = self.base.raw_root.rstrip("/")
        bronze_root = self.base.bronze_root.rstrip("/")

        source_directory = f"{raw_root}"  # monitorar contêiner raiz
        table_location = f"{bronze_root}/datasets/{catalog}/{schema}/{table}"
        checkpoint = f"{bronze_root}/_checkpoints/{catalog}/{schema}/{table}"
        if reprocess_label:
            checkpoint = f"{checkpoint}_{reprocess_label}"

        return Paths(
            source_directory=source_directory,
            table_location=table_location,
            checkpoint_location=checkpoint,
        )

    @staticmethod
    def _resolve_target_fqn(payload: Dict[str, Any], mgr: DataContractManager) -> str:
        """
        Resolve o FQN da tabela-alvo de forma resiliente.

        Ordem de resolução:
        - payload["target_table"] (novo contrato/payload)
        - payload["fqn"] (payload antigo)
        - mgr.fqn (sempre disponível)

        Retorno
        -------
        str
            FQN da tabela (catalog.schema.table).
        """
        return (
            payload.get("target_table")
            or payload.get("fqn")
            or getattr(mgr, "fqn")
        )

    # ---------------- passos do fluxo ---------------- #

    def validate(self, contractManager: DataContractManager) -> Dict[str, Any]:
        """
        Validação do contrato/table spec (para logs e inspeção).

        Retorno
        -------
        dict
            Informações úteis (target_table, colunas, partições efetivas, formato, reader options).
        """
        # Reconstituir reader_options para mostrar no output
        reader_opts_merged = DataContractManager(contract=contractManager._model.dict()).reader_options()
        # Preferir fqn; manter chave "target_table" no output por consistência com models
        target_fqn = getattr(contractManager, "target_table", None) or contractManager.fqn
        return {
            "target_table": target_fqn,
            "columns": contractManager.column_names,
            "partitions_effective": contractManager.effective_partitions,
            "format": contractManager.reader_kind(),
            "reader_options_merged": reader_opts_merged,
        }

    def plan(
        self,
        contractManager: DataContractManager,
        payload: Dict[str, Any],
        paths: Paths,
        include_existing_files: bool,
    ) -> IngestionPlan:
        """
        Gera o plano de execução (para impressão e auditoria).

        Retorno
        -------
        IngestionPlan
        """
        try:
            schema_json = json.dumps(obj=payload["schema_struct"].jsonValue())
        except Exception:
            schema_json = str(payload["schema_struct"])

        target_fqn = self._resolve_target_fqn(payload, contractManager)

        plan = IngestionPlan(
            contract_fqn=target_fqn,
            data_format=payload["format"],
            reader_options=payload["reader_options"],
            partitions=payload["partitions"],
            schema_json=schema_json,
            paths={
                "source_directory": paths.source_directory,
                "checkpointLocation": paths.checkpoint_location,
                "table_location": paths.table_location,
            },
            trigger="availableNow",
            include_existing_files=include_existing_files,
        )
        return plan

    def ensure_table(
        self,
        contractManager: DataContractManager,
        schema_struct,
        partitions: list[str],
        table_location: str,
    ) -> None:
        """
        Garante a tabela EXTERNA no UC (idempotente) antes da ingestão.

        Exceções
        --------
        Pode propagar exceções de DDL do Spark.
        """
        tm = TableManager(spark=self.spark)
        tm.ensure_external_table(
            catalog=contractManager.catalog,
            schema=contractManager.schema,
            table=contractManager.table,
            location=table_location,
            schema_struct=schema_struct,
            partitions=partitions,
            comment="Bronze table created by One Data Proccess!",
            column_comments=contractManager.column_comments(),
        )

    def ingest(
        self,
        contractManager: DataContractManager,
        payload: Dict[str, Any],
        paths: Paths,
        include_existing_files: bool,
    ) -> None:
        """
        Executa a ingestão via IngestorFactory (Auto Loader).

        Exceções
        --------
        Pode propagar exceções do Spark/streaming.
        """
        target_fqn = self._resolve_target_fqn(payload, contractManager)

        ingestor = IngestorFactory.create(
            data_format=payload["format"],
            spark=self.spark,
            target_table=target_fqn,
            schema=payload["schema_struct"],
            partitions=payload["partitions"],
            reader_options=payload["reader_options"],
            source_directory=paths.source_directory,
            checkpoint_location=paths.checkpoint_location,
        )
        ingestor.ingest(include_existing_files=include_existing_files)

    # ---------------- orquestração de alto nível ---------------- #

    def run(self, contract_json: str, configRunner: RunnerConfig) -> Dict[str, Any]:
        """
        Executa o pipeline conforme `configRunner.mode` e retorna um objeto de saída
        com status e detalhes (para logs/monitoramento).

        Parâmetros
        ----------
        contract_json : str
            Conteúdo do contrato (JSON).
        configRunner : RunnerConfig
            Configuração de execução (mode, include_existing, etc.).

        Retorno
        -------
        dict
            {"status": [...], "details": {...}}
        """
        contractManager = DataContractManager(contract=contract_json)
        payload = contractManager.as_ingestion_payload()
        do_validate, do_plan, do_ingest = self._normalize_mode(configRunner.mode)

        paths = self._build_paths(
            catalog=contractManager.catalog,
            schema=contractManager.schema,
            table=contractManager.table,
            reprocess_label=configRunner.reprocess_label,
        )

        outputs: Dict[str, Any] = {"status": [], "details": {}}

        # monitoramento
        run_extra = {"mode": configRunner.mode}
        with PipelineRunLogger(
            env=self.env,
            pipeline="bronze",
            schema=contractManager.schema,
            table=contractManager.table,
            target_fqn=contractManager.fqn,
            extra=run_extra,
        ) as runlog:

            # sempre garantir tabela antes de ingerir
            self.ensure_table(
                contractManager=contractManager,
                schema_struct=payload["schema_struct"],
                partitions=payload["partitions"],
                table_location=paths.table_location,
            )

            if do_validate:
                outputs["status"].append("validated")
                outputs["details"]["validate"] = self.validate(contractManager)

            if do_plan:
                outputs["status"].append("planned")
                plan = self.plan(
                    contractManager=contractManager,
                    payload=payload,
                    paths=paths,
                    include_existing_files=configRunner.include_existing_files,
                )
                outputs["details"]["plan"] = plan.as_dict()

            if do_ingest:
                try:
                    self.ingest(
                        contractManager=contractManager,
                        payload=payload,
                        paths=paths,
                        include_existing_files=configRunner.include_existing_files,
                    )
                    outputs["status"].append("ingested")
                    outputs["details"]["ingest"] = {
                        "table": self._resolve_target_fqn(payload, contractManager),
                        "checkpoint": paths.checkpoint_location,
                    }
                    runlog.finish(status="ok")
                except Exception as e:
                    runlog.finish(status="fail", error_json=str(e))
                    raise

            if not outputs["status"]:
                outputs["status"] = ["noop"]

        return outputs
