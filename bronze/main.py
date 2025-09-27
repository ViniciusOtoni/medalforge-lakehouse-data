"""
Runner/Orquestrador da Bronze — Python script puro (sem dbutils.widgets).

Modos (param --mode):
  - validate
  - validate+plan
  - ingest
  - validate+ingest
  - validate+plan+ingest
Default: validate+plan

Comportamento:
  - Lê/valida o contrato (Pydantic)
  - Deriva caminhos:
      RAW:     contêiner raiz (sem subpastas)  -> monitorado pelo Auto Loader
      BRONZE:  LOCATION da tabela e checkpoint
  - Garante a tabela (LOCATION + TBLPROPERTIES) sempre antes de ingerir
  - Usa dtypes do contrato (inclusive complexos)
  - Particiona fisicamente: [partições_do_usuário..., ingestion_date]
  - D-1 por padrão: trigger=availableNow, includeExistingFiles=True
"""

from __future__ import annotations
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import SparkSession

from managers.data_contract_manager import DataContractManager
from managers.table_manager import TableManager
from ingestors.factory import IngestorFactory
from monitoring.azure_table_runs import PipelineRunLogger



sys.path.append(os.getcwd())
ENV = os.getenv("ENV", "dev")

# ---------------------------------------------------------------------------
# HARD-CODED roots
# ---------------------------------------------------------------------------
BASE_LOCATIONS = {
    "raw":    "abfss://raw@medalforgestorage.dfs.core.windows.net",
    "bronze": "abfss://bronze@medalforgestorage.dfs.core.windows.net",
}

# ---------------------------------------------------------------------------

def _parse_args() -> Dict[str, Any]:
    """
    Lê os argumentos de CLI e normaliza tipos simples.

    Retorna
    -------
    dict
        {
          "contract_path": str,
          "mode": str,
          "reprocess_label": Optional[str],
          "include_existing_override": Optional[bool]  # "true"/"false" -> bool
        }
    """
    import argparse

    parser = argparse.ArgumentParser(description="Runner Bronze (validate/plan/ingest)")
    parser.add_argument("--contract_path", required=True, help="Caminho do contrato JSON (TableContract).")
    parser.add_argument("--mode", default="validate+plan", help="Modo: validate|plan|ingest combináveis com '+'.")
    parser.add_argument("--reprocess_label", default=None, help="Sufixo para isolar checkpoint (ex.: 'replay2024').")
    parser.add_argument("--include_existing_override", default=None, help="'true'|'false' força leitura de backlog.")

    args = parser.parse_args()
    result = vars(args)

    if result.get("include_existing_override") is not None:
        result["include_existing_override"] = str(result["include_existing_override"]).lower() == "true"
    return result


def _normalize_mode(mode: str) -> Tuple[bool, bool, bool]:
    """
    Converte a string do modo em flags de execução.

    Parâmetros
    ----------
    mode : str
        Ex.: "validate+plan", "ingest", "validate+ingest", etc.

    Retorna
    -------
    (do_validate, do_plan, do_ingest) : tuple[bool, bool, bool]
    """
    tokens = {t.strip().lower() for t in mode.replace(",", "+").split("+") if t.strip()}
    do_validate = "validate" in tokens
    do_plan = "plan" in tokens or "validate+plan" in mode  # compat
    do_ingest = "ingest" in tokens
    if not tokens:
        return True, True, False
    return do_validate, do_plan, do_ingest


def _build_paths(catalog: str, schema: str, table: str, reprocess_label: Optional[str]) -> Dict[str, str]:
    """
    Deriva diretórios de origem (RAW), LOCATION e checkpoint (BRONZE).

    Parâmetros
    ----------
    catalog, schema, table : str
        Identificadores UC.
    reprocess_label : Optional[str]
        Sufixo opcional para isolar o checkpoint (reprocessamentos).

    Retorna
    -------
    dict
        {
          "source_directory": RAW root (str),
          "table_location":   BRONZE location (str),
          "checkpointLocation": BRONZE checkpoint (str)
        }

    Raises
    ------
    ValueError
        Se BASE_LOCATIONS não contiver chaves 'raw' e 'bronze'.
    """
    if "raw" not in BASE_LOCATIONS or "bronze" not in BASE_LOCATIONS:
        raise ValueError("BASE_LOCATIONS deve conter 'raw' e 'bronze' configurados.")

    raw_root = BASE_LOCATIONS["raw"].rstrip("/")
    bronze_root = BASE_LOCATIONS["bronze"].rstrip("/")

    source_directory = f"{raw_root}"  # monitorar contêiner raiz (sem subpastas)
    table_location = f"{bronze_root}/datasets/{schema}/{table}"   # bronze/datasets/<schema>/<table>
    checkpoint = f"{bronze_root}/_checkpoints/{schema}/{table}"
    if reprocess_label:
        checkpoint = f"{checkpoint}_{reprocess_label}"

    return dict(
        source_directory=source_directory,
        table_location=table_location,
        checkpointLocation=checkpoint,
    )


def _ensure_table(
    spark: SparkSession,
    mgr: DataContractManager,
    schema_struct,
    partitions: list[str],
    table_location: str,
) -> None:
    """
    Garante a tabela EXTERNA no UC (idempotente) antes da ingestão.

    Parâmetros
    ----------
    spark : SparkSession
        Sessão Spark para emitir DDL.
    mgr : DataContractManager
        Fornece catalog/schema/table e comentários de coluna.
    schema_struct : StructType
        Schema base da tabela (contrato).
    partitions : list[str]
        Colunas de particionamento físico.
    table_location : str
        LOCATION externo (abfss://...).
    """
    tm = TableManager(spark=spark)
    tm.ensure_external_table(
        catalog=mgr.catalog,
        schema=mgr.schema,
        table=mgr.table,
        location=table_location,
        schema_struct=schema_struct,
        partitions=partitions,
        comment="Bronze table (LOCATION hard-coded; contract-managed)",
        column_comments=mgr.column_comments(),
    )


def _plan_dict(
    mgr: DataContractManager,
    payload: Dict[str, Any],
    paths: Dict[str, str],
    include_existing: bool,
) -> Dict[str, Any]:
    """
    Monta um dicionário plano com o plano de execução (para log/impressão).

    Retorna
    -------
    dict
        Campos principais do plano (formato, opções do leitor, partições, schema, etc.).
    """
    try:
        schema_repr = json.dumps(obj=payload["schema_struct"].jsonValue())
    except Exception:
        schema_repr = str(payload["schema_struct"])

    return {
        "contract_fqn": mgr.fqn,
        "format": payload["format"],                 # csv|json (txt -> csv)
        "reader_options": payload["reader_options"], # defaults ⊕ contrato
        "partitions": payload["partitions"],         # user..., ingestion_date
        "schema_struct": schema_repr,
        "trigger": "availableNow",                   # fixo (D-1)
        "includeExistingFiles": include_existing,    # true (default)
        "source_directory": paths["source_directory"],      # RAW contêiner raiz
        "checkpointLocation": paths["checkpointLocation"],  # BRONZE
        "table_location": paths["table_location"],          # BRONZE
    }


def main() -> None:
    """
    Ponto de entrada do runner Bronze.

    Fluxo
    - Carrega contrato.
    - Normaliza modo/flags.
    - Deriva caminhos.
    - Garante tabela.
    - (Opcional) validate/plan.
    - (Opcional) ingest.

    Saída
    - Imprime um JSON com 'status' e 'details'.
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    params = _parse_args()

    # Carrega contrato
    with open(params["contract_path"], "r", encoding="utf-8") as f:
        raw = f.read()
    mgr = DataContractManager(contract=raw)
    payload = mgr.as_ingestion_payload()

    # --- MONITORAMENTO (apenas pipeline_runs) ---
    run_extra = {"mode": params.get("mode")}
    with PipelineRunLogger(
        env=ENV,
        pipeline="bronze",
        schema=mgr.schema,
        table=mgr.table,
        target_fqn=mgr.fqn,
        extra=run_extra,
    ) as runlog:

        # Normaliza modo e D-1
        do_validate, do_plan, do_ingest = _normalize_mode(mode=params.get("mode", "validate+plan"))
        include_existing = True if params.get("include_existing_override") is None else bool(
            params["include_existing_override"]
        )

        # Deriva caminhos
        paths = _build_paths(
            catalog=mgr.catalog,
            schema=mgr.schema,
            table=mgr.table,
            reprocess_label=params.get("reprocess_label"),
        )

        # Garante tabela
        _ensure_table(
            spark=spark,
            mgr=mgr,
            schema_struct=payload["schema_struct"],
            partitions=payload["partitions"],
            table_location=paths["table_location"],
        )

        outputs: Dict[str, Any] = {"status": [], "details": {}}

        if do_validate:
            outputs["status"].append("validated")
            outputs["details"]["validate"] = {
                "fqn": mgr.fqn,
                "columns": mgr.column_names,
                "partitions_effective": mgr.effective_partitions,
                "format": payload["format"],
                "reader_options_merged": DataContractManager(
                    contract=mgr._model.dict()
                ).reader_options(),
            }

        if do_plan:
            outputs["status"].append("planned")
            outputs["details"]["plan"] = _plan_dict(
                mgr=mgr,
                payload=payload,
                paths=paths,
                include_existing=include_existing,
            )

        if do_ingest:
            try:
                ingestor = IngestorFactory.create(
                    data_format=payload["format"],
                    spark=spark,
                    target_table_fqn=payload["fqn"],
                    schema=payload["schema_struct"],
                    partitions=payload["partitions"],
                    reader_options=payload["reader_options"],
                    source_directory=paths["source_directory"],
                    checkpoint_location=paths["checkpointLocation"],
                )
                ingestor.ingest(include_existing_files=include_existing)
                outputs["status"].append("ingested")
                outputs["details"]["ingest"] = {
                    "table": payload["fqn"],
                    "checkpoint": paths["checkpointLocation"],
                }
                # Se quiser gravar alguma métrica:
                runlog.finish(status="ok")  # explícito (também será chamado no __exit__)
            except Exception as e:
                # marcar como fail (com erro); __exit__ também fará isso, mas deixo claro
                runlog.finish(status="fail", error_json=str(e))
                print(f"[INGEST][ERROR] {e}", file=sys.stderr)
                raise

        if not outputs["status"]:
            outputs["status"] = ["noop"]
        print(json.dumps(obj=outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
