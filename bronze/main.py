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
  - Usa dtypes do contrato (agora com complexos)
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

sys.path.append(os.getcwd())

# ---------------------------------------------------------------------------
# HARD-CODED roots
# ---------------------------------------------------------------------------
BASE_LOCATIONS = {
    "raw":    "abfss://raw@medalforgestorage.dfs.core.windows.net",
    "bronze": "abfss://bronze@medalforgestorage.dfs.core.windows.net"
}

# ---------------------------------------------------------------------------

def _parse_args() -> Dict[str, Any]:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract_path", required=True)
    ap.add_argument("--mode", default="validate+plan")
    ap.add_argument("--reprocess_label", default=None)
    ap.add_argument("--include_existing_override", default=None)  # "true" | "false"
    args = ap.parse_args()
    d = vars(args)

    if d.get("include_existing_override") is not None:
        d["include_existing_override"] = str(d["include_existing_override"]).lower() == "true"
    return d

def _normalize_mode(mode: str) -> Tuple[bool, bool, bool]:
    """
    Retorna flags (do_validate, do_plan, do_ingest) a partir de 'mode'.
    Suporta combinações separadas por '+' ou ','.
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
    Deriva RAW (raiz do contêiner), LOCATION (bronze) e checkpoint (bronze).
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
    table_location: str
) -> None:
    tm = TableManager(spark)
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
    include_existing: bool
) -> Dict[str, Any]:
    """Monta um dicionário plano para impressão/log."""
    try:
        schema_repr = json.dumps(payload["schema_struct"].jsonValue())
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

def main():
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    params = _parse_args()

    # Carrega e valida contrato
    with open(params["contract_path"], "r", encoding="utf-8") as f:
        raw = f.read()
    mgr = DataContractManager(raw)
    payload = mgr.as_ingestion_payload()  # fqn, schema_struct, format, reader_options, partitions, column_comments

    # Normaliza modo e defaults D-1
    do_validate, do_plan, do_ingest = _normalize_mode(params.get("mode", "validate+plan"))
    include_existing = True if params.get("include_existing_override") is None else bool(params["include_existing_override"])

    # Deriva caminhos (RAW raiz, BRONZE location/ckpt)
    paths = _build_paths(mgr.catalog, mgr.schema, mgr.table, params.get("reprocess_label"))

    # Sempre garante a tabela antes de qualquer ingestão
    _ensure_table(
        spark=spark,
        mgr=mgr,
        schema_struct=payload["schema_struct"],
        partitions=payload["partitions"],
        table_location=paths["table_location"],
    )

    outputs: Dict[str, Any] = {"status": [], "details": {}}

    # validate
    if do_validate:
        outputs["status"].append("validated")
        outputs["details"]["validate"] = {
            "fqn": mgr.fqn,
            "columns": mgr.column_names,
            "partitions_effective": mgr.effective_partitions,
            "format": payload["format"],
            "reader_options_merged": DataContractManager(mgr._model.dict()).reader_options(),  # mostra merge
        }

    # plan
    if do_plan:
        outputs["status"].append("planned")
        outputs["details"]["plan"] = _plan_dict(mgr, payload, paths, include_existing)

    # ingest
    if do_ingest:
        try:
            ingestor = IngestorFactory.create(
                fmt=payload["format"],
                spark=spark,
                fqn=payload["fqn"],
                schema_struct=payload["schema_struct"],
                partitions=payload["partitions"],
                reader_options=payload["reader_options"],
                source_directory=paths["source_directory"],
                checkpoint_location=paths["checkpointLocation"],
            )
            # gatilho sempre availableNow nos ingestors
            ingestor.ingest(include_existing_files=include_existing)
            outputs["status"].append("ingested")
            outputs["details"]["ingest"] = {
                "table": payload["fqn"],
                "checkpoint": paths["checkpointLocation"],
            }
        except Exception as e:
            print(f"[INGEST][ERROR] {e}", file=sys.stderr)
            raise

    if not outputs["status"]:
        outputs["status"] = ["noop"]
    print(json.dumps(outputs, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
