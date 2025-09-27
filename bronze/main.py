"""
Ponto de entrada do runner Bronze (CLI).
Delega a orquestração para BronzeOrchestrator.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
from typing import Any, Dict

from pyspark.sql import SparkSession

from .models import RunnerConfig, BaseLocations
from .orchestrator import BronzeOrchestrator


def _parse_args() -> Dict[str, Any]:
    """
    Lê os argumentos de CLI e normaliza tipos simples.

    Retorno
    -------
    dict
        {
          "contract_path": str,
          "mode": str,
          "reprocess_label": str | None,
          "include_existing_override": bool | None
        }
    """
    parser = argparse.ArgumentParser(description="Runner Bronze (validate/plan/ingest)")
    parser.add_argument("--contract_path", required=True, help="Caminho do contrato JSON (TableContract).")
    parser.add_argument("--mode", default="validate+plan", help="Modo: validate|plan|ingest combináveis com '+'.")
    parser.add_argument("--reprocess_label", default=None, help="Sufixo para isolar checkpoint (ex.: 'ingest2024_01_01').")
    parser.add_argument("--include_existing_override", default=None, help="'true'|'false' força leitura de backlog.")

    args = parser.parse_args()
    result = vars(args)

    if result.get("include_existing_override") is not None:
        val = str(result["include_existing_override"]).lower()
        result["include_existing_override"] = (val == "true")
    return result


def main() -> None:
    """
    Ponto de entrada do runner Bronze.

    Fluxo
    -----
    - Lê contrato do caminho informado.
    - Constrói RunnerConfig e BaseLocations (env-var friendly).
    - Instancia e executa BronzeOrchestrator.
    - Imprime JSON com 'status' e 'details'.
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    params = _parse_args()

    # ENV e locations (permite override por variáveis de ambiente)
    env = os.getenv("ENV", "dev")
    raw_root = os.getenv("RAW_ROOT", "abfss://raw@medalforgestorage.dfs.core.windows.net").rstrip("/")
    bronze_root = os.getenv("BRONZE_ROOT", "abfss://bronze@medalforgestorage.dfs.core.windows.net").rstrip("/")

    base = BaseLocations(raw_root=raw_root, bronze_root=bronze_root)
    cfg = RunnerConfig(
        mode=params.get("mode", "validate+plan"),
        include_existing_files=(
            params["include_existing_override"]
            if params.get("include_existing_override") is not None
            else True  # D-1 default
        ),
        reprocess_label=params.get("reprocess_label"),
        env=env,
    )

    with open(params["contract_path"], "r", encoding="utf-8") as f:
        contract_json = f.read()

    orch = BronzeOrchestrator(spark=spark, base_locations=base, env=env)
    outputs = orch.run(contract_json=contract_json, cfg=cfg)

    print(json.dumps(obj=outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    sys.exit(main())
