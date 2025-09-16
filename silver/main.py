import argparse
import yaml
from typing import List
from pyspark.sql import SparkSession, DataFrame
import os
import sys

sys.path.append(os.getcwd())

from framework.contract_model import SilverYaml
from framework import etl_core
from framework.customs_engine import apply_customs_stage
from dqx.driver import apply_checks_split
from utils.merge import merge_upsert

spark = SparkSession.builder.getOrCreate()

# ---------------- helpers ----------------

def _run_steps_core(df: DataFrame, steps: List, allow_missing: bool = False) -> DataFrame:
    """
    Executa steps (core ou internos) referenciando funções do etl_core.
    """
    for s in steps:
        method = s.method
        args = dict(s.args or {})
        if not hasattr(etl_core, method):
            if allow_missing:
                continue
            raise ValueError(f"Método core desconhecido: {method}")
        fn = getattr(etl_core, method)
        df = fn(df, **args)
    return df

# ---------------- pipeline ----------------

def run(contract_path: str):
    raw = yaml.safe_load(open(contract_path, "r", encoding="utf-8"))
    cfg = SilverYaml(**raw)

    # 1) Ler Bronze
    bronze_tbl = cfg.source["bronze_table"]
    df_bronze = spark.table(bronze_tbl)

    # 2) DQX inicial
    valid_df, quarantine_df = apply_checks_split(df_bronze, cfg.dqx.model_dump())

    # 3) Sink opcional da quarentena bruta
    if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
        quarantine_df.write.mode("append").saveAsTable(cfg.quarantine.sink["table"])

    # 4) Remediação (quarantine.remediate)
    if cfg.quarantine.remediate:
        remediated_df = _run_steps_core(quarantine_df, cfg.quarantine.remediate)
    else:
        remediated_df = quarantine_df

    # 5) Reaplicar DQX nos remediados
    fixed_valid_df, still_bad_df = apply_checks_split(remediated_df, cfg.dqx.model_dump())

    # 6) ETL standard nos válidos
    std_steps = cfg.etl.get("standard", [])
    valid_df = _run_steps_core(valid_df, std_steps)
    fixed_valid_df = _run_steps_core(fixed_valid_df, std_steps)

    # 7) Customs em standard (se houver)
    if cfg.customs.allow:
        valid_df = apply_customs_stage(valid_df, cfg.customs, stage_name="standard")
        fixed_valid_df = apply_customs_stage(fixed_valid_df, cfg.customs, stage_name="standard")

    # 8) Union válidos + válidos remediados
    final_df = valid_df.unionByName(fixed_valid_df, allowMissingColumns=True)

    # 9) Gravar Silver (merge/upsert)
    tgt = cfg.target_fqn
    merge_upsert(final_df, tgt, cfg.target.write.merge_keys, cfg.target.write.zorder_by)

    # 10) Persistir os que ainda falharam após remediação
    if still_bad_df.count() > 0:
        still_tbl = f"monitoring.quarantine.{cfg.target.table}_rejected"
        still_bad_df.write.mode("append").saveAsTable(still_tbl)

    print(f"[OK] Silver gravada em {tgt}")

# ---------------- cli ----------------

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--contract_path", required=True, help="Caminho do contrato YAML da Silver")
    args = p.parse_args()
    run(args.contract_path)
