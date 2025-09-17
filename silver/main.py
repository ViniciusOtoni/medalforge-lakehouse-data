import argparse
import yaml
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import os
import sys

sys.path.append(os.getcwd())

from framework.contract_model import SilverYaml
from framework import etl_core
from framework.customs_engine import apply_customs_stage
from dqx.driver import apply_checks_split
from utils.merge import merge_upsert
from utils.uc import split_fqn, ensure_catalog_schema


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

# remove colunas técnicas criadas pelo DQX (_errors, _warnings e qualquer prefixo _dqx_)
def _strip_dqx_cols(df: DataFrame) -> DataFrame:
    # regra conservadora: tira colunas que certamente são técnicas do DQX
    drop_names = {"_errors", "_warnings"}
    # se preferir ser mais agressivo, também remova tudo que começa com "_dqx_"
    keep = [c for c in df.columns if c not in drop_names and not c.startswith("_dqx_")]
    return df.select(*keep) if set(keep) != set(df.columns) else df

# ---------------- pipeline ----------------

def run(contract_path: str):
    raw = yaml.safe_load(open(contract_path, "r", encoding="utf-8"))
    cfg = SilverYaml(**raw)

    bronze_tbl = cfg.source["bronze_table"]
    df_bronze = spark.table(bronze_tbl)

    # 2) DQX inicial
    valid_df, quarantine_df = apply_checks_split(df_bronze, cfg.dqx.model_dump())

    # 3) Sink da quarentena BRUTA (com motivos). NÃO stripar aqui!
    if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
        sink_tbl = cfg.quarantine.sink["table"]  # ex: "monitoring.quarantine.sales_bronze_teste"
        cat, sch, _ = split_fqn(sink_tbl)
        ensure_catalog_schema(cat, sch)
        quarantine_df.write.mode("append").saveAsTable(sink_tbl)

    # 4) Remediação: strip antes de rodar ETL de remediação
    if cfg.quarantine.remediate:
        remediated_df = _run_steps_core(_strip_dqx_cols(quarantine_df), cfg.quarantine.remediate)
    else:
        remediated_df = _strip_dqx_cols(quarantine_df)

    # 5) Reaplicar DQX nos remediados: strip ANTES de reaplicar (já fizemos acima)
    fixed_valid_df, still_bad_df = apply_checks_split(remediated_df, cfg.dqx.model_dump())

    # 6) ETL standard nos válidos: strip ANTES do ETL (para não carregar colunas técnicas)
    valid_df = _strip_dqx_cols(valid_df)
    fixed_valid_df = _strip_dqx_cols(fixed_valid_df)

    std_steps = cfg.etl.get("standard", [])
    valid_df = _run_steps_core(valid_df, std_steps)
    fixed_valid_df = _run_steps_core(fixed_valid_df, std_steps)

    # 7) Customs em standard (se houver)
    if cfg.customs.allow:
        valid_df = apply_customs_stage(valid_df, cfg.customs, stage_name="standard")
        fixed_valid_df = apply_customs_stage(fixed_valid_df, cfg.customs, stage_name="standard")

    # 8) Union válidos + válidos remediados
    final_df = valid_df.unionByName(fixed_valid_df, allowMissingColumns=True)

    # 9) Gravar Silver
    tgt = cfg.target_fqn
    cat, sch, _ = split_fqn(tgt)
    ensure_catalog_schema(cat, sch)

    merge_upsert(
        final_df,
        tgt,
        cfg.target.write.merge_keys,
        cfg.target.write.zorder_by,
        partition_by=cfg.target.write.partition_by  
    )

    # 10) Rejeitados pós-remediação: (opcional) strip para não “sujar” a tabela final de rejeitados
    if still_bad_df.count() > 0:
        if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
            cat, sch, _ = split_fqn(cfg.quarantine.sink["table"])
        else:
            # fallback: usa o mesmo catálogo da Silver e um schema derivado
            cat = cfg.target.catalog
            sch = f"{cfg.target.schema_name}_quarantine"

        ensure_catalog_schema(cat, sch)
        still_tbl = f"{cat}.{sch}.{cfg.target.table}_rejected"
        still_bad_df.write.mode("append").saveAsTable(still_tbl)

    print(f"[OK] Silver gravada em {tgt}")

    print(
        "Counts -> valid_df:", valid_df.count(),
        "fixed_valid_df:", fixed_valid_df.count()
    )

    print("final_df:", final_df.count())

