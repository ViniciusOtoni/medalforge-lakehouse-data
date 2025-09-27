"""
Pipeline Silver (Databricks)

Objetivo
- Orquestrar o fluxo Silver 100% dirigido por contrato:
  * lê Bronze
  * aplica DQX (split válidos/quarentena)
  * persiste quarentena bruta (opcional)
  * remedia, reaplica DQX
  * ETL padrão (core) e customs
  * merge na tabela Silver alvo
  * persiste rejeitados pós-remediação

Observações
- Mantém colunas técnicas do DQX até o momento da remediação/ETL (strip controlado).
- Usa Unity Catalog (garante catálogo/esquema antes de gravar).
"""

import argparse
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import os
import sys
import yaml

sys.path.append(os.getcwd())

from framework.contract_model import SilverYaml
from framework import etl_core
from framework.customs_engine import apply_customs_stage
from dqx.driver import apply_checks_split
from utils.merge import merge_upsert
from utils.uc import split_fqn, ensure_catalog_schema
from monitoring.azure_table_runs import PipelineRunLogger



spark = SparkSession.builder.getOrCreate()
ENV = os.getenv("ENV", "dev")

# ---------------- helpers ----------------

def _run_steps_core(df: DataFrame, steps: List, allow_missing: bool = False) -> DataFrame:
    """
    Aplica, em sequência, steps do ETL "core" referenciando funções de `framework.etl_core`.

    Parâmetros
    - df: DataFrame de entrada.
    - steps: lista de objetos Step (do contrato) com `method` e `args`.
    - allow_missing: se True, ignora métodos não encontrados em `etl_core`; caso contrário, lança erro.

    Retorno
    - DataFrame resultante após a aplicação sequencial de cada step.

    Exceptions
    - ValueError: quando `allow_missing=False` e o método informado não existe em `etl_core`.
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
    """
    Remove colunas técnicas do DQX do DataFrame.

    Regras
    - Remove explicitamente: `_errors`, `_warnings`.
    - Remove qualquer coluna cujo nome comece com `_dqx_`.

    Parâmetros
    - df: DataFrame possivelmente enriquecido pelo DQX.

    Retorno
    - DataFrame sem as colunas técnicas; se nada for removido, retorna o próprio `df`.
    """
    # regra conservadora: tira colunas que certamente são técnicas do DQX
    drop_names = {"_errors", "_warnings"}
    # se preferir ser mais agressivo, também remova tudo que começa com "_dqx_"
    keep = [c for c in df.columns if c not in drop_names and not c.startswith("_dqx_")]
    return df.select(*keep) if set(keep) != set(df.columns) else df

# ---------------- pipeline ----------------

def run(contract_path: str):
    """
    Executa o pipeline Silver conforme contrato YAML.

    Passos
    1) Carrega/valida contrato (SilverYaml).
    2) Lê Bronze (`cfg.source.bronze_table`).
    3) Aplica DQX e separa (válidos, quarentena).
    4) Persiste quarentena bruta (opcional).
    5) Remove colunas DQX, aplica remediação na quarentena e reaplica DQX.
    6) Remove colunas DQX dos válidos; aplica ETL "standard".
    7) Aplica customs (se habilitado) na stage "standard".
    8) Faz union dos válidos (originais + remediados bons).
    9) Garante catálogo/esquema e faz MERGE na Silver.
    10) Persiste rejeitados pós-remediação (se houver).

    Parâmetros
    - contract_path: caminho do arquivo YAML de contrato da Silver.

    Retorno
    - None

    Exceptions
    - Pode propagar erros de parsing do YAML, validação Pydantic, DQX, e operações Spark/Delta.
    """
    raw = yaml.safe_load(open(contract_path, "r", encoding="utf-8"))
    cfg = SilverYaml(**raw)

    # Monitoramento de execução (no-op se libs/ENVs ausentes)
    from monitoring.azure_table_runs import PipelineRunLogger
    env_name = os.getenv("ENV", "dev")
    with PipelineRunLogger(
        env=env_name,
        pipeline="silver",
        schema=cfg.target.schema_name,
        table=cfg.target.table,
        target_fqn=cfg.target_fqn,
        extra={"contract_version": cfg.version},
    ) as _runlog:

        bronze_tbl = cfg.source["bronze_table"]
        df_bronze = spark.table(bronze_tbl)

        # 2) DQX inicial
        valid_df, quarantine_df = apply_checks_split(df_bronze, cfg.dqx.model_dump())

        # 3) Sink da quarentena BRUTA (com motivos). NÃO stripar aqui!
        if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
            sink_tbl = cfg.quarantine.sink["table"]  # ex.: silver.quarantine.sales_bronze_data
            cat, sch, _ = split_fqn(sink_tbl)
            ensure_catalog_schema(cat, sch)

            # grava como EXTERNAL (append)
            from utils.merge import append_external
            append_external(
                quarantine_df,
                sink_tbl,
                external_base="abfss://silver@medalforgestorage.dfs.core.windows.net",
                partition_by=None
            )

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

        def log(msg: str):
            """
            Helper de log com flush imediato (útil para materializar planos ao chamar actions).
            """
            print(msg, flush=True)

         # 8) Union válidos + válidos remediados
        final_df = valid_df.unionByName(fixed_valid_df, allowMissingColumns=True)

        # DEBUG útil (vira ação e força avaliar o plano):
        log(f"valid_df.count()={valid_df.count()} | fixed_valid_df.count()={fixed_valid_df.count()} | final_df.count()={final_df.count()}")
        log(f"final_df schema = {[ (c.name, c.dataType.simpleString()) for c in final_df.schema ]}")

        # 9) Gravar Silver
        tgt = cfg.target_fqn  # ex.: silver.sales.sales_clean

        # **GARANTE catálogo/esquema do ALVO** (antes de salvar/merge)
        cat, sch, _ = split_fqn(tgt)
        ensure_catalog_schema(cat, sch)

        merge_upsert(
            final_df,
            tgt,
            cfg.target.write.merge_keys,
            cfg.target.write.zorder_by,
            partition_by=cfg.target.write.partition_by,   # <— usa particionamento do contrato
            external_base=f"abfss://silver@medalforgestorage.dfs.core.windows.net"
        )

        # sanity check pós-merge (tabela existe?)
        exists = spark.catalog.tableExists(tgt)
        log(f"tableExists({tgt})={exists}")
        if exists:
            cnt = spark.table(tgt).count()
            log(f"{tgt} row_count={cnt}")

        # 10) Rejeitados pós-remediação
        if still_bad_df.count() > 0:
            still_bad_df = _strip_dqx_cols(still_bad_df)
            if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
                cat, sch, _ = split_fqn(cfg.quarantine.sink["table"])
            else:
                cat = cfg.target.catalog
                sch = f"{cfg.target.schema_name}_quarantine"
            ensure_catalog_schema(cat, sch)

            still_tbl = f"{cat}.{sch}.{cfg.target.table}_rejected"

            from utils.merge import append_external
            append_external(
                still_bad_df,
                still_tbl,
                external_base="abfss://silver@medalforgestorage.dfs.core.windows.net",
                partition_by=None
            )
            log(f"rejected saved at {still_tbl} rows={spark.table(still_tbl).count()}")

        # Finaliza run com métricas básicas (no-op se monitor desabilitado)
        try:
            _runlog.finish(
                status="ok",
                counts_valid=int(valid_df.count()),
                counts_quarantine=int(quarantine_df.count()),
                counts_final=int(final_df.count()),
            )
        except Exception:
            # Evita quebrar o pipeline por falha na telemetria
            pass


def _parse_args():
    """
    Parser de argumentos da linha de comando.

    Flags
    - --contract_path (obrigatório): caminho do contrato YAML da Silver.

    Retorno
    - argparse.Namespace com os argumentos parseados.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--contract_path", required=True, help="Caminho do contrato YAML da Silver")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    print(f"[BOOT] Iniciando silver/main.py com contract_path={args.contract_path}", flush=True)
    try:
        run(args.contract_path)
        print("[DONE] silver/main.py finalizado com sucesso", flush=True)
    except Exception as e:
        # garante que o Databricks Jobs marque como FAILED
        import traceback, sys
        print("[ERROR] Falha na execução:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
