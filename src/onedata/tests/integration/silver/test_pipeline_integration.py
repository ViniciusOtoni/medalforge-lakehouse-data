"""
Módulo: test_pipeline_integration
Finalidade (integração): validar o orquestrador Silver (application/pipeline.py)
de ponta a ponta, exercitando as etapas DQX → ETL → WRITE com stubs/minimocks
e conferindo os efeitos esperados (persistências de quarentena, rejected,
merge final, contagens de métricas).

Estratégia:
- Usamos Spark real apenas para criar DataFrames.
- Evitamos IO/UC/Delta/Databricks: monkeypatch para dqx_stage/etl_stage/write_stage/PipelineRunLogger/SETTINGS.
- Exercitamos dois cenários:
  1) Com sink de quarentena configurado (escreve quarentena bruta + *_rejected herdando cat/schema do sink).
  2) Sem sink (escreve *_rejected em <target.catalog>.<target.schema>_quarantine).
"""

from typing import Dict, Any
import types
import importlib

from pyspark.sql import Row, types as T

from onedata.silver.application.pipeline import run_pipeline
from onedata.silver.domain.silver import SilverYaml
from onedata.silver.domain.target import TargetCfg, TargetWriteCfg
from onedata.silver.domain.dqx import DQXCfg
from onedata.silver.domain.etl import Step, QuarantineCfg, CustomsCfg


# -----------------------
# Helpers de contrato/DF
# -----------------------
def _bronze_df(spark):
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("val", T.IntegerType(), True),
        T.StructField("_dqx_meta", T.StringType(), True),
    ])
    return spark.createDataFrame(
        [Row(id="A", val=1, _dqx_meta=None),  # válido
         Row(id=None, val=2, _dqx_meta=None), # vai para quarentena
         Row(id="B", val=3, _dqx_meta=None)], # válido
        schema
    )


def _silver_cfg(with_sink: bool) -> SilverYaml:
    """
    Propósito: construir um contrato SilverYaml mínimo e válido para os testes.
    """
    target = TargetCfg(catalog="silver", schema="sales", table="orders", write=TargetWriteCfg())
    dqx = DQXCfg()
    etl = {"standard": [Step(method="noop")]}  # será ignorado pelos stubs
    quarantine = QuarantineCfg(
        remediate=[Step(method="noop")],  # será ignorado em stub
        sink={"table": "monitoring.quarantine.orders"} if with_sink else None
    )
    customs = CustomsCfg(allow=False, registry=[], use_in=[])
    return SilverYaml(
        version="1.0",
        source={"bronze_table": "bronze.sales.orders"},
        target=target,
        dqx=dqx,
        etl=etl,
        quarantine=quarantine,
        customs=customs,
    )


# -----------------------
# Cenário 1: com sink
# -----------------------
def test_pipeline_e2e_com_sink(monkeypatch, spark):
    """
    Propósito: cenário completo com sink de quarentena:
      - DQX inicial separa válidos vs. quarentena
      - Persist da quarentena bruta no sink.table
      - Remediação + recheck: tudo da quarentena vira válido (still_bad vazio)
      - ETL standard/customs no-op
      - União final -> merge (ensure_uc_and_merge) com contagem correta
      - Envia métricas ao PipelineRunLogger.finish
      - *_rejected herdado do cat/schema do sink (mas aqui still_bad é vazio ⇒ sem persist rejected)
    """
    # Spark.table → df da Bronze
    bronze_df = _bronze_df(spark)
    monkeypatch.setattr(spark, "table", lambda name: bronze_df, raising=False)

    # Import do módulo alvo para patchar dependências internas dele
    import onedata.silver.application.pipeline as app

    # ---- DQX stage stubs ----
    def fake_initial_split(df, dqx_cfg):
        valid = df.where("id IS NOT NULL")
        quarantine = df.where("id IS NULL").withColumn("_errors", df.id)
        return valid, quarantine

    def fake_recheck_after_remediation(df, dqx_cfg):
        # Tudo corrigido: vira válido; ainda-ruins = vazio
        fixed_valid = df.select("val")   # shape diferente para exercitar unionByName
        still_bad = df.limit(0)
        return fixed_valid, still_bad

    monkeypatch.setattr(app.dqx_stage, "initial_split", fake_initial_split, raising=True)
    monkeypatch.setattr(app.dqx_stage, "recheck_after_remediation", fake_recheck_after_remediation, raising=True)

    # ---- ETL stage stubs ----
    monkeypatch.setattr(app.etl_stage, "strip_dqx_cols", lambda df: df.select([c for c in df.columns if not c.startswith("_")]), raising=True)
    monkeypatch.setattr(app.etl_stage, "run_core_steps", lambda df, steps, allow_missing=False: df, raising=True)
    monkeypatch.setattr(app.etl_stage, "run_customs_standard", lambda df, *a, **k: df, raising=True)

    # ---- WRITE stage stubs (captura de chamadas) ----
    calls: Dict[str, Any] = {"persist": [], "merge": None}

    def fake_persist_df_append_external(df, table_fqn, *, external_base, partition_by=None):
        calls["persist"].append({
            "table": table_fqn,
            "rows": df.count(),
            "external_base": external_base,
            "partition_by": partition_by,
        })

    def fake_ensure_uc_and_merge(spark_in, final_df, target_fqn, merge_keys, zorder_by, partition_by, external_base):
        calls["merge"] = {
            "rows": final_df.count(),
            "target_fqn": target_fqn,
            "keys": merge_keys,
            "zorder": zorder_by,
            "partition_by": partition_by,
            "external_base": external_base,
        }

    monkeypatch.setattr(app.write_stage, "persist_df_append_external", fake_persist_df_append_external, raising=True)
    monkeypatch.setattr(app.write_stage, "ensure_uc_and_merge", fake_ensure_uc_and_merge, raising=True)

    # ---- SETTINGS e RunLogger ----
    class FakeSettings:
        env = "dev"
        customs_strict = False
        customs_prefixes = None
        silver_external_base = "abfss://silver@acc/tables"

    monkeypatch.setattr(app, "SETTINGS", FakeSettings, raising=True)

    class FakeRunLogger:
        def __init__(self, **kw): self.kw=kw; self.finished=None
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def finish(self, **kw): self.finished = kw

    import onedata.monitoring.azure_table_runs as mon
    monkeypatch.setattr(mon, "PipelineRunLogger", FakeRunLogger, raising=True)

    # ---- Executa ----
    cfg = _silver_cfg(with_sink=True)
    run_pipeline(spark, cfg)

    # ---- Asserts ----
    # 1) Persist de quarentena bruta no sink.table
    assert len(calls["persist"]) == 1
    assert calls["persist"][0]["table"] == "monitoring.quarantine.orders"
    # 2) Merge final: válidos (2 linhas) + fixed_valid (1 linha) = 3
    assert calls["merge"]["rows"] == 3
    assert calls["merge"]["target_fqn"] == "silver.sales.orders"
    assert calls["merge"]["external_base"].startswith("abfss://")
    # 3) Métricas emitidas (status ok e contagens numéricas)
    #    counts_valid = 2; counts_quarantine = 1; counts_final = 3
    #    (finish é chamado dentro do contexto; aqui só garantimos que não explodiu e foi preenchido)
    #    Como não retemos a instância, não validamos diretamente — foco é a orquestração externa.


# -----------------------
# Cenário 2: sem sink
# -----------------------
def test_pipeline_e2e_sem_sink_cria_rejected_em_schema_quarantine(monkeypatch, spark):
    """
    Propósito: quando não há sink.table configurado:
      - DQX inicial pode não gerar quarentena persistida
      - Após remediação, ainda-ruins devem ser gravados em:
        <target.catalog>.<target.schema_name>_quarantine.<target.table>_rejected
    """
    bronze_df = _bronze_df(spark)
    monkeypatch.setattr(spark, "table", lambda name: bronze_df, raising=False)

    import onedata.silver.application.pipeline as app

    # DQX inicial: tudo válido para simplificar (não haverá persist da quarentena bruta)
    monkeypatch.setattr(app.dqx_stage, "initial_split", lambda df, _: (df, df.limit(0)), raising=True)
    # Recheck: tudo ainda ruim → aciona persist do rejected
    def fake_recheck(df, _):
        still = bronze_df.where("id IS NULL")  # 1 linha ruim
        return df.limit(0), still
    monkeypatch.setattr(app.dqx_stage, "recheck_after_remediation", fake_recheck, raising=True)

    # ETL no-op
    monkeypatch.setattr(app.etl_stage, "strip_dqx_cols", lambda df: df, raising=True)
    monkeypatch.setattr(app.etl_stage, "run_core_steps", lambda df, steps, allow_missing=False: df, raising=True)
    monkeypatch.setattr(app.etl_stage, "run_customs_standard", lambda df, *a, **k: df, raising=True)

    # Captura writes
    seen = {"tables": []}
    monkeypatch.setattr(app.write_stage, "persist_df_append_external", lambda df, table_fqn, *, external_base, partition_by=None: seen["tables"].append(table_fqn), raising=True)
    monkeypatch.setattr(app.write_stage, "ensure_uc_and_merge", lambda *a, **k: None, raising=True)

    # SETTINGS
    class FakeSettings:
        env = "dev"
        customs_strict = False
        customs_prefixes = None
        silver_external_base = "abfss://silver@acc/tables"
    monkeypatch.setattr(app, "SETTINGS", FakeSettings, raising=True)

    cfg = _silver_cfg(with_sink=False)
    run_pipeline(spark, cfg)

    # Apenas um persist: o rejected (sem quarentena bruta)
    assert seen["tables"] == ["silver.sales_quarantine.orders_rejected"]
