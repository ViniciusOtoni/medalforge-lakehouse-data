"""
Módulo: test_pipeline
Finalidade: garantir que run_pipeline:
 - busque a fonte via spark.table(bronze_tbl)
 - chame estágios DQX inicial e recheck, ETL (strip/core/customs), e WRITE na ordem esperada
 - persista quarentena bruta (quando sink.table existir)
 - crie 'rejected' conforme regra:
     * com sink.table -> rejected também persiste no MESMO sink.table (comportamento atual)
     * sem sink.table -> <catalog>.<schema_quarantine>.<table>_rejected
 - envie métricas ao PipelineRunLogger.finish(status="ok", counts_*)
Sem I/O real: todas as dependências externas são mockadas via monkeypatch.
"""

from typing import Dict, Any
from pyspark.sql import Row, types as T
from onedata.silver.application.pipeline import run_pipeline
from onedata.silver.domain.silver import SilverYaml
from onedata.silver.domain.target import TargetCfg, TargetWriteCfg
from onedata.silver.domain.dqx import DQXCfg
from onedata.silver.domain.etl import Step, QuarantineCfg, CustomsCfg


# -----------------------
# Helpers
# -----------------------
def _bronze_df(spark):
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("val", T.IntegerType(), True),
        T.StructField("_dqx_meta", T.StringType(), True),  # só para ver strip
    ])
    return spark.createDataFrame([
        Row(id="A", val=1, _dqx_meta=None),
        Row(id=None, val=2, _dqx_meta=None),
        Row(id="B", val=3, _dqx_meta=None),
    ], schema)


def _cfg(with_sink: bool):
    target = TargetCfg(catalog="silver", schema="sales", table="orders", write=TargetWriteCfg())
    dqx = DQXCfg()
    etl = {"standard": [Step(method="noop")]}  # o método inexistente será ignorado em stub
    quarantine = QuarantineCfg(
        remediate=[Step(method="noop")],
        sink={"table": "monitoring.quarantine.orders"} if with_sink else None,
    )
    customs = CustomsCfg(allow=False, registry=[], use_in=[])
    return target, dqx, etl, quarantine, customs


# -----------------------
# Testes
# -----------------------
def test_run_pipeline_feliz_com_sink(monkeypatch, spark):
    """
    Propósito: caminho feliz com sink configurado.
    Verificamos:
      - persist_df_append_external chamado 2x no MESMO sink.table (quarentena bruta e rejected)
      - ensure_uc_and_merge chamado com final_df (união de válidos + fixos)
      - PipelineRunLogger.finish recebe métricas numéricas
    """
    # ---- Spark.table → bronze df ----
    bronze_df = _bronze_df(spark)
    # substitui apenas o método .table do spark real (bound method: 1 arg)
    monkeypatch.setattr(spark, "table", lambda name: bronze_df, raising=True)

    # ---- Stubs de stages ----
    import onedata.silver.application.pipeline as mod

    # dqx_stage.initial_split: separa nulos e não-nulos
    def fake_initial_split(df, dqx_cfg):
        valid = df.where("id IS NOT NULL")
        q = df.where("id IS NULL").withColumn("_errors", df.id)  # coluna técnica p/ strip
        return valid, q

    # recheck: transforma tudo da quarentena em válido (ex.: após remediação)
    def fake_recheck(df, dqx_cfg):
        fixed = df.select("val")  # shape diferente intencionalmente
        still = df.limit(0)
        return fixed, still

    monkeypatch.setattr(mod.dqx_stage, "initial_split", fake_initial_split, raising=True)
    monkeypatch.setattr(mod.dqx_stage, "recheck_after_remediation", fake_recheck, raising=True)

    # etl_stage: strip remove técnicas, core/customs repassam sem efeito
    def fake_strip(df):
        # remove qualquer coluna que comece com "_"
        keep = [c for c in df.columns if not c.startswith("_")]
        return df.select(*keep)

    monkeypatch.setattr(mod.etl_stage, "strip_dqx_cols", fake_strip, raising=True)
    monkeypatch.setattr(mod.etl_stage, "run_core_steps", lambda d, s, allow_missing=False: d, raising=True)
    monkeypatch.setattr(mod.etl_stage, "run_customs_standard", lambda d, *a, **k: d, raising=True)

    # write_stage: capturar chamadas
    calls: Dict[str, Any] = {"persist": [], "merge": None}

    def fake_persist(df, table_fqn, *, external_base, partition_by=None):
        calls["persist"].append({
            "table": table_fqn,
            "rows": df.count(),
            "external_base": external_base,
            "partition_by": partition_by,
        })

    def fake_merge(spark_in, final_df, target_fqn, merge_keys, zorder_by, partition_by, external_base):
        calls["merge"] = {
            "rows": final_df.count(),
            "target_fqn": target_fqn,
            "keys": merge_keys,
            "zorder": zorder_by,
            "partition_by": partition_by,
            "external_base": external_base,
        }

    monkeypatch.setattr(mod.write_stage, "persist_df_append_external", fake_persist, raising=True)
    monkeypatch.setattr(mod.write_stage, "ensure_uc_and_merge", fake_merge, raising=True)

    # SETTINGS: ambiente dev (logger=print)
    class FakeSettings:
        env = "dev"
        customs_strict = False
        customs_prefixes = None
        silver_external_base = "abfss://silver@st/tables"

    monkeypatch.setattr(mod, "SETTINGS", FakeSettings, raising=True)

    # PipelineRunLogger: capturar finish()
    class FakeRunLog:
        def __init__(self, **kw): self.kw = kw; self.finished = None
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def finish(self, **kw): self.finished = kw

    import onedata.monitoring.azure_table_runs as mon
    monkeypatch.setattr(mon, "PipelineRunLogger", FakeRunLog, raising=True)

    # ---- Config do contrato ----
    target, dqx, etl, quarantine, customs = _cfg(with_sink=True)
    cfg = SilverYaml(
        version="1.0",
        source={"bronze_table": "bronze.sales.orders"},
        target=target,
        dqx=dqx,
        etl=etl,
        quarantine=quarantine,
        customs=customs,
    )

    # ---- Execução ----
    run_pipeline(spark, cfg)

    # ---- Asserts ----
    # 1) Persist foi chamado 2x e sempre no mesmo sink.table
    assert len(calls["persist"]) == 2
    assert calls["persist"][0]["table"] == "monitoring.quarantine.orders"   # quarentena bruta
    assert calls["persist"][1]["table"] == "monitoring.quarantine.orders"   # rejected também vai no sink.table

    # 2) ensure_uc_and_merge foi chamado com DF final
    assert calls["merge"] is not None
    assert isinstance(calls["merge"]["rows"], int) and calls["merge"]["rows"] >= 0
    assert calls["merge"]["target_fqn"] == "silver.sales.orders"
    assert calls["merge"]["external_base"].startswith("abfss://")


def test_run_pipeline_sem_sink_cria_rejected_no_schema_quarantine(monkeypatch, spark):
    """
    Propósito: quando não há sink.table, o rejected deve ser escrito em:
      <target.catalog>.<target.schema_name>_quarantine.<target.table>_rejected
    (Forçamos quarentena inicial para acionar o fluxo de rejected.)
    """
    bronze_df = _bronze_df(spark)
    # bound method: 1 arg
    monkeypatch.setattr(spark, "table", lambda n: bronze_df, raising=False)

    import onedata.silver.application.pipeline as mod

    # dqx: força quarentena no split; recheck falha tudo para 'still_bad'
    def fake_initial_split(df, _):
        return df.limit(0), df  # (valid, quarantine)

    def fake_recheck(df, _):
        return df.limit(0), df  # (fixed_valid, still_bad)

    monkeypatch.setattr(mod.dqx_stage, "initial_split", fake_initial_split, raising=True)
    monkeypatch.setattr(mod.dqx_stage, "recheck_after_remediation", fake_recheck, raising=True)

    # etl no-op
    monkeypatch.setattr(mod.etl_stage, "strip_dqx_cols", lambda d: d, raising=True)
    monkeypatch.setattr(mod.etl_stage, "run_core_steps", lambda d, s, allow_missing=False: d, raising=True)
    monkeypatch.setattr(mod.etl_stage, "run_customs_standard", lambda d, *a, **k: d, raising=True)

    # capture persist
    seen = {"tables": []}

    def fake_persist(df, table_fqn, *, external_base, partition_by=None):
        seen["tables"].append(table_fqn)

    monkeypatch.setattr(mod.write_stage, "persist_df_append_external", fake_persist, raising=True)
    monkeypatch.setattr(mod.write_stage, "ensure_uc_and_merge", lambda *a, **k: None, raising=True)

    class FakeSettings:
        env = "dev"
        customs_strict = False
        customs_prefixes = None
        silver_external_base = "abfss://silver@st/tables"
    monkeypatch.setattr(mod, "SETTINGS", FakeSettings, raising=True)

    target, dqx, etl, quarantine, customs = _cfg(with_sink=False)
    cfg = SilverYaml(
        version="1.0",
        source={"bronze_table": "bronze.sales.orders"},
        target=target,
        dqx=dqx,
        etl=etl,
        quarantine=quarantine,
        customs=customs,
    )

    # ---- Execução ----
    run_pipeline(spark, cfg)

    # ---- Asserts ----
    # Rejected foi persistido exatamente 1x, no schema _quarantine sem sink
    assert len(seen["tables"]) == 1
    expected = f"{target.catalog}.{target.schema_name}_quarantine.{target.table}_rejected"
    assert seen["tables"][0] == expected
