"""
Módulo: test_dqx_stage
Finalidade: garantir que as funções do estágio DQX
 - chamem o driver com o dqx_cfg serializado (model_dump())
 - repassem e retornem exatamente os dois DataFrames do driver
 - funcionem igualmente no split inicial e no recheck após remediação
"""

from pyspark.sql import Row, types as T
from onedata.silver.stage.dqx_stage import initial_split, recheck_after_remediation
from onedata.silver.domain.dqx import DQXCfg


def test_initial_split_usa_model_dump_e_retorna_tupla_df(monkeypatch, spark):
    """Propósito: validar serialização do DQXCfg e repasse ao driver no split inicial."""
    # DF simples
    df = spark.createDataFrame([Row(id="A"), Row(id=None)], T.StructType([T.StructField("id", T.StringType(), True)]))

    seen_cfg = {}

    def fake_apply_checks_split(df_in, cfg_dict):
        # captura o dict recebido e devolve um split determinístico
        seen_cfg["value"] = cfg_dict
        valid = df_in.where("id IS NOT NULL")
        quarantine = df_in.where("id IS NULL")
        return valid, quarantine

    import onedata.silver.dqx.driver as driver_mod
    monkeypatch.setattr(driver_mod, "apply_checks_split", fake_apply_checks_split, raising=True)

    dqx = DQXCfg()  # defaults (error, checks=[], custom=[])
    valid_df, quarantine_df = initial_split(df, dqx)

    # Asserts
    assert valid_df.count() == 1 and quarantine_df.count() == 1
    assert isinstance(seen_cfg["value"], dict)
    assert "checks" in seen_cfg["value"] and "custom" in seen_cfg["value"]


def test_recheck_after_remediation_reaplica_driver(monkeypatch, spark):
    """Propósito: revalidar após remediação chamando o mesmo driver com model_dump()."""
    df = spark.createDataFrame([Row(x=1)], T.StructType([T.StructField("x", T.IntegerType())]))

    called = {"n": 0}

    def fake_apply_checks_split(df_in, cfg_dict):
        called["n"] += 1
        return df_in, df_in.limit(0)  # tudo válido

    import onedata.silver.dqx.driver as driver_mod
    monkeypatch.setattr(driver_mod, "apply_checks_split", fake_apply_checks_split, raising=True)

    dqx = DQXCfg()
    valid_df, quarantine_df = recheck_after_remediation(df, dqx)

    assert called["n"] == 1
    assert valid_df.count() == 1 and quarantine_df.count() == 0
