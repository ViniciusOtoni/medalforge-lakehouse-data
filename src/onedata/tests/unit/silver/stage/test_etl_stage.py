"""
Módulo: test_etl_stage
Finalidade: validar o estágio ETL (strip de colunas DQX, steps core e customs):
 - strip_dqx_cols remove _errors/_warnings e colunas com prefixo _dqx_
 - run_core_steps aplica funções do core por nome e respeita allow_missing
 - run_customs_standard delega ao runner quando enabled, mantendo parâmetros
"""

import pytest
from pyspark.sql import Row, functions as F, types as T
from onedata.silver.stage.etl_stage import strip_dqx_cols, run_core_steps, run_customs_standard
from onedata.silver.domain.etl import Step, CustomsCfg, CustomDecl


def _df(spark):
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("_errors", T.StringType(), True),
        T.StructField("_warnings", T.StringType(), True),
        T.StructField("_dqx_score", T.IntegerType(), True),
    ])
    return spark.createDataFrame([Row(id="A1", name="  Ana  ", _errors=None, _warnings=None, _dqx_score=1)], schema)


def test_strip_dqx_cols_remove_colunas_tecnicas(spark):
    """Propósito: garantir remoção de _errors, _warnings e prefixo '_dqx_'."""
    df = _df(spark)
    out = strip_dqx_cols(df)
    cols = set(out.columns)
    assert "_errors" not in cols and "_warnings" not in cols and "_dqx_score" not in cols
    assert {"id", "name"}.issubset(cols)


def test_run_core_steps_aplica_funcoes_por_nome(monkeypatch, spark):
    """
    Propósito: aplicar sequência de valores com funções do módulo etl_core por nome.
    Cenário: trim_columns seguido de cast_columns.
    """
    df = _df(spark)

    # stubs de core
    def fake_trim(df_in, columns):
        return df_in.withColumn("name", F.trim("name")) if "name" in columns else df_in

    def fake_cast(df_in, mapping):
        if "id" in mapping and mapping["id"] == "string":
            return df_in.withColumn("id", F.concat(F.col("id"), F.lit("_ok")))
        return df_in

    import onedata.silver.etl.core as etl_core
    monkeypatch.setattr(etl_core, "trim_columns", fake_trim, raising=True)
    monkeypatch.setattr(etl_core, "cast_columns", fake_cast, raising=True)

    steps = [
        Step(method="trim_columns", args={"columns": ["name"]}),
        Step(method="cast_columns", args={"mapping": {"id": "string"}}),
    ]

    out = run_core_steps(df, steps)
    r = out.select("id", "name").first()
    assert r.id.endswith("_ok") and r.name == "Ana"


def test_run_core_steps_erro_quando_metodo_inexistente_e_allow_missing_false(spark):
    """Propósito: quando método não existe e allow_missing=False, deve lançar ValueError."""
    df = _df(spark)
    with pytest.raises(ValueError):
        run_core_steps(df, [Step(method="nao_existe")], allow_missing=False)


def test_run_core_steps_ignora_metodo_inexistente_com_allow_missing_true(spark):
    """Propósito: quando allow_missing=True, steps desconhecidos são ignorados."""
    df = _df(spark)
    out = run_core_steps(df, [Step(method="nao_existe")], allow_missing=True)
    assert out.collect() == df.collect()


def test_run_customs_standard_delega_para_runner_com_parametros(monkeypatch, spark):
    """
    Propósito: validar que quando customs.allow=True, o estágio chama apply_customs_stage com:
     - stage_name='standard'
     - allow_module_prefixes e require_marked_decorator conforme repassados
     - logger (quando fornecido)
    """
    called = {}

    def fake_apply_customs_stage(df_in, customs_cfg, stage_name, allow_module_prefixes, require_marked_decorator, logger=None):
        called["stage_name"] = stage_name
        called["allow_module_prefixes"] = tuple(allow_module_prefixes) if allow_module_prefixes else None
        called["require_marked_decorator"] = require_marked_decorator
        called["logger_present"] = logger is not None
        return df_in.withColumn("flag", F.lit(1))

    import onedata.silver.customs.runner as runner_mod
    monkeypatch.setattr(runner_mod, "apply_customs_stage", fake_apply_customs_stage, raising=True)

    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="x", module="custom_mod", method="fn")],
        use_in=[]
    )

    df = _df(spark)
    out = run_customs_standard(
        df,
        customs=cfg,
        allow_module_prefixes=("custom_",),
        require_marked_decorator=True,
        logger=lambda m: None,
    )

    assert "flag" in out.columns
    assert called["stage_name"] == "standard"
    assert called["allow_module_prefixes"] == ("custom_",)
    assert called["require_marked_decorator"] is True
    assert called["logger_present"] is True


def test_run_customs_standard_quando_disabled_retorna_df_inalterado(spark):
    """Propósito: com customs.allow=False, nenhum runner é chamado e o DF não muda."""
    cfg = CustomsCfg(allow=False, registry=[], use_in=[])
    df = _df(spark)
    out = run_customs_standard(
        df,
        customs=cfg,
        allow_module_prefixes=("custom_",),
        require_marked_decorator=True,
    )
    assert out.collect() == df.collect()
