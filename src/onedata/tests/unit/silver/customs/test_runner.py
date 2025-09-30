"""
Módulo: test_runner
Propósito: cobrir o fluxo de apply_customs_stage com foco em:
 - no-op quando cfg ausente/desligado
 - aplicação sequencial por stage
 - validação de args (ok/erro)
 - registro duplicado / custom desconhecido
 - enforcement de prefixo e decorator
 - propagação de falhas do custom (wrapped em RuntimeError)
 - retorno deve ser DataFrame
"""

import pytest
from dataclasses import dataclass
from pyspark.sql import functions as F, types as T, Row
from onedata.silver.customs.sdk import custom as mark
from onedata.silver.customs.runner import apply_customs_stage


# Modelos mínimos compatíveis com os atributos que o runner usa.
@dataclass
class CustomDecl:
    name: str
    module: str
    method: str
    args_schema: dict | None = None

@dataclass
class Step:
    stage: str
    method: str
    args: dict | None = None

@dataclass
class CustomsCfg:
    allow: bool
    registry: list[CustomDecl] | None
    use_in: list[Step] | None


def _df(spark):
    schema = T.StructType([T.StructField("id", T.StringType()), T.StructField("amount", T.IntegerType())])
    return spark.createDataFrame([Row(id="A", amount=50), Row(id="B", amount=99999)], schema)


def test_noop_quando_cfg_nula_ou_desligada(spark):
    """Se cfg é None, allow=False ou use_in vazio, runner retorna o próprio DF (no-op)."""
    df = _df(spark)
    same = apply_customs_stage(df, None, "silver")  # cfg None
    assert same is df

    cfg_off = CustomsCfg(allow=False, registry=[], use_in=[])
    same2 = apply_customs_stage(df, cfg_off, "silver")
    assert same2 is df


def test_aplica_por_stage_e_valida_args_ok(spark, make_module):
    """
    Aplica um único custom no stage 'silver', validando coerce e max.
    Função 'cap_amount' aplica clamp em 'amount'.
    """
    @mark
    def cap_amount(df, cap: int):
        return df.withColumn("amount", F.when(F.col("amount") > F.lit(cap), F.lit(cap)).otherwise(F.col("amount")))

    make_module("custom_x", {"cap_amount": cap_amount})

    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(
            name="cap",
            module="custom_x",
            method="cap_amount",
            args_schema={"cap": {"type": "integer", "coerce": True, "min": 0, "max": 20000}},
        )],
        use_in=[Step(stage="silver", method="cap", args={"cap": "20000"})],
    )

    out = apply_customs_stage(_df(spark), cfg, "silver")
    mx = out.agg(F.max("amount").alias("m")).collect()[0]["m"]
    assert mx == 20000


def test_ignora_steps_de_outro_stage(spark, make_module):
    """Steps com stage diferente não devem executar."""
    @mark
    def bump(df, inc: int):
        return df.withColumn("amount", F.col("amount") + F.lit(inc))

    make_module("custom_y", {"bump": bump})

    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="b", module="custom_y", method="bump", args_schema={"inc": {"type": "integer"}})],
        use_in=[Step(stage="bronze", method="b", args={"inc": 10})],  # outro stage
    )
    df = _df(spark)
    out = apply_customs_stage(df, cfg, "silver")
    assert out.collect() == df.collect()  # não alterado


def test_registry_duplicado_e_custom_desconhecido(spark):
    """Detecta nomes duplicados no registry e erro quando step aponta para método inexistente."""
    cfg_dup = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="x", module="m", method="a"), CustomDecl(name="x", module="m2", method="b")],
        use_in=[Step(stage="silver", method="x")],
    )
    with pytest.raises(ValueError) as e1:
        apply_customs_stage(_df(spark), cfg_dup, "silver")
    assert "duplicatas" in str(e1.value)

    cfg_unknown = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="x", module="m", method="a")],
        use_in=[Step(stage="silver", method="y")],
    )
    with pytest.raises(ValueError) as e2:
        apply_customs_stage(_df(spark), cfg_unknown, "silver")
    assert "não registrado" in str(e2.value)


def test_args_invalidos_propagam_mensagem_do_validator(spark, make_module):
    """Erros de args vêm do validator e devem aparecer embrulhados com contexto do custom."""
    @mark
    def ok(df, n: int):  # nunca executa; erro vem antes
        return df

    make_module("custom_z", {"ok": ok})
    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="ok", module="custom_z", method="ok", args_schema={"n": {"type": "integer"}})],
        use_in=[Step(stage="silver", method="ok", args={"n": "not-int"})],
    )
    with pytest.raises(TypeError) as e:
        apply_customs_stage(_df(spark), cfg, "silver")
    assert "Args inválidos em 'ok' (custom_z.ok):" in str(e.value)


def test_retorno_deve_ser_dataframe(spark, make_module):
    """Se o custom não retornar DataFrame, o runner deve falhar com RuntimeError informativo."""
    @mark
    def bad(df):
        return 123  # errado

    make_module("custom_badret", {"bad": bad})
    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="bad", module="custom_badret", method="bad")],
        use_in=[Step(stage="silver", method="bad")],
    )
    with pytest.raises(RuntimeError) as e:
        apply_customs_stage(_df(spark), cfg, "silver")
    assert "retornou <class 'int'>" in str(e.value)


def test_falha_do_custom_e_wrap_em_runtimeerror(spark, make_module):
    """Exceções dentro do custom devem ser wrapadas em RuntimeError com contexto módulo.método."""
    @mark
    def boom(df):
        raise ValueError("estouro")

    make_module("custom_boom", {"boom": boom})
    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="boom", module="custom_boom", method="boom")],
        use_in=[Step(stage="silver", method="boom")],
    )
    with pytest.raises(RuntimeError) as e:
        apply_customs_stage(_df(spark), cfg, "silver")
    s = str(e.value)
    assert "Falha no custom 'boom' (custom_boom.boom): estouro" in s


def test_enforcement_prefixo_e_decorator(spark, make_module):
    """Runner deve respeitar allow_module_prefixes e require_marked_decorator=True."""
    # módulo sem prefixo permitido
    def raw(df):  # não marcado
        return df
    make_module("plain_mod", {"raw": raw})

    cfg1 = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="raw", module="plain_mod", method="raw")],
        use_in=[Step(stage="silver", method="raw")],
    )
    with pytest.raises(PermissionError):
        apply_customs_stage(_df(spark), cfg1, "silver", allow_module_prefixes=("custom_",))

    # módulo OK mas sem marcação exigida
    make_module("custom_mod", {"raw": raw})
    cfg2 = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="raw", module="custom_mod", method="raw")],
        use_in=[Step(stage="silver", method="raw")],
    )
    with pytest.raises(PermissionError):
        apply_customs_stage(_df(spark), cfg2, "silver", allow_module_prefixes=("custom_",), require_marked_decorator=True)
