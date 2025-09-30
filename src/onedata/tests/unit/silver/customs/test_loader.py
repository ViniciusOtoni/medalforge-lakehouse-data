"""
Módulo: test_loader
Propósito: validar o carregamento e as salvaguardas de customs/loader.py:
 - prefixo de módulo permitido
 - exigência do decorator de marcação
 - assinatura do método (primeiro parâmetro 'df')
 - erros de import/método
"""

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T
from onedata.silver.customs.loader import load_custom, _is_marked_custom
from onedata.silver.customs.sdk import custom as mark


def test__is_marked_custom_detecta_flag():
    """Confirma que o decorator insere __onedata_custom__ = True."""
    @mark
    def fn(df):  # pragma: no cover (simple marker)
        return df
    assert _is_marked_custom(fn) is True


def test_load_custom_happy_path_com_prefixo_e_marcacao(spark, make_module):
    """Carrega função válida: módulo com prefixo permitido e função marcada como custom."""
    @mark
    def good(df, **kwargs):
        return df

    mod = make_module("custom_xforms", {"cap": good})

    fn = load_custom(
        module="custom_xforms",
        method="cap",
        allow_module_prefixes=("custom_",),
        require_marked_decorator=True,
    )
    # smoke-call
    df = spark.createDataFrame([Row(x=1)], T.StructType([T.StructField("x", T.IntegerType())]))
    out = fn(df)
    assert out.collect() == df.collect()


def test_load_custom_bloqueia_prefixo_nao_permitido(make_module):
    """Recusa import se módulo não tiver o prefixo listado."""
    make_module("evil_mod", {"noop": lambda df: df})
    with pytest.raises(PermissionError):
        load_custom("evil_mod", "noop", allow_module_prefixes=("custom_",))


def test_load_custom_import_errors_e_missing_method(make_module):
    """Mensagens claras quando módulo não importa ou quando método não existe."""
    with pytest.raises(ImportError):
        load_custom("mod_que_nao_existe", "fn")

    make_module("custom_sample", {"fn_a": lambda df: df})
    with pytest.raises(AttributeError):
        load_custom("custom_sample", "fn_b", allow_module_prefixes=("custom_",))


def test_load_custom_exige_marcacao_quando_solicitado(make_module):
    """Se require_marked_decorator=True, função sem marca deve ser recusada."""
    def raw(df):  # não marcado
        return df
    make_module("custom_plain", {"raw": raw})
    with pytest.raises(PermissionError):
        load_custom("custom_plain", "raw", allow_module_prefixes=("custom_",), require_marked_decorator=True)


def test_load_custom_assinatura_primeiro_parametro_df(make_module):
    """Primeiro parâmetro deve se chamar 'df' — caso contrário, TypeError."""
    @mark
    def bad(x):  # errado: deveria ser df
        return x
    make_module("custom_sig", {"bad": bad})
    with pytest.raises(TypeError):
        load_custom("custom_sig", "bad", allow_module_prefixes=("custom_",), require_marked_decorator=True)
