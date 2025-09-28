"""
Testes unitários da engine de customs.

Intuito
-------
- Garantir validação de assinatura (df como 1º parâmetro).
- Validar normalização/validação de argumentos via schema.
- Exercitar apply_customs_stage aplicando um custom simples no stage 'standard'.
"""

import types
import sys
import pytest
from pyspark.sql import functions as F
from silver.framework.contract_model import CustomsCfg, CustomDecl, Step
from silver.framework import customs_engine as CE


def _install_fake_module(mod_name="my_customs"):
    """
    Registra dinamicamente um módulo fake em sys.modules com funções alvo dos testes.

    - good(df, *, k=1): retorna df com coluna 'k' = k
    - bad_signature(x): assinatura inválida (1º param não é 'df')
    """
    m = types.ModuleType(mod_name)

    def good(df, k=1):
        return df.withColumn("k", F.lit(k))

    def bad_signature(x):
        return x

    m.good = good
    m.bad_signature = bad_signature
    sys.modules[mod_name] = m
    return mod_name


def test_load_custom_signature_ok_and_bad(monkeypatch):
    """
    _load_custom: aceita função com 1º arg 'df' e rejeita assinatura inválida.
    """
    mod = _install_fake_module()
    fn = CE._load_custom(mod, "good")
    assert callable(fn)

    with pytest.raises(TypeError):
        CE._load_custom(mod, "bad_signature")


def test_apply_customs_stage_happy_path(sample_df):
    """
    apply_customs_stage: aplica custom registrado apenas no stage 'standard'.
    """
    mod = _install_fake_module()
    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="c1", module=mod, method="good", args_schema={"k": {"type": "integer", "required": False, "default": 7}})],
        use_in=[Step(method="c1", args={}, stage="standard")],
    )

    out = CE.apply_customs_stage(sample_df, cfg, stage_name="standard")
    assert "k" in out.columns
    assert out.select("k").distinct().count() == 1
    assert out.select("k").first()[0] == 7


def test_apply_customs_stage_args_validation_error(sample_df):
    """
    apply_customs_stage: falha com mensagem clara quando argumento obrigatório falta.
    """
    mod = _install_fake_module()
    cfg = CustomsCfg(
        allow=True,
        registry=[CustomDecl(name="need_arg", module=mod, method="good", args_schema={"k": {"type": "integer", "required": True}})],
        use_in=[Step(method="need_arg", args={}, stage="standard")],
    )

    with pytest.raises(Exception) as exc:
        CE.apply_customs_stage(sample_df, cfg, stage_name="standard")
    assert "Argumento obrigatório ausente: 'k'" in str(exc.value)
