"""
Módulo: test_args
Propósito: validar conversões e validações do arquivo customs/args.py.
Cobertura-alvo: caminhos "felizes" e erros relevantes (tipos, ranges, regex, enum, defaults e extras).
"""

import pytest
from onedata.silver.customs.args import (
    coerce_boolean,
    coerce_number,
    coerce_integer,
    validate_and_normalize_args,
)


def test_coerce_boolean_varios_formatos():
    """Garante que strings e números sejam coeridos corretamente para bool."""
    assert coerce_boolean(True) is True
    assert coerce_boolean(False) is False
    assert coerce_boolean(" yes ") is True
    assert coerce_boolean("NO") is False
    assert coerce_boolean(1) is True
    assert coerce_boolean(0.0) is False
    with pytest.raises(TypeError):
        coerce_boolean("talvez")


def test_coerce_number_int_e_float():
    """Confere coercion numérica de int/float/strings válidas e erro em strings inválidas."""
    assert coerce_number(10) == 10
    assert coerce_number(3.14) == 3.14
    assert coerce_number("  -42 ") == -42
    assert coerce_number("  +2.5 ") == 2.5
    with pytest.raises(TypeError):
        coerce_number("2,5")  # vírgula não suportada


def test_coerce_integer_casos():
    """Assegura que inteiros venham de int, float inteiro e string inteira; demais casos dão erro."""
    assert coerce_integer(5) == 5
    assert coerce_integer(10.0) == 10
    assert coerce_integer(" -7 ") == -7
    with pytest.raises(TypeError):
        coerce_integer(3.14)
    with pytest.raises(TypeError):
        coerce_integer("9.0")


def test_validate_and_normalize_args_basico_sucesso():
    """
    Objetivo: exercício completo do validador com tipos diversos, ranges e defaults.
    Esperado: retorno normalizado sem erros e sem campos extra.
    """
    schema = {
        "n": {"type": "number", "coerce": True, "min": 0, "max": 10},
        "i": {"type": "integer", "coerce": True, "exclusive_min": -1, "exclusive_max": 11},
        "b": {"type": "boolean", "coerce": True},
        "s": {"type": "string", "trim": True, "min_len": 1, "max_len": 5},
        "e": {"type": "enum", "choices": ["a", "b"]},
        "r": {"type": "regex", "pattern": r"[A-Z]{3}\d{2}", "ignore_case": True, "partial": False},
        "opt": {"type": "integer", "required": False, "default": 99},
        "mult": {"type": "number", "coerce": True, "multiple_of": 0.5},
    }
    args = {
        "n": " 10 ",
        "i": "10",
        "b": "TRUE",
        "s": "  ok ",
        "e": "a",
        "r": "abc12",
        "mult": "1.5",
    }
    out = validate_and_normalize_args(args, schema)
    assert out == {
        "n": 10.0,
        "i": 10,
        "b": True,
        "s": "ok",
        "e": "a",
        "r": "abc12",
        "opt": 99,
        "mult": 1.5,
    }


def test_validate_and_normalize_args_erros_frequentes():
    """Verifica mensagens de erro típicas: falta requerido, extra, range, enum, regex."""
    schema = {
        "x": {"type": "number", "coerce": True, "min": 0},
        "mode": {"type": "enum", "choices": ["fast", "safe"]},
        "code": {"type": "regex", "pattern": r"\d{3}", "partial": False},
    }

    # faltando obrigatório
    with pytest.raises(ValueError) as e1:
        validate_and_normalize_args({"mode": "fast", "code": "123"}, schema)
    assert "falta argumento obrigatório: 'x'" in str(e1.value)

    # argumento extra
    with pytest.raises(ValueError) as e2:
        validate_and_normalize_args({"x": 1, "mode": "fast", "code": "123", "zz": 1}, schema)
    assert "args não permitidos: ['zz']" in str(e2.value)

    # enum inválido
    with pytest.raises(ValueError) as e3:
        validate_and_normalize_args({"x": 1, "mode": "turbo", "code": "123"}, schema)
    assert "∉" in str(e3.value)

    # regex inválida
    with pytest.raises(ValueError) as e4:
        validate_and_normalize_args({"x": 1, "mode": "fast", "code": "12a"}, schema)
    assert "não casa" in str(e4.value)

    # range inválido
    with pytest.raises(ValueError) as e5:
        validate_and_normalize_args({"x": "-1", "mode": "fast", "code": "123"}, schema)
    assert "x>= 0" in str(e5.value)


def test_validate_and_normalize_args_multiple_of_zero():
    """Garante que multiple_of=0 seja rejeitado conforme regra."""
    schema = {"m": {"type": "number", "coerce": True, "multiple_of": 0}}
    with pytest.raises(ValueError) as e:
        validate_and_normalize_args({"m": 10}, schema)
    assert "multiple_of não pode ser 0" in str(e.value)
