"""
Módulo: test_settings
Finalidade: verificar parsing de variáveis de ambiente e defaults em application/settings.py:
 - ENV defaulta para "dev"
 - CUSTOMS_STRICT controla booleans e parsing de CUSTOMS_PREFIXES (apenas quando estrito)
 - SILVER_EXTERNAL_BASE respeita default e override por env
"""

import importlib
import os


def test_settings_defaults(monkeypatch):
    """Propósito: sem envs setadas, Settings deve assumir os defaults documentados."""
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("CUSTOMS_STRICT", raising=False)
    monkeypatch.delenv("CUSTOMS_PREFIXES", raising=False)
    monkeypatch.delenv("SILVER_EXTERNAL_BASE", raising=False)

    # Reimporta o módulo para recalcular SETTINGS com env limpa
    mod = importlib.import_module("onedata.silver.application.settings")
    importlib.reload(mod)

    assert mod.SETTINGS.env == "dev"
    assert mod.SETTINGS.customs_strict is False
    # quando não estrito, prefixes deve ser None
    assert mod.SETTINGS.customs_prefixes is None
    # base default deve ser a configurada no módulo
    assert mod.SETTINGS.silver_external_base.startswith("abfss://silver@")


def test_settings_customs_strict_e_prefixes(monkeypatch):
    """
    Propósito: quando CUSTOMS_STRICT=1, 'customs_strict' vira True e 'customs_prefixes'
    é derivado (split por vírgula) da variável CUSTOMS_PREFIXES.
    """
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("CUSTOMS_STRICT", "1")
    monkeypatch.setenv("CUSTOMS_PREFIXES", "pkg.customs,custom_")

    mod = importlib.import_module("onedata.silver.application.settings")
    importlib.reload(mod)

    assert mod.SETTINGS.env == "prod"
    assert mod.SETTINGS.customs_strict is True
    assert tuple(mod.SETTINGS.customs_prefixes) == ("pkg.customs", "custom_")


def test_settings_external_base_override(monkeypatch):
    """Propósito: SILVER_EXTERNAL_BASE deve aceitar override por variável de ambiente."""
    monkeypatch.setenv("SILVER_EXTERNAL_BASE", "abfss://my-silver@storage/tables")
    mod = importlib.import_module("onedata.silver.application.settings")
    importlib.reload(mod)

    assert mod.SETTINGS.silver_external_base == "abfss://my-silver@storage/tables"
