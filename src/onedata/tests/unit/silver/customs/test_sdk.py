"""
Módulo: test_sdk
Propósito: validar o decorator minimalista de customs/sdk.py, garantindo
que ele marque a função e não altere seu comportamento.
"""

from onedata.silver.customs.sdk import custom


def test_custom_decorator_marca_sem_alterar_chamada():
    """Decorator deve apenas setar __onedata_custom__ e retornar a mesma função (mesma identidade)."""
    def f(df, k=1):
        return (df, k)

    g = custom(f)
    assert g is f
    assert getattr(g, "__onedata_custom__", False) is True
