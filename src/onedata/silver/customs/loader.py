from __future__ import annotations
import importlib, inspect, sys
from typing import Iterable, Callable, Any, Optional
from pyspark.sql import DataFrame

def _is_marked_custom(fn: Callable[..., Any]) -> bool:
    return bool(getattr(fn, "__onedata_custom__", False))

def load_custom(
    module: str,
    method: str,
    *,
    allow_module_prefixes: Optional[Iterable[str]] = None,
    require_marked_decorator: bool = False,
) -> Callable[..., DataFrame]:
    """
    Carrega `module.method` e valida:
      - módulo permitido por prefixo (Ex: custom_)
      - decorator __onedata_custom__ 
      - assinatura: primeiro parâmetro chama-se 'df'
    """
    if allow_module_prefixes and not any(module.startswith(p) for p in allow_module_prefixes):
        raise PermissionError(f"Módulo '{module}' não permitido: {list(allow_module_prefixes)}")

    try:
        mod = importlib.import_module(module)
    except Exception as e:
        raise ImportError(f"Falha ao importar módulo '{module}': {e}") from e

    try:
        fn = getattr(mod, method)
    except AttributeError as e:
        raise AttributeError(f"Método '{method}' não encontrado em '{module}'.") from e

    if require_marked_decorator and not _is_marked_custom(fn):
        raise PermissionError(f"'{module}.{method}' sem marca '__onedata_custom__'.")

    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params or params[0].name != "df":
        raise TypeError("Assinatura esperada: fn(df: DataFrame, **kwargs) -> DataFrame")

    return fn
