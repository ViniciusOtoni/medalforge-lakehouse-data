import importlib
import inspect
from typing import Dict, Any
from pyspark.sql import DataFrame
from .contract_model import CustomsCfg, CustomDecl, Step

def _load_custom(module: str, method: str):
    mod = importlib.import_module(module)
    fn = getattr(mod, method)
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params or params[0].name != "df":
        raise TypeError("Custom deve ter assinatura: fn(df: DataFrame, **kwargs) -> DataFrame")
    return fn

def _validate_args_schema(args: Dict[str, Any], schema: Dict[str, Any]):
    """
    Validações simples para tipos numéricos e limites.
    Exemplo de schema:
      { "percent": {"type": "number", "min": 0, "max": 50}, "threshold": {"type": "number", "min": 0} }
    """
    for key, rule in schema.items():
        if key not in args:
            raise ValueError(f"Argumento obrigatório ausente: '{key}'")
        val = args[key]
        rtype = rule.get("type")
        if rtype == "number":
            if not isinstance(val, (int, float)):
                raise TypeError(f"'{key}' deve ser número")
            if "min" in rule and val < rule["min"]:
                raise ValueError(f"'{key}' < min permitido")
            if "max" in rule and val > rule["max"]:
                raise ValueError(f"'{key}' > max permitido")
        # (poderíamos expandir para string, regex, enum etc.)

def apply_customs_stage(df: DataFrame, customs_cfg: CustomsCfg, stage_name: str) -> DataFrame:
    if not customs_cfg.allow or not customs_cfg.use_in:
        return df

    registry_index: Dict[str, CustomDecl] = {c.name: c for c in customs_cfg.registry}

    for step in customs_cfg.use_in:
        if step.stage != stage_name:
            continue
        decl = registry_index.get(step.method)
        if not decl:
            raise ValueError(f"Custom '{step.method}' não registrado em customs.registry")

        _validate_args_schema(step.args, decl.args_schema)
        fn = _load_custom(decl.module, decl.method)
        df = fn(df, **step.args)

    return df
