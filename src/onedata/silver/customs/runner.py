from __future__ import annotations
from typing import Dict, Optional, Iterable, Callable
from pyspark.sql import DataFrame
from onedata.silver.domain.etl import CustomsCfg, CustomDecl, Step
from onedata.silver.customs.loader import load_custom
from onedata.silver.customs.args import validate_and_normalize_args

def apply_customs_stage(
    df: DataFrame,
    customs_cfg: CustomsCfg,
    stage_name: str,
    *,
    dev_reload: bool = False,
    allow_extra_args: bool = True,
    logger: Optional[Callable[[str], None]] = None,
    allow_module_prefixes: Optional[Iterable[str]] = "custom_",
    require_marked_decorator: bool = True,
) -> DataFrame:
    """
    Aplica customs de `customs_cfg.use_in` filtrando por `stage_name`.
    """
    if not customs_cfg or not customs_cfg.allow or not customs_cfg.use_in:
        return df

    names = [c.name for c in (customs_cfg.registry or [])]
    if len(names) != len(set(names)):
        dup = sorted({n for n in names if names.count(n) > 1})
        raise ValueError(f"customs.registry com duplicatas: {dup}")

    registry: Dict[str, CustomDecl] = {c.name: c for c in customs_cfg.registry}
    if logger: logger(f"[customs] stage={stage_name} steps={len(customs_cfg.use_in)} regs={len(registry)}")

    cur: DataFrame = df
    for idx, step in enumerate(customs_cfg.use_in, 1):
        if step.stage != stage_name:
            continue

        decl = registry.get(step.method)
        if not decl:
            known = ", ".join(sorted(registry.keys()))
            raise ValueError(f"Custom '{step.method}' não registrado (conhecidos=[{known}])")

        try:
            norm = validate_and_normalize_args(step.args or {}, decl.args_schema or {}, allow_extra_args=allow_extra_args)
        except Exception as e:
            raise type(e)(f"Args inválidos em '{decl.name}' ({decl.module}.{decl.method}): {e}")

        fn = load_custom(
            decl.module, decl.method,
            dev_reload=dev_reload,
            allow_module_prefixes=allow_module_prefixes,
            require_marked_decorator=require_marked_decorator,
        )

        if logger: logger(f"[customs] ({idx}) {decl.name} -> {decl.module}.{decl.method} args={norm}")
        try:
            cur = fn(cur, **norm)
            if not isinstance(cur, DataFrame):
                raise TypeError(f"'{decl.name}' retornou {type(cur)} (esperado DataFrame)")
        except Exception as e:
            raise RuntimeError(f"Falha no custom '{decl.name}' ({decl.module}.{decl.method}): {e}") from e

    return cur
