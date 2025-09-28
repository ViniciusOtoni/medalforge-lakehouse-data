"""
Estágio ETL: strip colunas técnicas DQX, aplica steps core e customs por estágio.
"""

from __future__ import annotations
from typing import List, Optional, Iterable, Callable, Dict
from pyspark.sql import DataFrame
from onedata.silver.domain.etl import Step, CustomsCfg
from onedata.silver.etl import core as etl_core
from onedata.silver.customs.runner import apply_customs_stage

def strip_dqx_cols(df: DataFrame) -> DataFrame:
    """
    Remove colunas técnicas do DQX: _errors, _warnings e prefixo _dqx_.
    """
    drop_names = {"_errors", "_warnings"}
    keep = [c for c in df.columns if c not in drop_names and not c.startswith("_dqx_")]
    return df.select(*keep) if len(keep) != len(df.columns) else df

def run_core_steps(df: DataFrame, steps: List[Step], allow_missing: bool = False) -> DataFrame:
    """
    Aplica sequência de steps core (etl_core.*) conforme contrato.
    """
    for s in steps or []:
        method = s.method
        args: Dict = dict(s.args or {})
        if not hasattr(etl_core, method):
            if allow_missing:
                continue
            raise ValueError(f"Método core desconhecido: {method}")
        df = getattr(etl_core, method)(df, **args)
    return df

def run_customs_standard(
    df: DataFrame,
    customs: CustomsCfg,
    *,
    env_is_dev: bool,
    allow_extra_args: bool,
    allow_module_prefixes: Optional[Iterable[str]],
    require_marked_decorator: bool,
    logger: Optional[Callable[[str], None]] = None,
) -> DataFrame:
    """
    Executa customs no stage 'standard' se habilitado.
    """
    if not customs or not customs.allow:
        return df
    return apply_customs_stage(
        df,
        customs_cfg=customs,
        stage_name="standard",
        dev_reload=env_is_dev,
        allow_extra_args=allow_extra_args,
        allow_module_prefixes=allow_module_prefixes,
        require_marked_decorator=require_marked_decorator,
        logger=logger,
    )
