"""
Estágio DQX: aplica checks e separa válidos/quarentena; re-check após remediação.
"""

from __future__ import annotations
from pyspark.sql import DataFrame
from onedata.silver.domain.dqx import DQXCfg
from onedata.silver.dqx.driver import apply_checks_split

def initial_split(df_bronze: DataFrame, dqx: DQXCfg) -> tuple[DataFrame, DataFrame]:
    """
    DQX inicial. Retorna (valid_df, quarantine_df).
    """
    return apply_checks_split(df_bronze, dqx.model_dump())

def recheck_after_remediation(df_remediated: DataFrame, dqx: DQXCfg) -> tuple[DataFrame, DataFrame]:
    """
    DQX após remediação. Retorna (fixed_valid_df, still_bad_df).
    """
    return apply_checks_split(df_remediated, dqx.model_dump())
