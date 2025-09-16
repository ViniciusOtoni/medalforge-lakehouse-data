from typing import Tuple, Dict, Any, List
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

def apply_checks_split(df: DataFrame, dqx_config: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    """
    Usa o DQEngine para aplicar checks definidos por metadata (inclui 'custom' com sql_expression)
    e retorna (valid_df, quarantine_df).
    """
    ws = WorkspaceClient()
    engine = DQEngine(ws)
    checks: List[Dict[str, Any]] = []

    # checks "prontos"
    checks.extend([c for c in dqx_config.get("checks", [])])

    # checks custom (ex.: sql_expression)
    checks.extend([c for c in dqx_config.get("custom", [])])

    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks, globals())
    return valid_df, quarantine_df
