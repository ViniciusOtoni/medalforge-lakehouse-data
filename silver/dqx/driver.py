from typing import Tuple, Dict, Any, List
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

def _collect_checks(dqx_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Recebe o dqx_cfg já normalizado (ex.: cfg.dqx.model_dump()) e
    retorna a lista de checks no formato nativo do DQX:
      { name, criticality, check: { function, arguments } }
    """
    checks: List[Dict[str, Any]] = []
    for section in ("checks", "custom"):
        items = dqx_cfg.get(section, []) or []
        for item in items:
            # sanity-check para mensagens de erro mais claras
            if "check" not in item or "function" not in item["check"]:
                raise ValueError(f"Item DQX inválido (esperado 'check.function'): {item}")
            checks.append(item)
    return checks

def apply_checks_split(df: DataFrame, dqx_cfg: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica as regras via DQEngine e retorna (valid_df, quarantine_df).
    dqx_cfg deve vir de cfg.dqx.model_dump() (Pydantic já padroniza o formato).
    """
    engine = DQEngine(WorkspaceClient())
    checks = _collect_checks(dqx_cfg)
    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks, globals())
    return valid_df, quarantine_df
