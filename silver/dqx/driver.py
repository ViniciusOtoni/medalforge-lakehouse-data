from typing import Tuple, Dict, Any, List
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

def _normalize_checks(dqx_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Aceita dois formatos:
      A) Achato:
         - name, function, arguments, criticality
      B) Nativo do DQX:
         - name, criticality, check: { function, arguments }

    Converte tudo para (B).
    """
    checks: List[Dict[str, Any]] = []
    default_crit = dqx_cfg.get("criticality_default", "error")

    raw_items = []
    raw_items.extend(dqx_cfg.get("checks", []) or [])
    raw_items.extend(dqx_cfg.get("custom", []) or [])

    for item in raw_items:
        # já está no formato DQX?
        if "check" in item and isinstance(item["check"], dict):
            checks.append(item)
            continue

        # formato achato -> embrulhar em "check"
        name = item.get("name")
        func = item.get("function")
        args = item.get("arguments", {}) or {}
        crit = item.get("criticality", default_crit)

        if not func:
            raise ValueError(f"Cada regra precisa de 'function'. Faltou em: {item}")

        normalized = {
            "name": name or func,
            "criticality": crit,
            "check": {
                "function": func,
                "arguments": args
            }
        }
        checks.append(normalized)

    return checks

def apply_checks_split(df: DataFrame, dqx_config: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    """
    Usa o DQEngine para aplicar checks definidos por metadata (inclui 'custom' com sql_expression)
    e retorna (valid_df, quarantine_df).
    """
    ws = WorkspaceClient()
    engine = DQEngine(ws)
    checks = _normalize_checks(dqx_config)
    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks, globals())
    return valid_df, quarantine_df
