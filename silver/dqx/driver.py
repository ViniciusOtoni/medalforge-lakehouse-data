"""
Driver DQX: aplica regras de qualidade (Databricks Labs DQX) e separa válidos/quarentena.

Funções
- _collect_checks(dqx_cfg): valida e coleta os checks no formato aceito pelo DQX.
- apply_checks_split(df, dqx_cfg): executa o DQX e retorna (valid_df, quarantine_df).

Observações
- Espera receber o dqx_cfg já normalizado (ex.: via Pydantic: cfg.dqx.model_dump()).
- Usa WorkspaceClient padrão do ambiente Databricks.
"""

from typing import Tuple, Dict, Any, List
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


def _collect_checks(dqx_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Coleta e valida a lista de checks a partir de um dicionário de configuração DQX.

    Parâmetros
    ----------
    dqx_cfg : dict
        Configuração já normalizada do DQX. Formato esperado:
        {
          "checks": [ { name, criticality, check: { function, arguments } }, ... ],
          "custom": [ { name, criticality, check: { function, arguments } }, ... ]
        }

    Retorno
    -------
    list[dict]
        Lista de checks no formato nativo do DQX:
        { "name": str, "criticality": "error"|"warning", "check": { "function": str, "arguments": dict } }

    Exceptions
    ----------
    ValueError
        Se algum item não contiver 'check.function' (mensagem explícita para facilitar o debug).
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
    Aplica as regras do DQX e retorna dois DataFrames: válidos e quarentenados.

    Parâmetros
    ----------
    df : pyspark.sql.DataFrame
        DataFrame de entrada (Bronze/Silver) sobre o qual os checks serão executados.
    dqx_cfg : dict
        Configuração já normalizada do DQX (ex.: cfg.dqx.model_dump()).

    Retorno
    -------
    (valid_df, quarantine_df) : tuple[DataFrame, DataFrame]
        - valid_df: linhas que passaram em todos os checks críticos.
        - quarantine_df: linhas que falharam em pelo menos um check (com colunas técnicas do DQX).

    Exceptions
    ----------
    ValueError
        Propagada se o dqx_cfg estiver malformado (ex.: itens sem 'check.function').
    Outras
        Exceções do DQX/SDK podem ser propagadas (problemas de workspace/perm/execução).
    """
    # Instancia o engine DQX usando o WorkspaceClient padrão do ambiente
    engine = DQEngine(WorkspaceClient())

    # Coleta/valida checks no formato nativo do DQX
    checks = _collect_checks(dqx_cfg=dqx_cfg)

    # Executa checks e separa registros válidos vs. quarentenados
    # (mantém posicionais por compatibilidade com a API do DQX)
    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks, globals())
    return valid_df, quarantine_df
