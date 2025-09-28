"""
Transformações "core" reutilizáveis para a camada Silver.

Escopo
------
- Limpeza e normalização (trim, cast, datas, deduplicação).
- Funções auxiliares de remediação para uso em quarentena
  (coerce_date, clamp_range, drop_if_null).

Contrato
--------
- Todas as funções recebem e retornam `pyspark.sql.DataFrame` (estilo funcional/imutável).
- Por padrão, **colunas ausentes** geram erro; opcionalmente é possível ignorá-las com `missing="skip"`.
"""

from __future__ import annotations
from typing import List, Dict, Optional, Literal, Union

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


# ----------------------------- Helpers internos -----------------------------

def _ensure_columns(df: DataFrame, cols: List[str], *, where: str) -> None:
    """
    Garante que todas as colunas em `cols` existam no DataFrame.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de referência.
    cols : list[str]
        Colunas que devem existir.
    where : str
        Contexto da verificação (usado na mensagem de erro).

    Exceptions
    ----------
    ValueError: quando alguma coluna não existe.
    """
    df_cols = set(df.columns)
    missing = [column for column in cols if column not in df_cols]
    if missing:
        raise ValueError(f"Colunas ausentes em {where}: {missing} (presentes={sorted(df_cols)})")


# --------- Métodos "core" padrão (aplicados em standard e/ou quarantine) ---------

def trim_columns(
    df: DataFrame,
    columns: List[str],
    *,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Remove espaços em branco (trim) nas colunas informadas.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    columns : list[str]
        Lista de nomes de colunas a aplicar `trim`.
    missing : {"error","skip"}
        O que fazer se alguma coluna não existir:
        - "error" (padrão): lança erro com lista das ausentes.
        - "skip": ignora silenciosamente as colunas ausentes.

    Retorno
    -------
    DataFrame
        DataFrame com as colunas normalizadas.

    Observações
    -----------
    - Apenas faz sentido em colunas de tipo string; em outros tipos o Spark tentará cast interno.
    """
    cols = columns or []
    if missing == "error":
        _ensure_columns(df, cols, where="trim_columns")
    for column in cols:
        if column in df.columns:
            df = df.withColumn(column, F.trim(F.col(column)))
    return df


def cast_columns(
    df: DataFrame,
    mapping: Dict[str, str],
    *,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Faz cast de colunas conforme o mapeamento {coluna: tipo_spark}.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    mapping : dict[str,str]
        Dicionário do tipo { "amount": "double", "id": "string", ... }.
    missing : {"error","skip"}
        Política para colunas não encontradas (ver `trim_columns`).

    Retorno
    -------
    DataFrame
        DataFrame com colunas convertidas.

    Exceptions
    ----------
    - Pode propagar erros do Spark se o tipo alvo for inválido.
    """
    cols = list(mapping.keys())
    if missing == "error":
        _ensure_columns(df, cols, where="cast_columns")
    for col, dtype in (mapping or {}).items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def normalize_dates(
    df: DataFrame,
    columns: List[str] | Dict[str, str],
    format: Optional[str] = None,
    *,
    project_ano_mes: bool = True,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Converte colunas de data a partir de um ou mais formatos e, opcionalmente,
    projeta colunas derivadas 'ano' e 'mes' a partir da primeira coluna.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    columns : list[str] | dict[str,str]
        - list[str]: todas as colunas serão convertidas usando `format` (obrigatório).
        - dict[str,str]: mapa coluna->formato específico por coluna.
    format : str | None
        Máscara de parsing (ex.: "yyyy-MM-dd") quando `columns` é lista.
    project_ano_mes : bool
        Se True (padrão), cria 'ano' e 'mes' se ainda não existirem.
    missing : {"error","skip"}
        Política para colunas não encontradas.

    Retorno
    -------
    DataFrame
        DataFrame com datas normalizadas e, se habilitado, colunas derivadas.

    Exceptions
    ----------
    - ValueError: quando `columns` é lista e `format` está ausente.
    """
    if isinstance(columns, dict):
        col_map = columns
        cols_to_check = list(col_map.keys())
    else:
        if format is None:
            raise ValueError("normalize_dates: quando 'columns' é lista, 'format' é obrigatório.")
        col_map = {column: format for column in (columns or [])}
        cols_to_check = list(col_map.keys())

    if missing == "error":
        _ensure_columns(df, cols_to_check, where="normalize_dates")

    for column, fmt in col_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.to_date(F.col(column), fmt))

    # Projeta ano/mês apenas uma vez, a partir da primeira coluna válida
    if project_ano_mes and cols_to_check:
        main = next((column for column in cols_to_check if column in df.columns), None)
        if main:
            if "ano" not in df.columns:
                df = df.withColumn("ano", F.year(F.col(main)))
            if "mes" not in df.columns:
                df = df.withColumn("mes", F.month(F.col(main)))
    return df


def deduplicate(
    df: DataFrame,
    keys: List[str],
    order_by: List[str],
    *,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Remove duplicidades por chave, mantendo a 1ª linha conforme a ordenação desejada.

    Estratégia
    ---------
    - Janela por `keys` + `row_number()` ordenado por expressões em `order_by`
      (strings compatíveis com `F.expr`, ex.: "created_at desc", "priority asc").
    - Mantém apenas `_rn == 1`.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    keys : list[str]
        Chaves de particionamento da janela (colunas de unicidade).
    order_by : list[str]
        Expressões de ordenação (maior prioridade primeiro).
        Exemplos: ["ingestion_ts desc", "updated_at desc"].
    missing : {"error","skip"}
        Política para colunas de `keys` inexistentes.

    Retorno
    -------
    DataFrame
        DataFrame sem duplicidades por `keys`.

    Exceptions
    ----------
    - ValueError: quando `order_by` está vazio.
    """
    if not order_by:
        raise ValueError("deduplicate: 'order_by' não pode ser vazio.")
    if missing == "error":
        _ensure_columns(df, keys, where="deduplicate")

    exprs = [F.expr(e) for e in order_by]
    w = Window.partitionBy(*[k for k in keys if (missing == "skip" and k not in df.columns) is False]) \
             .orderBy(*exprs)
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .where(F.col("_rn") == 1)
          .drop("_rn")
    )


# -------------------- Métodos auxiliares de remediação (quarantine) --------------------

def coerce_date(
    df: DataFrame,
    column: str,
    from_patterns: List[str],
    to_format: str,
    *,
    as_date: bool = False,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Tenta converter strings de data usando múltiplos formatos, na ordem fornecida.

    Comportamento
    -------------
    - Para cada padrão em `from_patterns`, tenta `to_date(col, pattern)`.
    - Usa o primeiro parsing bem-sucedido (via `coalesce`).

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    column : str
        Nome da coluna de data.
    from_patterns : list[str]
        Padrões de entrada (ex.: ["dd/MM/yyyy", "yyyy-MM-dd"]).
    to_format : str
        Formato de saída quando `as_date=False` (ex.: "yyyy-MM-dd").
    as_date : bool
        Se True, mantém a coluna como `date` (não formata string).
        Se False (padrão), aplica `date_format(..., to_format)` e retorna string.
    missing : {"error","skip"}
        Política para coluna inexistente.

    Retorno
    -------
    DataFrame
        DataFrame com a coluna substituída pelo resultado parseado (ou nulo quando não parseável).
    """
    if missing == "error":
        _ensure_columns(df, [column], where="coerce_date")
    if column not in df.columns:
        return df  

    src = F.col(column)
    parsed = F.lit(None).cast("date")  
    for fmt in from_patterns or []:
        parsed = F.coalesce(parsed, F.to_date(src, fmt))

    return df.withColumn(
        column,
        parsed if as_date else F.date_format(parsed, to_format)
    )


def clamp_range(
    df: DataFrame,
    column: str,
    min: Optional[float] = None,
    max: Optional[float] = None,
    *,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    "Clipa" os valores de uma coluna numérica a um intervalo [min, max].

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    column : str
        Nome da coluna numérica.
    min : float | None
        Limite inferior opcional.
    max : float | None
        Limite superior opcional.
    missing : {"error","skip"}
        Política para coluna inexistente.

    Retorno
    -------
    DataFrame
        DataFrame com valores fora do intervalo substituídos pelo limite mais próximo.

    Observações
    -----------
    - Se apenas `min` for fornecido, aplica max(col, min).
    - Se apenas `max` for fornecido, aplica min(col, max).
    - Esta função **não faz cast** automático para numérico.
    """
    if missing == "error":
        _ensure_columns(df, [column], where="clamp_range")
    if column not in df.columns:
        return df  

    col = F.col(column)
    if min is not None and max is not None:
        return df.withColumn(
            column,
            F.when(col < F.lit(min), F.lit(min)).when(col > F.lit(max), F.lit(max)).otherwise(col)
        )
    if min is not None:
        return df.withColumn(column, F.when(col < F.lit(min), F.lit(min)).otherwise(col))
    if max is not None:
        return df.withColumn(column, F.when(col > F.lit(max), F.lit(max)).otherwise(col))
    return df


def drop_if_null(
    df: DataFrame,
    columns: List[str],
    *,
    missing: Literal["error", "skip"] = "error",
) -> DataFrame:
    """
    Remove linhas onde **QUALQUER** uma das colunas informadas é nula (OR).

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada.
    columns : list[str]
        Lista de colunas cujo NULL elimina a linha.
    missing : {"error","skip"}
        Política para colunas inexistentes.

    Retorno
    -------
    DataFrame
        DataFrame filtrado (mantém apenas linhas onde todas as colunas estão não nulas).

    Detalhes
    --------
    - Constrói uma condição OR acumulada: (col1 IS NULL) OR (col2 IS NULL) OR ...
    - Filtra com `where(~cond)` (negação: nenhuma delas é nula).
    """
    cols = columns or []
    if missing == "error":
        _ensure_columns(df, cols, where="drop_if_null")

    cond = None
    for column in cols:
        if column not in df.columns:
            continue
        cnd = F.col(column).isNull()
        cond = cnd if cond is None else (cond | cnd)
    return df if cond is None else df.where(~cond)
