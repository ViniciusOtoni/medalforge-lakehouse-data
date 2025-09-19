"""
Transformações "core" reutilizáveis para a camada Silver:
- Limpeza e normalização (trim, cast, datas, deduplicação).
- Funções auxiliares de remediação para uso em quarentena (coerce_date, clamp_range, drop_if_null).

Todas as funções recebem e retornam `pyspark.sql.DataFrame` (imutabilidade funcional).
"""

from typing import List, Dict, Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


# --------- Métodos "core" padrão (aplicados em standard e/ou quarantine) ---------

def trim_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Remove espaços em branco (trim) nas colunas informadas.

    Parâmetros
    - df: DataFrame de entrada.
    - columns: lista de nomes de colunas a aplicar `trim`.

    Retorno
    - DataFrame com as colunas normalizadas.

    Exceptions
    - Pode propagar erros do Spark em tempo de execução se alguma coluna não existir.
    """
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def cast_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """
    Faz cast de colunas conforme o mapeamento {coluna: tipo_spark}.

    Parâmetros
    - df: DataFrame de entrada.
    - mapping: dicionário { "amount": "double", "id": "string", ... }.

    Retorno
    - DataFrame com colunas convertidas.

    Exceptions
    - Pode propagar erros do Spark se o tipo for inválido ou coluna inexistente.
    """
    for col, dtype in mapping.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def normalize_dates(
    df: DataFrame,
    columns: List[str],
    format: str,
    project_ano_mes: bool = True
) -> DataFrame:
    """
    Converte colunas de data a partir de um formato e, opcionalmente,
    projeta colunas derivadas 'ano' e 'mes' (a partir da primeira coluna da lista).

    Parâmetros
    - df: DataFrame de entrada.
    - columns: colunas que serão convertidas com `to_date`.
    - format: máscara de parsing (ex.: "yyyy-MM-dd").
    - project_ano_mes: se True, cria 'ano' e 'mes' se ainda não existirem.

    Retorno
    - DataFrame com datas normalizadas e eventuais colunas derivadas.

    Exceptions
    - Pode propagar erros do Spark se alguma coluna não existir ou o formato for inválido.
    """
    for c in columns:
        df = df.withColumn(c, F.to_date(F.col(c), format))
    # Projeta ano/mês apenas uma vez
    if project_ano_mes and columns:
        main = columns[0]
        if "ano" not in df.columns:
            df = df.withColumn("ano", F.year(F.col(main)))
        if "mes" not in df.columns:
            df = df.withColumn("mes", F.month(F.col(main)))
    return df


def deduplicate(df: DataFrame, keys: List[str], order_by: List[str]) -> DataFrame:
    """
    Remove duplicidades por chave, mantendo a 1ª linha conforme a ordenação desejada.

    Estratégia
    - Janela por `keys` + `row_number()` ordenado por expressões em `order_by`
      (strings compatíveis com `F.expr`, ex.: "created_at desc", "priority asc").
    - Mantém apenas `_rn == 1`.

    Parâmetros
    - df: DataFrame de entrada.
    - keys: chaves de particionamento da janela (colunas de unicidade).
    - order_by: expressões de ordenação (maior prioridade primeiro).

    Retorno
    - DataFrame sem duplicidades por `keys`.

    Exceptions
    - Pode propagar erros do Spark se colunas/expressões forem inválidas.
    """
    exprs = [F.expr(e) for e in order_by]
    w = Window.partitionBy(*keys).orderBy(*exprs)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


# -------------------- Métodos auxiliares de remediação (quarantine) --------------------

def coerce_date(
    df: DataFrame,
    column: str,
    from_patterns: List[str],
    to_format: str
) -> DataFrame:
    """
    Tenta converter strings de data usando múltiplos formatos, na ordem fornecida.

    Comportamento
    - Para cada padrão em `from_patterns`, tenta `to_date(col, pattern)`.
    - Usa o primeiro parsing bem-sucedido (via `coalesce`).
    - Emite a coluna final como string formatada (`date_format(..., to_format)`).

    Parâmetros
    - df: DataFrame de entrada.
    - column: nome da coluna de data (string).
    - from_patterns: lista de padrões de entrada (ex.: ["dd/MM/yyyy", "yyyy-MM-dd"]).
    - to_format: formato de saída (ex.: "yyyy-MM-dd").

    Retorno
    - DataFrame com a coluna substituída pelo resultado formatado (ou nulo quando não parseável).
    """
    col = F.col(column)
    parsed = F.lit(None).cast("date")  # começa assumindo nulo
    for fmt in from_patterns:
        parsed = F.coalesce(parsed, F.to_date(col, fmt))
    # formata como string alvo; útil quando contratos esperam string e não date
    df = df.withColumn(column, F.date_format(parsed, to_format))
    return df


def clamp_range(
    df: DataFrame,
    column: str,
    min: Optional[float] = None,
    max: Optional[float] = None
) -> DataFrame:
    """
    "Clipa" os valores de uma coluna numérica a um intervalo [min, max].

    Parâmetros
    - df: DataFrame de entrada.
    - column: nome da coluna numérica.
    - min: limite inferior opcional.
    - max: limite superior opcional.

    Retorno
    - DataFrame com valores fora do intervalo substituídos pelo limite mais próximo.

    Observações
    - Se apenas `min` for fornecido, aplica max(df, min).
    - Se apenas `max` for fornecido, aplica min(df, max).
    """
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


def drop_if_null(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Remove linhas onde QUALQUER uma das colunas informadas é nula (OR).

    Parâmetros
    - df: DataFrame de entrada.
    - columns: lista de colunas cujo NULL elimina a linha.

    Retorno
    - DataFrame filtrado (mantém apenas linhas onde todas as colunas estão não nulas).

    Detalhes
    - Constrói uma condição OR acumulada: (col1 IS NULL) OR (col2 IS NULL) OR ...
    - Filtra com `where(~cond)` (negação: nenhuma delas é nula).
    """
    cond = None
    for c in columns:
        cnd = F.col(c).isNull()
        cond = cnd if cond is None else (cond | cnd)
    return df.where(~cond)
