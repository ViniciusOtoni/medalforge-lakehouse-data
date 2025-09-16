from typing import List, Dict, Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

# --------- Métodos "core" padrão (aplicados em standard e/ou quarantine) ---------

def trim_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df

def cast_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    for col, dtype in mapping.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df

def normalize_dates(df: DataFrame, columns: List[str], format: str, project_ano_mes: bool = True) -> DataFrame:
    for c in columns:
        df = df.withColumn(c, F.to_date(F.col(c), format))
    # Projeta ano/mês apenas uma vez
    if project_ano_mes:
        main = columns[0]
        if "ano" not in df.columns:
            df = df.withColumn("ano", F.year(F.col(main)))
        if "mes" not in df.columns:
            df = df.withColumn("mes", F.month(F.col(main)))
    return df

def deduplicate(df: DataFrame, keys: List[str], order_by: List[str]) -> DataFrame:
    exprs = [F.expr(e) for e in order_by]
    w = Window.partitionBy(*keys).orderBy(*exprs)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")

# -------------------- Métodos auxiliares de remediação (quarantine) --------------------

def coerce_date(df: DataFrame, column: str, from_patterns: List[str], to_format: str) -> DataFrame:
    """
    Tenta converter strings de datas usando múltiplos formatos. Mantém valor original caso falhe.
    """
    col = F.col(column)
    # começa assumindo nulo
    parsed = F.lit(None).cast("date")
    for fmt in from_patterns:
        parsed = F.coalesce(parsed, F.to_date(col, fmt))
    # projeta coluna final no formato desejado (como string yyyy-MM-dd) OU mantém date
    # aqui, escolhemos manter como string formatada (para harmonizar com contratos que esperam string)
    df = df.withColumn(column, F.date_format(parsed, to_format))
    return df

def clamp_range(df: DataFrame, column: str, min: Optional[float] = None, max: Optional[float] = None) -> DataFrame:
    col = F.col(column)
    if min is not None and max is not None:
        return df.withColumn(column, F.when(col < F.lit(min), F.lit(min)).when(col > F.lit(max), F.lit(max)).otherwise(col))
    if min is not None:
        return df.withColumn(column, F.when(col < F.lit(min), F.lit(min)).otherwise(col))
    if max is not None:
        return df.withColumn(column, F.when(col > F.lit(max), F.lit(max)).otherwise(col))
    return df

def drop_if_null(df: DataFrame, columns: List[str]) -> DataFrame:
    cond = None
    for c in columns:
        cnd = F.col(c).isNull()
        cond = cnd if cond is None else (cond | cnd)
    return df.where(~cond)
