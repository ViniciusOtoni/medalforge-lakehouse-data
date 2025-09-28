from pyspark.sql import DataFrame, functions as F
from onedata.silver.customs.sdk import custom

@custom
def apply_discount_if_high_value(df: DataFrame, percent: float, threshold: float) -> DataFrame:
    """
    Aplica desconto percentual na coluna 'amount' quando amount >= threshold.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada, deve conter a coluna 'amount'.
    percent : float
        Percentual de desconto (0 a 50).
    threshold : float
        Valor mínimo de 'amount' para aplicar desconto.

    Retorno
    -------
    DataFrame
        DataFrame resultante, com:
        - Coluna 'amount' possivelmente reduzida.
        - Coluna booleana 'discount_applied' indicando se o desconto foi aplicado.

    Observações
    -----------
    - Garante idempotência simples: se já existir 'discount_applied', não reaplica.
    - Lança ValueError se a coluna 'amount' não existir ou se 'percent' estiver fora de [0,50].
    """
    colname = "amount"
    if colname not in df.columns:
        raise ValueError(f"Coluna obrigatória '{colname}' não encontrada no DataFrame.")

    if not (0 <= percent <= 50):
        raise ValueError("O parâmetro 'percent' deve estar entre 0 e 50.")

    amount = F.col(colname)
    cond_base = amount.isNotNull() & (~F.isnan(amount)) & (amount >= F.lit(threshold))

    already = F.col("discount_applied") if "discount_applied" in df.columns else F.lit(False)
    apply_now = (~already) & cond_base

    multiplier = F.lit(1.0) - (F.lit(percent) / F.lit(100.0))

    return (
        df
        .withColumn("discount_applied", F.coalesce(already | cond_base, F.lit(False)).cast("boolean"))
        .withColumn(colname, F.when(apply_now, amount * multiplier).otherwise(amount).cast("double"))
    )
