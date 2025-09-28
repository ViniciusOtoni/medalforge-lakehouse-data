from pyspark.sql import DataFrame, functions as F

def apply_discount_if_high_value(df: DataFrame, percent: float, threshold: float) -> DataFrame:
    """
    Aplica desconto percentual na coluna 'amount' quando amount >= threshold.
    Adiciona 'discount_applied' (boolean) indicando elegibilidade.
    """
    colname = "amount"
    if colname not in df.columns:
        raise ValueError(f"Coluna obrigatória '{colname}' não encontrada no DataFrame.")

    if not (0 <= percent <= 50):
        raise ValueError("O parâmetro 'percent' deve estar entre 0 e 50.")

    amount = F.col(colname)
    # Condição de elegibilidade: não nulo, não NaN e >= threshold
    cond_base = amount.isNotNull() & (~F.isnan(amount)) & (amount >= F.lit(threshold))

    # Se já existir 'discount_applied', não reaplique (idempotência simples)
    already = F.col("discount_applied") if "discount_applied" in df.columns else F.lit(False)
    apply_now = (~already) & cond_base

    multiplier = F.lit(1.0) - (F.lit(percent) / F.lit(100.0))

    out = (
        df
        .withColumn("discount_applied", F.coalesce(already | cond_base, F.lit(False)).cast("boolean"))
        .withColumn(colname, F.when(apply_now, amount * multiplier).otherwise(amount).cast("double"))
    )
    return out
