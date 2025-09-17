from pyspark.sql import DataFrame, functions as F

def apply_discount_if_high_value(df: DataFrame, percent: float, threshold: float) -> DataFrame:
    """
    Aplica desconto percentual na coluna 'amount' quando o valor for >= threshold.
    Assinatura compatível com a engine de customs: primeiro arg é o DF, seguido de kwargs validados.

    Parâmetros
    ----------
    df : DataFrame
        DataFrame de entrada (espera coluna 'amount').
    percent : float
        Percentual de desconto (0 a 50, por exemplo).
    threshold : float
        Valor a partir do qual o desconto é aplicado.

    Retorna
    -------
    DataFrame
        Mesmo schema de entrada, com:
          - 'amount' atualizado conforme a regra
          - coluna auxiliar 'discount_applied' (boolean) indicando se houve desconto
    """
    colname = "amount"

    if colname not in df.columns:
        raise ValueError(f"Coluna obrigatória '{colname}' não encontrada no DataFrame.")

    if percent < 0 or percent > 50:
        # Limites de segurança; o args_schema no YAML já valida, mas deixamos o guard-rail aqui também.
        raise ValueError("O parâmetro 'percent' deve estar entre 0 e 50.")

    # Condição marcada ANTES da atualização para não depender do valor já descontado
    cond = F.col(colname) >= F.lit(threshold)
    multiplier = 1.0 - (percent / 100.0)

    df = (
        df
        .withColumn("discount_applied", cond)  # marca linhas elegíveis
        .withColumn(
            colname,
            F.when(cond, F.col(colname) * F.lit(multiplier)).otherwise(F.col(colname))
        )
        .withColumn(colname, F.col(colname).cast("double"))  # garante tipo final
    )

    return df
