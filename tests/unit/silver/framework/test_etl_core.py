"""
Testes unitários das funções core de ETL (trim, cast, datas, dedup, remediações).
"""

from pyspark.sql import functions as F
from silver.framework import etl_core


def test_trim_and_cast(sample_df):
    """
    trim_columns + cast_columns: aplica trim e converte tipos.
    """
    df = sample_df.withColumn("id", F.concat(F.lit(" "), F.col("id"), F.lit(" ")))
    out = etl_core.trim_columns(df, ["id"])
    assert out.select(F.length("id")).agg(F.max("length(id)")).first()[0] <= 1  # "A" ou None

    out2 = etl_core.cast_columns(out, {"amount": "double"})
    assert dict(out2.dtypes)["amount"] in ("double", "double precision")


def test_normalize_dates_adds_ano_mes(sample_df):
    """
    normalize_dates: cria 'ano' e 'mes' a partir da 1ª coluna.
    """
    out = etl_core.normalize_dates(sample_df, ["created_at"], "yyyy-MM-dd", project_ano_mes=True)
    cols = set(out.columns)
    assert "ano" in cols and "mes" in cols


def test_deduplicate_keeps_first(sample_df):
    """
    deduplicate: remove duplicidade por chave com prioridade via order_by.
    """
    # Linhas duplicadas em id="B" (ver conftest): manter created_at mais recente
    out = etl_core.deduplicate(sample_df, keys=["id"], order_by=["created_at desc"])
    assert out.count() == 3
    # "B" deve restar apenas 1 linha
    assert out.filter(F.col("id") == "B").count() == 1


def test_coerce_date_and_clamp_and_drop(sample_df):
    """
    coerce_date / clamp_range / drop_if_null: fluxo básico.
    """
    df = sample_df.withColumn("weird_date", F.when(F.col("id") == "A", F.lit("01/01/2024")).otherwise(F.lit("2024-01-02")))
    df2 = etl_core.coerce_date(df, "weird_date", from_patterns=["dd/MM/yyyy", "yyyy-MM-dd"], to_format="yyyy-MM-dd")
    assert set(df2.select("weird_date").distinct().rdd.map(lambda r: r[0]).collect()) <= {"2024-01-01", "2024-01-02", None}

    df3 = etl_core.clamp_range(sample_df.withColumn("x", F.lit(200)), "x", min=0, max=100)
    assert df3.select("x").first()[0] == 100

    df4 = etl_core.drop_if_null(sample_df, ["id"])
    assert df4.filter(F.col("id").isNull()).count() == 0
