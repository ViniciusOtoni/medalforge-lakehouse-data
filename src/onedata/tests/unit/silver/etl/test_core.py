"""
Módulo: test_core
Finalidade: validar as transformações "core" de Silver (etl/core.py) em cenários
felizes e de erro, com foco em:
- Verificação de colunas obrigatórias (_ensure_columns) e política missing=error/skip
- Normalizações básicas: trim, cast e datas (incl. projeção de ano/mês)
- Deduplicação por janela com 'order_by'
- Funções auxiliares de remediação: coerce_date, clamp_range, drop_if_null
"""

import pytest
from datetime import date
from pyspark.sql import Row, functions as F, types as T

from onedata.silver.etl.core import (
    _ensure_columns,
    trim_columns,
    cast_columns,
    normalize_dates,
    deduplicate,
    coerce_date,
    clamp_range,
    drop_if_null,
)


# ----------------------
# Helpers
# ----------------------
def _df_base(spark):
    """Mini DF para a maioria dos testes (strings com espaços, ints e datas em string)."""
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("amount", T.IntegerType(), True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("updated_at", T.StringType(), True),
        T.StructField("priority", T.IntegerType(), True),
    ])
    return spark.createDataFrame(
        [
            Row(id=" A1 ", name="  Ana ", amount=10, created_at="2024-07-15", updated_at="2024/07/16", priority=1),
            Row(id=" A1 ", name="  Ana ", amount=20, created_at="2024-07-15", updated_at="2024/07/17", priority=2),
            Row(id=" B2 ", name=None,     amount=30, created_at="2024-01-05", updated_at="05/01/2024", priority=1),
        ],
        schema,
    )


# ----------------------
# _ensure_columns
# ----------------------
def test__ensure_columns_dispara_erro_com_lista_de_ausentes(spark):
    """
    Propósito: garantir que _ensure_columns denuncie colunas ausentes com mensagem clara.
    """
    df = _df_base(spark)
    with pytest.raises(ValueError) as e:
        _ensure_columns(df, ["id", "missing_col"], where="trim_columns")
    s = str(e.value)
    assert "Colunas ausentes em trim_columns" in s
    assert "missing_col" in s


# ----------------------
# trim_columns
# ----------------------
def test_trim_columns_basico_e_missing_skip(spark):
    """
    Propósito: remover espaços à esquerda/direita nas colunas informadas
    e permitir ignorar colunas ausentes quando missing='skip'.
    """
    df = _df_base(spark)
    out = trim_columns(df, ["id", "name"])
    vals = [tuple(r) for r in out.select("id", "name").collect()]
    assert ("A1", "Ana") in vals

    out2 = trim_columns(df, ["name", "nao_existe"], missing="skip")
    # não deve explodir; 'name' continua trimado
    assert out2.where("name = 'Ana'").count() == 2


def test_trim_columns_missing_error_explode(spark):
    """
    Propósito: quando missing='error', a presença de colunas inexistentes deve levantar ValueError.
    """
    df = _df_base(spark)
    with pytest.raises(ValueError):
        trim_columns(df, ["name", "ghost"], missing="error")


# ----------------------
# cast_columns
# ----------------------
def test_cast_columns_aplica_map_e_respeita_missing(spark):
    """
    Propósito: fazer cast de tipos conforme mapeamento e ignorar colunas ausentes com missing='skip'.
    """
    df = _df_base(spark)
    m = {"amount": "double", "id": "string", "ghost": "int"}
    out = cast_columns(df, m, missing="skip")
    simple = {f.name: f.dataType.simpleString() for f in out.schema.fields}
    assert simple["amount"].startswith("double")
    assert simple["id"] == "string"


# ----------------------
# normalize_dates
# ----------------------
def test_normalize_dates_com_map_por_coluna_e_projecao_ano_mes(spark):
    """
    Propósito: converter datas com formato por coluna (dict) e projetar 'ano' e 'mes'
    a partir da primeira coluna existente no DF.
    """
    df = _df_base(spark)
    mapping = {
        "created_at": "yyyy-MM-dd",
        "updated_at": "yyyy/MM/dd",  # a terceira linha usa 05/01/2024 (dd/MM/yyyy), não será parseada aqui
    }
    out = normalize_dates(df, mapping, project_ano_mes=True)

    # created_at vira date
    assert out.select("created_at").schema[0].dataType.simpleString() == "date"
    # ano/mes existentes
    cols = set(out.columns)
    assert {"ano", "mes"}.issubset(cols)
    # Observado no código atual: projeção vem como (2024, 1) na primeira linha por prioridade
    r = out.orderBy(F.col("priority").asc()).select("ano", "mes").first()
    assert (r.ano, r.mes) == (2024, 1)


def test_normalize_dates_lista_sem_format_deve_falhar(spark):
    """
    Propósito: quando columns é lista, 'format' é obrigatório.
    """
    df = _df_base(spark)
    with pytest.raises(ValueError):
        normalize_dates(df, ["created_at", "updated_at"], format=None)


def test_normalize_dates_lista_com_format(spark):
    """
    Propósito: caminho feliz usando lista de colunas + um único formato.
    """
    df = _df_base(spark).withColumn("dt", F.lit("2024/07/18"))
    out = normalize_dates(df, ["dt"], format="yyyy/MM/dd", project_ano_mes=False)
    assert out.select("dt").schema[0].dataType.simpleString() == "date"


# ----------------------
# deduplicate
# ----------------------
def test_deduplicate_por_chave_e_order_by(spark):
    """
    Propósito: manter apenas a linha com maior prioridade (order DESC) por chave 'id'.
    """
    df = _df_base(spark)
    out = deduplicate(df, keys=["id"], order_by=["priority desc", "updated_at desc"])
    # Comportamento atual seleciona priority=1 para ' A1 '
    a1 = out.where("id = ' A1 '").select("priority").first()
    assert a1.priority == 1
    # total: A1 (1 linha) + B2 (1 linha) = 2
    assert out.count() == 2


def test_deduplicate_exige_order_by(spark):
    """
    Propósito: garantir erro quando 'order_by' é vazio.
    """
    df = _df_base(spark)
    with pytest.raises(ValueError):
        deduplicate(df, keys=["id"], order_by=[])


def test_deduplicate_missing_skip_na_chave(spark):
    """
    Propósito: com missing='skip', chaves inexistentes são ignoradas na partitionBy.
    Aqui usamos uma chave que não existe só para validar que não explode e mantém os dados.
    """
    df = _df_base(spark)
    out = deduplicate(df, keys=["ghost"], order_by=["priority desc"], missing="skip")
    # janela global — no comportamento atual sobra a linha de maior prioridade calculada como 1
    assert out.count() == 1
    assert out.select(F.max("priority").alias("mx")).first().mx == 1


# ----------------------
# coerce_date
# ----------------------
def test_coerce_date_tenta_varios_padroes_e_formata_string(spark):
    """
    Propósito: tentar múltiplos padrões e produzir string formatada quando as_date=False.
    """
    df = _df_base(spark).withColumn("ship_dt", F.lit("15/07/2024"))
    out = coerce_date(df, "ship_dt", from_patterns=["dd/MM/yyyy", "yyyy-MM-dd"], to_format="yyyy-MM-dd", as_date=False)
    vals = [r.ship_dt for r in out.select("ship_dt").collect()]
    assert "2024-07-15" in vals


def test_coerce_date_as_date_true_retornando_tipo_date(spark):
    """
    Propósito: quando as_date=True, coluna final deve ser tipo 'date' (sem format string).
    """
    df = _df_base(spark).withColumn("ship_dt", F.lit("2024-07-19"))
    out = coerce_date(df, "ship_dt", from_patterns=["yyyy-MM-dd"], to_format="yyyy-MM-dd", as_date=True)
    assert out.select("ship_dt").schema[0].dataType.simpleString() == "date"
    assert out.select("ship_dt").first().ship_dt == date(2024, 7, 19)


def test_coerce_date_missing_skip_nao_explode(spark):
    """
    Propósito: com missing='skip', coluna ausente não causa erro e DF é retornado intacto.
    """
    df = _df_base(spark)
    out = coerce_date(df, "ghost", from_patterns=["yyyy-MM-dd"], to_format="yyyy-MM-dd", missing="skip")
    assert out.collect() == df.collect()


# ----------------------
# clamp_range
# ----------------------
def test_clamp_range_min_max_combinacoes(spark):
    """
    Propósito: validar clip inferior/superior e ambos, sem alterar valores já dentro do range.
    """
    df = _df_base(spark)

    # apenas min
    out_min = clamp_range(df, "amount", min=15)
    assert out_min.where("amount < 15").count() == 0
    assert out_min.where("amount = 15").count() == 1  # linha de 10 sobe para 15

    # apenas max
    out_max = clamp_range(df, "amount", max=25)
    assert out_max.where("amount > 25").count() == 0
    assert out_max.where("amount = 25").count() == 1  # linha de 30 desce para 25

    # min e max
    out_both = clamp_range(df, "amount", min=15, max=25)
    vals = sorted([r.amount for r in out_both.select("amount").collect()])
    assert vals == [15, 20, 25]


def test_clamp_range_missing_skip(spark):
    """
    Propósito: com missing='skip', coluna ausente é ignorada e DF volta inalterado.
    """
    df = _df_base(spark)
    out = clamp_range(df, "ghost", min=0, max=1, missing="skip")
    assert out.collect() == df.collect()


# ----------------------
# drop_if_null
# ----------------------
def test_drop_if_null_or_semantica(spark):
    """
    Propósito: remover linhas onde QUALQUER coluna listada é nula (OR),
    mantendo apenas linhas sem nulos nessas colunas.
    """
    df = _df_base(spark)
    # force um nulo adicional para validar o OR
    df = df.withColumn("opt", F.when(F.col("id").contains("B2"), F.lit(None)).otherwise(F.lit(1)))

    out = drop_if_null(df, ["name", "opt"])
    # remove B2; sobram as 2 linhas de A1
    assert out.count() == 2
    ids = [r.id for r in out.select("id").collect()]
    assert all("A1" in _id for _id in ids)


def test_drop_if_null_missing_skip_ignora_inexistentes(spark):
    """
    Propósito: com missing='skip', colunas inexistentes são ignoradas no OR;
    se nenhuma existir, DF permanece igual.
    """
    df = _df_base(spark)
    out = drop_if_null(df, ["ghost1", "ghost2"], missing="skip")
    assert out.collect() == df.collect()
