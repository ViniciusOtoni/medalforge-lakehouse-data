"""
Módulo: test_dqx_stage
Finalidade: garantir que as funções do estágio DQX
 - funcionem com DQXCfg
 - retornem dois DataFrames válidos
(ajustado para o comportamento atual: passthrough)
"""

from pyspark.sql import Row, types as T
from onedata.silver.stages.dqx_stage import initial_split, recheck_after_remediation
from onedata.silver.domain.dqx import DQXCfg


def test_initial_split_passthrough(spark):
    """Comportamento atual: split retorna DF de entrada como 'válidos' e quarentena vazia."""
    df = spark.createDataFrame([Row(id="A"), Row(id=None)], T.StructType([T.StructField("id", T.StringType(), True)]))

    dqx = DQXCfg()  # defaults
    valid_df, quarantine_df = initial_split(df, dqx)

    assert valid_df.count() == df.count()
    assert quarantine_df.count() in (0, )


def test_recheck_after_remediation_passthrough(spark):
    """Comportamento atual: recheck também faz passthrough."""
    df = spark.createDataFrame([Row(x=1)], T.StructType([T.StructField("x", T.IntegerType())]))
    dqx = DQXCfg()
    valid_df, quarantine_df = recheck_after_remediation(df, dqx)

    assert valid_df.count() == 1
    assert quarantine_df.count() in (0, )
