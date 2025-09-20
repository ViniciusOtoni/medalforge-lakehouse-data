"""
Fixtures compartilhadas:
- Spark local para DataFrames
- DataFrame de exemplo (id, created_at, amount)
"""

from __future__ import annotations
import pytest
from pyspark.sql import SparkSession

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)


@pytest.fixture(scope="session")
def spark():
    """Cria uma SparkSession 'local[*]' para a suíte."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    """
    DataFrame simples para transformar/testar.

    Colunas
    -------
    id          : str
    created_at  : str (yyyy-MM-dd)
    amount      : str (será convertida em numérica em alguns testes)
    """
    data = [
        ("A", "2024-01-01", "10"),
        ("B", "2024-01-02", "100"),
        ("B", "2024-01-03", "100"),  # duplicado para dedup nos testes do Silver 
        (None, "2024-01-01", "5"),
    ]
    return spark.createDataFrame(data, ["id", "created_at", "amount"])
