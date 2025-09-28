"""
Entrada CLI do pipeline Silver.
"""

from __future__ import annotations
import argparse, yaml
from pyspark.sql import SparkSession
from onedata.silver.domain import SilverYaml
from onedata.silver.application.pipeline import run_pipeline

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--contract_path", required=True, help="Caminho do contrato YAML da Silver")
    return p.parse_args()

def main():
    args = _parse_args()
    with open(args.contract_path, "r", encoding="utf-8") as fh:
        cfg = SilverYaml(**yaml.safe_load(fh))
    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark, cfg)

if __name__ == "__main__":
    main()
