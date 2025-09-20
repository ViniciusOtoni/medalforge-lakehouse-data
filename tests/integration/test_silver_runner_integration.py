"""
Teste de integração leve do pipeline Silver (run).

Intuito
-------
- Executar `silver/main.run` ponta a ponta com mocks para:
  * leitura da Bronze
  * DQX (split)
  * customs (no-op)
  * merge_upsert (captura dos argumentos)
  * ensure_catalog_schema (no-op)

- Verificar que:
  * o contrato é lido
  * o pipeline chama merge_upsert com FQN e chaves esperadas
  * nenhuma exceção é lançada
"""

import textwrap
from pathlib import Path
from pyspark.sql import functions as F
from silver import main as silver_main


def test_run_happy_path(monkeypatch, tmp_path, spark, sample_df):
    """
    run: cenário feliz sem quarentena persistida; customs desabilitados.
    """
    # 1) mock bronze read
    def _tbl(name: str):
        # ignora 'name' e devolve sample_df com tipos simples
        return sample_df

    monkeypatch.setattr(silver_main.spark, "table", _tbl)

    # **EVITA resolver namespace 3-part em catálogos locais (spark_catalog)**
    # tableExists('silver.sales.t_clean') em Spark local pode falhar com 3-part.
    monkeypatch.setattr(silver_main.spark.catalog, "tableExists", lambda *_a, **_k: True)

    # 2) mock DQX split → tudo válido, quarentena vazia
    def _split(df, _cfg):
        return df, df.limit(0)

    monkeypatch.setattr(silver_main, "apply_checks_split", _split)

    # 3) customs no-op
    monkeypatch.setattr(silver_main, "apply_customs_stage", lambda df, *_a, **_k: df)

    # 4) ensure_catalog_schema no-op
    monkeypatch.setattr(silver_main, "ensure_catalog_schema", lambda *_a, **_k: None)

    # 5) merge_upsert capturando argumentos
    called = {}
    def _merge(df, tgt, keys, zorder_by, partition_by, external_base):
        called.update(dict(tgt=tgt, keys=keys, zorder_by=zorder_by, partition_by=partition_by, external_base=external_base))
        # força avaliação leve
        _ = df.limit(1).count()
    monkeypatch.setattr(silver_main, "merge_upsert", _merge)

    # 6) contrato YAML mínimo
    yaml_text = textwrap.dedent(
        """
        version: "1.0"
        source:
          bronze_table: "bronze.sales.t_raw"
        target:
          catalog: "silver"
          schema: "sales"
          table: "t_clean"
          write:
            mode: "merge"
            merge_keys: ["id"]
            partition_by: []
            zorder_by: ["id"]
        dqx:
          criticality_default: "error"
          checks: []
          custom: []
        etl:
          standard:
            - method: "trim_columns"
              args: { columns: ["id"] }
        quarantine:
          remediate: []
          sink: null
        customs:
          allow: false
          registry: []
          use_in: []
        """
    )
    contract_path = Path(tmp_path) / "silver_contract.yaml"
    contract_path.write_text(yaml_text, encoding="utf-8")

    # 7) executa
    silver_main.run(str(contract_path))

    # 8) asserções chave
    assert called["tgt"] == "silver.sales.t_clean"
    assert called["keys"] == ["id"]
    assert called["zorder_by"] == ["id"]
    assert isinstance(called["external_base"], str) and called["external_base"].startswith("abfss://silver@")
