"""
Integração leve do runner Bronze (main.py) com mocks para IO/stream.

Objetivo
--------
- Exercitar o fluxo `main()`/`IngestorFactory.create().ingest()` sem Delta/Autoloader.

Estratégia
----------
- Criar contrato JSON mínimo em arquivo temporário.
- Monkeypatch:
  * IngestorFactory.create -> retorna FakeIngestor (registra chamada de ingest).
  * TableManager.ensure_external_table -> no-op (evita DDL real).
- Executar main() parcialmente invocando funções internas (sem process builder de argparse).
"""

import json
import types
import tempfile
import pathlib
import sys

import bronze.managers.table_manager as tm_mod
import bronze.ingestors.factory as factory_mod
import bronze.main as bronze_main


def test_bronze_runner_happy_path(monkeypatch, spark, tmp_path, capsys):
    """
    Fluxo feliz:
    - contrato mínimo
    - ingestor fake captura include_existing_files
    - ensure_external_table no-op
    - execução imprime JSON final (status planned/ingested)
    """
    # 1) contrato JSON mínimo (TableContract em formato dict)
    contract = {
        "version": "1.0",
        "catalog": "bronze",
        "schema": "sales",
        "table": "orders",
        "columns": [{"name": "id", "dtype": "string"}],
        "source": {"format": "csv", "options": {"header": True}},
    }
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    # 2) patches
    # 2.1 TableManager.ensure_external_table -> no-op
    #     IMPORTANTE: patch no símbolo referenciado por bronze_main (evita CREATE SCHEMA real)
    monkeypatch.setattr(bronze_main.TableManager, "ensure_external_table", lambda *a, **k: None)

    # 2.2 Fake ingestor
    calls = {}

    class FakeIngestor:
        def __init__(self, **kwargs):
            self.kw = kwargs
        def ingest(self, include_existing_files=True):
            calls["ingested"] = True
            calls["include_existing_files"] = include_existing_files

    # mantém a fábrica, mas troca o create para devolver o fake
    monkeypatch.setattr(factory_mod, "IngestorFactory", factory_mod.IngestorFactory)
    monkeypatch.setattr(
        factory_mod.IngestorFactory, "create",
        lambda **kwargs: FakeIngestor(**kwargs)
    )

    # 3) simula CLI chamando main() com args: --mode validate+plan+ingest
    argv_bkp = list(sys.argv)
    sys.argv = [
        argv_bkp[0],
        "--contract_path", str(contract_path),
        "--mode", "validate+plan+ingest",
    ]
    try:
        bronze_main.main()
    finally:
        sys.argv = argv_bkp

    # 4) assert: ingest chamado e stdout com JSON
    assert calls.get("ingested") is True
    captured = capsys.readouterr().out
    assert '"ingested"' in captured
    assert '"planned"' in captured
