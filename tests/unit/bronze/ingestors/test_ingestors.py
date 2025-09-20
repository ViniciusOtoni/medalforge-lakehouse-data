"""
Testes unitários dos ingestors (CSV/JSON/TEXT) com mocks do Auto Loader.

Relação com código sob teste
----------------------------
Este arquivo valida o módulo: bronze/ingestors/ingestors.py
- CSVIngestor.ingest
- JSONIngestor.ingest
- TextIngestor.ingest

Intuito
-------
Verificar o encadeamento de chamadas do Auto Loader (readStream → format/option/schema/load →
withColumn* → writeStream → toTable) sem acionar streaming real.

Estratégia de mock
------------------
- `SparkSession.readStream` é uma @property; por isso, o patch é feito **na classe**
  substituindo-a por uma property que retorna nosso `_DummyRead`.
- `_DummyRead.load()` retorna um proxy de DF com:
    * `writeStream` → `_DummyWrite` (mock do DataStreamWriter)
    * `withColumn(...)` que mantém o encadeamento retornando o próprio proxy
- Assim, todo o fluxo feliz é exercitado, mas nada roda de verdade.
"""

from __future__ import annotations
from typing import Any, Dict
import pytest
from types import SimpleNamespace
from pyspark.sql import SparkSession

from bronze.ingestors.ingestors import CSVIngestor, JSONIngestor, TextIngestor


# -------------------- Dummies / Proxies --------------------

class _DummyWrite:
    """
    Mock do DataStreamWriter com API encadeável mínima.

    Propósito
    ---------
    Capturar parâmetros e interceptar `toTable` sem I/O real.

    Atributos
    ---------
    opts : Dict[str, Any]
        Dicionário de opções/flags (ex.: checkpointLocation, partitionBy, tableName).
    """
    def __init__(self) -> None:
        self.opts: Dict[str, Any] = {}

    def format(self, source: str) -> "_DummyWrite":
        self.opts["format"] = source
        return self

    def option(self, key: str, value: Any) -> "_DummyWrite":
        self.opts[key] = value
        return self

    def outputMode(self, outputMode: str) -> "_DummyWrite":
        self.opts["outputMode"] = outputMode
        return self

    def trigger(self, availableNow: bool = False, **_: Any) -> "_DummyWrite":
        self.opts["trigger.availableNow"] = availableNow
        return self

    def partitionBy(self, *cols: str) -> "_DummyWrite":
        self.opts["partitionBy"] = cols
        return self

    def toTable(self, tableName: str) -> None:
        """
        Simula a chamada final de sink para Delta.

        Parâmetros
        ----------
        tableName : str
            Tabela alvo totalmente qualificada.
        """
        self.opts["tableName"] = tableName
        # nada a fazer (sem I/O)


class _DFProxy:
    """
    Proxy que emula um DataFrame de streaming apenas o suficiente
    para a cadeia `withColumn(...).withColumn(...).writeStream...`.
    """
    def __init__(self) -> None:
        self.writeStream = _DummyWrite()

    def withColumn(self, *_: Any, **__: Any) -> "_DFProxy":
        # mantém encadeamento sempre retornando o próprio proxy
        return self


class _DummyRead:
    """
    Mock do DataStreamReader (Auto Loader) com API encadeável.

    Atributos
    ---------
    opts : Dict[str, Any]
        Opções passadas via `.option(...)`.
    fmt : str | None
        Formato do reader (ex.: "cloudFiles").
    """
    def __init__(self) -> None:
        self.opts: Dict[str, Any] = {}
        self.fmt: str | None = None

    def format(self, source: str) -> "_DummyRead":
        self.fmt = source
        return self

    def option(self, key: str, value: Any) -> "_DummyRead":
        self.opts[key] = value
        return self

    def schema(self, schema: Any) -> "_DummyRead":
        # marca que schema foi definido (útil para debug, se quiser inspecionar)
        self.opts["schema.set"] = True
        return self

    def load(self, path: str) -> _DFProxy:
        # devolve o proxy de DF (com writeStream mock e withColumn encadeável)
        return _DFProxy()


# -------------------- Fixtures --------------------

@pytest.fixture
def patch_streams(monkeypatch: pytest.MonkeyPatch, spark):
    """
    Substitui `SparkSession.readStream` (property) por outra property
    que devolve `_DummyRead()`.

    Por que na classe?
    ------------------
    `readStream` é uma @property; trocar na instância falha com
    "AttributeError: can't set attribute". Por isso o patch é aplicado
    em `SparkSession` diretamente, com `raising=True` para restaurar no teardown.

    Retorno
    -------
    SparkSession
        A mesma sessão Spark já com o mock ativo.
    """
    mock_prop = property(lambda self: _DummyRead())
    monkeypatch.setattr(SparkSession, "readStream", mock_prop, raising=True)
    return spark


def _build_ingestor(Cls, spark):
    """
    Helper: instancia ingestors com parâmetros padrão de teste.

    Parâmetros
    ----------
    Cls : type[CSVIngestor|JSONIngestor|TextIngestor]
    spark : SparkSession

    Retorno
    -------
    Instância do ingestor indicado.
    """
    return Cls(
        spark=spark,
        fqn="bronze.sales.t1",
        schema_struct=None,
        partitions=["ingestion_date"],
        reader_options={},
        source_directory="/raw",
        checkpoint_location="/chk",
    )


# -------------------- Testes --------------------

def test_csv_ingestor_calls_read_write_ok(patch_streams):
    """
    CSVIngestor: fluxo feliz com trigger=availableNow, mergeSchema e toTable.

    Verifica
    --------
    - Não lança exceção ao encadear Auto Loader + writeStream.
    """
    ing = _build_ingestor(CSVIngestor, patch_streams)
    ing.ingest(include_existing_files=True)  # apenas exercita o caminho feliz


def test_json_ingestor_multiline_option_ok(patch_streams):
    """
    JSONIngestor: aplica 'multiline' quando presente e usa include_existing_files=False.

    Verifica
    --------
    - Não lança exceção.
    """
    ing = _build_ingestor(JSONIngestor, patch_streams)
    ing.reader_options["multiline"] = True
    ing.ingest(include_existing_files=False)


def test_text_ingestor_requires_delimiter(patch_streams):
    """
    TextIngestor: valida obrigatoriedade de `reader_options["delimiter"]`.

    Cenários
    --------
    - Sem delimiter → `ValueError`
    - Com delimiter → caminho feliz
    """
    ing = _build_ingestor(TextIngestor, patch_streams)
    with pytest.raises(ValueError):
        ing.ingest()  # falta delimiter

    ing.reader_options["delimiter"] = "|"
    ing.ingest()  # agora deve passar
