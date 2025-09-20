"""
Testes unitários dos ingestors (CSV/JSON/TEXT) com mocks do Auto Loader.

Intuito
-------
Validar o encadeamento de chamadas de leitura/escrita (readStream → option/schema/load → 
writeStream → toTable) sem acionar o Auto Loader real – apenas verificando fluxos felizes 
e erros de validação.

Escopo
------
- Exercita CSVIngestor, JSONIngestor e TextIngestor.
- Usa um DataFrame real (fixture `sample_df`) acoplado a um `writeStream` mockado.
"""

from types import SimpleNamespace
from typing import Any, Dict
import pytest
from bronze.ingestors.ingestors import CSVIngestor, JSONIngestor, TextIngestor


class _DummyWrite:
    """
    Mock minimalista de DataStreamWriter (API encadeável).

    Propósito
    ---------
    Capturar opções passadas pelo ingestor e interceptar `toTable` sem escrever de verdade.

    Atributos
    ---------
    opts : Dict[str, Any]
        Armazena opções e metadados (ex.: partitionBy, tableName).

    Métodos encadeáveis relevantes
    ------------------------------
    - format(source: str) -> _DummyWrite
    - option(key: str, value: Any) -> _DummyWrite
    - outputMode(outputMode: str) -> _DummyWrite
    - trigger(availableNow: bool = False, **_) -> _DummyWrite
    - partitionBy(*cols: str) -> _DummyWrite
    - toTable(tableName: str) -> None
    """

    def __init__(self) -> None:
        self.opts: Dict[str, Any] = {}

    def format(self, source: str):
        self.opts["format"] = source
        return self

    def option(self, key: str, value: Any):
        self.opts[key] = value
        return self

    def outputMode(self, outputMode: str):
        self.opts["outputMode"] = outputMode
        return self

    def trigger(self, availableNow: bool = False, **_: Any):
        self.opts["trigger.availableNow"] = availableNow
        return self

    def partitionBy(self, *cols: str):
        self.opts["partitionBy"] = cols
        return self

    def toTable(self, tableName: str) -> None:
        """
        Simula o sink para tabela Delta.

        Parâmetros
        ----------
        tableName : str
            Nome totalmente qualificado da tabela de destino.

        Retorno
        -------
        None
        """
        self.opts["tableName"] = tableName


class _DummyRead:
    """
    Mock minimalista de DataStreamReader (API encadeável).

    Propósito
    ---------
    Emular a sequência `format/option/schema/load` do Auto Loader e devolver
    um “DataFrame” com atributo `writeStream` mockado.

    Atributos
    ---------
    df : Any
        DataFrame real (fixture `sample_df`) para encadear transforms de forma inofensiva.
    opts : Dict[str, Any]
        Opções passadas ao reader (para inspeção se necessário).
    fmt : str | None
        Formato solicitado (ex.: "cloudFiles").

    Métodos relevantes
    ------------------
    - format(source: str) -> _DummyRead
    - option(key: str, value: Any) -> _DummyRead
    - schema(schema: Any) -> _DummyRead
    - load(path: str) -> SimpleNamespace(writeStream=_DummyWrite(), withColumn=...)
    """

    def __init__(self, df: Any) -> None:
        self.df = df
        self.opts: Dict[str, Any] = {}
        self.fmt: str | None = None

    def format(self, source: str):
        self.fmt = source
        return self

    def option(self, key: str, value: Any):
        self.opts[key] = value
        return self

    def schema(self, schema: Any):
        # Apenas armazena se quiser inspecionar depois
        self.opts["schema.set"] = True
        return self

    def load(self, path: str) -> SimpleNamespace:
        """
        Simula a leitura do diretório de origem.

        Parâmetros
        ----------
        path : str
            Caminho de origem (ignorado para o mock).

        Retorno
        -------
        SimpleNamespace
            Objeto com:
              - writeStream: _DummyWrite()
              - withColumn: lambda que retorna o `df` real (permite encadear).
        """
        return SimpleNamespace(
            writeStream=_DummyWrite(),
            withColumn=lambda *a, **k: self.df,
        )


@pytest.fixture
def patch_streams(monkeypatch: pytest.MonkeyPatch, spark, sample_df):
    """
    Fixture
    -------
    Substitui `spark.readStream` por `_DummyRead(sample_df)`, acoplando `writeStream`
    falso ao DataFrame real de exemplo.

    Parâmetros
    ----------
    monkeypatch : pytest.MonkeyPatch
        Utilitário do pytest para sobrescrever atributos temporariamente.
    spark : SparkSession
        Sessão Spark da suíte de testes.
    sample_df : DataFrame
        DataFrame real (fixture) usado como base para encadeamento.

    Retorno
    -------
    SparkSession
        A mesma sessão Spark, porém com `readStream` monkeypatched.
    """
    monkeypatch.setattr(spark, "readStream", _DummyRead(sample_df))
    return spark


def _build_ingestor(Cls, spark):
    """
    Helper para instanciar ingestors com parâmetros nomeados de forma consistente.

    Parâmetros
    ----------
    Cls : type
        Classe do ingestor (CSVIngestor | JSONIngestor | TextIngestor).
    spark : SparkSession
        Sessão Spark.

    Retorno
    -------
    DataIngestor
        Instância inicializada com fqn/partitions/options/paths padrões de teste.
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


def test_csv_ingestor_calls_read_write_ok(patch_streams):
    """
    CSVIngestor: verifica o fluxo feliz de leitura e escrita.

    Intuito
    -------
    Exercitar:
      - format=cloudFiles
      - options cloudFiles.includeExistingFiles e cloudFiles.schemaLocation
      - mergeSchema + checkpoint + availableNow
      - partitionBy + toTable

    Critério
    --------
    Não deve lançar exceção.
    """
    ing = _build_ingestor(CSVIngestor, patch_streams)
    ing.ingest(include_existing_files=True)


def test_json_ingestor_multiline_option_ok(patch_streams):
    """
    JSONIngestor: aplica 'multiline' quando presente.

    Intuito
    -------
    Verificar que a opção `multiline` é aceita e não causa erro no caminho
    `include_existing_files=False`.

    Critério
    --------
    Não deve lançar exceção.
    """
    ing = _build_ingestor(JSONIngestor, patch_streams)
    ing.reader_options["multiline"] = True
    ing.ingest(include_existing_files=False)


def test_text_ingestor_requires_delimiter(patch_streams):
    """
    TextIngestor: exige `reader_options["delimiter"]`.

    Intuito
    -------
    Garantir validação de pré-condição para TXT delimitado.

    Critério
    --------
    - Sem delimiter → deve levantar ValueError.
    - Com delimiter → execução segue sem exceção.
    """
    ing = _build_ingestor(TextIngestor, patch_streams)
    with pytest.raises(ValueError):
        ing.ingest()

    ing.reader_options["delimiter"] = "|"
    ing.ingest()
