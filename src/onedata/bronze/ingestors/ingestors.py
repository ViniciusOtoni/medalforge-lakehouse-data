"""
Ingestors concretos baseados em Auto Loader (cloudFiles) para CSV, JSON e TXT-delimitado.
As classes abaixo herdam o fluxo de ingestão de DataIngestor.ingest() (Template Method)
e customizam apenas defaults/validações via hooks.
"""

from __future__ import annotations
from typing import Any, Dict
from bronze.interfaces.ingestor_interfaces import (
    StructuredDataIngestor,
    SemiStructuredDataIngestor,
)
# OBS: TXT "delimitado" também herda de StructuredDataIngestor (ver classe DelimitedTextIngestor)


class CSVIngestor(StructuredDataIngestor):
    """
    Ingestor para arquivos CSV via Auto Loader.

    Objetivo
    --------
    Aplicar leitura de CSV (Auto Loader cloudFiles → "csv") com defaults e validações
    típicas de CSV, delegando o fluxo completo de ingestão (leitura → auditoria →
    pós-leitura → escrita) ao método herdado `DataIngestor.ingest()`.

    Parâmetros esperados (herdados de DataIngestor)
    ----------------------------------------------
    spark : SparkSession
        Sessão Spark ativa.
    target_table : str
        Tabela alvo no formato FQN (catalog.schema.table).
    schema_struct : StructType
        Schema a ser aplicado na leitura.
    partitions : list[str]
        Colunas de particionamento físico (opcional).
    reader_options : dict[str, Any]
        Opções específicas do leitor (sobrepõem os defaults da família CSV).
        Ex.: {"delimiter": ";", "header": "true", "nullValue": ""}
    source_directory : str
        Diretório/prefixo de origem (RAW).
    checkpoint_location : str
        Caminho do checkpoint do stream.

    Métodos sobrescritos (hooks)
    ----------------------------
    (nenhum obrigatório)
    - Pode-se sobrescrever `default_reader_options()` para alterar defaults.
    - Pode-se sobrescrever `validate_reader_options()` para validações extras.

    Retorno
    -------
    N/A (o método `ingest()` herdado não retorna; realiza o streaming write para UC)

    Exceções
    --------
    Pode propagar exceções do Spark durante leitura/escrita/stream.
    """
    # A classe já herda defaults e validações adequados de StructuredDataIngestor.
    # Se quiser customizar algo de CSV em um caso específico (ex.: quote/escape),
    # basta sobrescrever `default_reader_options()` ou `validate_reader_options()`.


class JSONIngestor(SemiStructuredDataIngestor):
    """
    Ingestor para arquivos JSON via Auto Loader.

    Objetivo
    --------
    Aplicar leitura de JSON (Auto Loader cloudFiles → "json") com defaults e
    validações típicas (ex.: coerência de "multiline") e delegar o fluxo completo
    de ingestão ao método herdado `DataIngestor.ingest()`.

    Parâmetros esperados (herdados de DataIngestor)
    ----------------------------------------------
    spark : SparkSession
        Sessão Spark ativa.
    target_table : str
        Tabela alvo no formato FQN (catalog.schema.table).
    schema_struct : StructType
        Schema a ser aplicado na leitura.
    partitions : list[str]
        Colunas de particionamento físico (opcional).
    reader_options : dict[str, Any]
        Opções específicas do leitor (sobrepõem os defaults de JSON).
        Ex.: {"multiline": "true"} quando a fonte é JSON array; para NDJSON, "false".
    source_directory : str
        Diretório/prefixo de origem (RAW).
    checkpoint_location : str
        Caminho do checkpoint do stream.

    Métodos sobrescritos (hooks)
    ----------------------------
    - validate_reader_options(opts): valida coerência de "multiline".

    Retorno
    -------
    N/A (o método `ingest()` herdado não retorna; realiza o streaming write para UC)

    Exceções
    --------
    ValueError: quando "multiline" possui valor inválido.
    Pode propagar exceções do Spark durante leitura/escrita/stream.
    """

    def validate_reader_options(self, opts: Dict[str, Any]) -> None:
        """
        Valida coerência de opções JSON.

        Parâmetros
        ----------
        opts : Dict[str, Any]
            Opções finais aplicadas ao reader (defaults + overrides).

        Retorno
        -------
        None

        Exceções
        --------
        ValueError
            Se 'multiline' não estiver em {"true", "false"} (quando fornecido).
        """
        ml = opts.get("multiline")
        if ml is not None and str(ml).lower() not in {"true", "false"}:
            raise ValueError("JSON: 'multiline' deve ser 'true' ou 'false'.")


class DelimitedTextIngestor(StructuredDataIngestor):
    """
    Ingestor para arquivos TXT delimitados (tratados como CSV) via Auto Loader.

    Observação
    ----------
    TXT "delimitado" é conceitualmente CSV. Por isso, herdamos de StructuredDataIngestor
    (formato 'csv') e apenas reforçamos a obrigatoriedade do 'delimiter' via validação.

    Parâmetros esperados (herdados de DataIngestor)
    ----------------------------------------------
    spark : SparkSession
        Sessão Spark ativa.
    target_table : str
        Tabela alvo no formato FQN (catalog.schema.table).
    schema_struct : StructType
        Schema a ser aplicado na leitura.
    partitions : list[str]
        Colunas de particionamento físico (opcional).
    reader_options : dict[str, Any]
        Deve conter 'delimiter' obrigatoriamente.
        Ex.: {"delimiter": "|", "header": "false"}
    source_directory : str
        Diretório/prefixo de origem (RAW).
    checkpoint_location : str
        Caminho do checkpoint do stream.

    Métodos sobrescritos (hooks)
    ----------------------------
    - default_reader_options(): usa defaults de CSV e permite override.
    - validate_reader_options(opts): exige 'delimiter'.

    Retorno
    -------
    N/A (o método `ingest()` herdado não retorna; realiza o streaming write para UC)

    Exceções
    --------
    ValueError: se 'reader_options["delimiter"]' estiver ausente/vazio.
    Pode propagar exceções do Spark durante leitura/escrita/stream.
    """

    def default_reader_options(self) -> Dict[str, Any]:
        """
        Opções default baseadas em CSV para TXT-delimitado.

        Retorno
        -------
        Dict[str, Any]
            Defaults herdados de CSV + qualquer ajuste desejado para TXT.
        """
        # Reaproveita os defaults de StructuredDataIngestor e,
        # se quiser, adicione/ajuste algo específico aqui:
        opts = super().default_reader_options()
        # Ex.: remover header como default para TXT, se preferir:
        # opts["header"] = "false"
        return opts

    def validate_reader_options(self, opts: Dict[str, Any]) -> None:
        """
        Validações específicas para TXT-delimitado.

        Parâmetros
        ----------
        opts : Dict[str, Any]
            Opções finais aplicadas ao reader (defaults + overrides).

        Retorno
        -------
        None

        Exceções
        --------
        ValueError
            Se 'delimiter' não existir ou estiver vazio.
        """
        delimiter = opts.get("delimiter")
        if delimiter is None or str(delimiter) == "":
            raise ValueError(
                "TXT-delimitado: 'reader_options[\"delimiter\"]' é obrigatório e não pode ser vazio."
            )
        # Também reutiliza validações de CSV (delimiter não vazio)
        super().validate_reader_options(opts)
