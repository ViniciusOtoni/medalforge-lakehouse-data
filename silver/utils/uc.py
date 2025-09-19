"""
Utilitários para Unity Catalog:
- split_fqn: valida e divide um FQN (catalog.schema.table).
- ensure_schema: cria schema no UC se não existir.
- ensure_catalog_schema: garante o schema com mensagem amigável em caso de falta de permissão.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def split_fqn(fqn: str) -> tuple[str, str, str]:
    """
    Divide um nome totalmente qualificado (FQN) no formato catalog.schema.table.

    Parâmetros
    - fqn: string no formato "catalog.schema.table".

    Retorno
    - tuple[str, str, str]: (catalog, schema, table)

    Exceptions
    - ValueError: se o FQN não tiver exatamente 3 partes separadas por ponto.
    """
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Esperado FQN no formato catalog.schema.table, recebido: '{fqn}'"
        )
    return parts[0], parts[1], parts[2]


def ensure_schema(catalog: str, schema: str) -> None:
    """
    Cria o schema no Unity Catalog caso não exista (idempotente).

    Parâmetros
    - catalog: nome do catálogo.
    - schema: nome do schema.

    Retorno
    - None

    Observação
    - Não usa quoting/backticks; assuma nomes válidos em UC.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ensure_catalog_schema(catalog: str, schema: str) -> None:
    """
    Garante a existência de um schema no UC e melhora a mensagem de erro
    quando a falha decorre de permissão insuficiente.

    Parâmetros
    - catalog: nome do catálogo.
    - schema: nome do schema.

    Retorno
    - None

    Exceptions
    - RuntimeError: se o erro indicar falta de permissão (orienta sobre USAGE/CREATE ao SPN).
    - Relevanta a exceção original para outros tipos de erro.
    """
    # Schema costuma requerer privilégios do owner/administrador.
    try:
        ensure_schema(catalog=catalog, schema=schema)
    except Exception as e:
        msg = str(e).lower()
        if "permission" in msg or "not authorized" in msg:
            raise RuntimeError(
                f"Sem permissão para criar o schema '{catalog}.{schema}'. "
                f"Crie-o previamente e conceda USAGE/CREATE ao SPN do job."
            ) from e
        raise
