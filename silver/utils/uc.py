from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def split_fqn(fqn: str) -> tuple[str, str, str]:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Esperado FQN no formato catalog.schema.table, recebido: '{fqn}'")
    return parts[0], parts[1], parts[2]



def ensure_schema(catalog: str, schema: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def ensure_catalog_schema(catalog: str, schema: str):
    
    # Schema costuma ser permitido ao owner/privileged; se não, a mensagem orienta.
    try:
        ensure_schema(catalog, schema)
    except Exception as e:
        msg = str(e).lower()
        if "permission" in msg or "not authorized" in msg:
            raise RuntimeError(
                f"Sem permissão para criar o schema '{catalog}.{schema}'. "
                f"Crie-o previamente e conceda USAGE/CREATE ao SPN do job."
            ) from e
        raise
