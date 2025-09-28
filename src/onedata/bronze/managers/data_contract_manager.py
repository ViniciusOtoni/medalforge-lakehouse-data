"""
Gerencia o contrato de dados de uma tabela Bronze (schema, partições e origem).
Produz schema Spark, opções do Auto Loader e payload para os ingestors.
"""

from __future__ import annotations
from typing import List, Optional, Dict, Union, Any
from pydantic import BaseModel, Field, validator, constr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, DataType
)
import json, re

# Parser interno do Spark para tipos DDL complexos (array/map/struct); pode não existir em todas versões.
try:
    from pyspark.sql.types import _parse_datatype_string as _parse_ddl_type 
except Exception:  
    _parse_ddl_type = None

# Identificadores válidos (coluna/esquema/tabela)
_IDENT_REGEX = r"^[A-Za-z_][A-Za-z0-9_]*$"
try:
    Ident = constr(strip_whitespace=True, min_length=1, pattern=_IDENT_REGEX)  # pydantic v2
except TypeError:  # pydantic v1
    Ident = constr(strip_whitespace=True, min_length=1, regex=_IDENT_REGEX)

# Aliases suportados para tipos simples
SUPPORTED_DTYPES: Dict[str, Any] = {
    "string": StringType,
    "int": IntegerType, "integer": IntegerType,
    "bigint": LongType, "long": LongType,
    "double": DoubleType, "float64": DoubleType,
    "boolean": BooleanType, "bool": BooleanType,
    "date": DateType,
    "timestamp": TimestampType, "timestamptz": TimestampType,
}


class ColumnSpec(BaseModel):
    """
    Coluna declarada no contrato.

    Parâmetros
    - name: nome da coluna (ident).
    - dtype: tipo lógico/DDL (ex.: "string", "decimal(10,2)", "array<string>").
    - comment: comentário opcional.

    Normalizações
    - dtype aliases: integer→int, long→bigint, bool→boolean, float64→double.
    """
    name: Ident
    dtype: constr(strip_whitespace=True, min_length=1) = "string"
    comment: Optional[str] = None

    @validator("dtype")
    def normalize_dtype(cls, v: str) -> str:
        v = v.strip().lower()
        aliases = {"integer": "int", "long": "bigint", "bool": "boolean", "float64": "double"}
        return aliases.get(v, v)


class SourceSpec(BaseModel):
    """
    Origem dos arquivos para Auto Loader.

    Parâmetros
    - format: "csv" | "json" | "txt" (txt = csv com delimiter).
    - options: opções do leitor por formato.

    Exceptions
    - ValueError para opções inválidas/ausentes.
    """
    format: str
    options: Dict[str, Any] = Field(default_factory=dict)

    @validator("format")
    def valid_format(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in {"csv", "json", "txt"}:
            raise ValueError("source.format deve ser 'csv', 'json' ou 'txt'")
        return v

    @validator("options")
    def validate_options(cls, opts: Dict[str, Any], values) -> Dict[str, Any]:
        fmt = values.get("format")
        if fmt == "csv":
            if "header" in opts and not isinstance(opts["header"], bool):
                raise ValueError("options.header (csv) deve ser booleano")
            if "delimiter" in opts and not isinstance(opts["delimiter"], str):
                raise ValueError("options.delimiter (csv) deve ser string")
            if "nullValue" in opts and not isinstance(opts["nullValue"], str):
                raise ValueError("options.nullValue (csv) deve ser string")
        elif fmt == "json":
            if "multiline" in opts and not isinstance(opts["multiline"], bool):
                raise ValueError("options.multiline (json) deve ser booleano")
        elif fmt == "txt":
            if "delimiter" not in opts or not isinstance(opts["delimiter"], str):
                raise ValueError("options.delimiter (txt) é obrigatório e deve ser string")
        return opts


class TableContract(BaseModel):
    """
    Contrato de uma tabela Bronze (UC).

    Parâmetros
    - version: versão do contrato (default "1.0").
    - catalog/schema/table: destino no Unity Catalog.
    - columns: colunas e tipos.
    - partitions: colunas de particionamento (sempre adicionamos 'ingestion_date').
    - source: especificação da origem (Auto Loader).

    Propriedades
    - fqn / target_table_fqn: catalog.schema.table.

    Exceptions
    - ValueError para colunas vazias/duplicadas, partições inválidas.
    """
    version: Optional[str] = "1.0"
    catalog: Ident = "bronze"
    schema: Ident
    table: Ident
    columns: List[ColumnSpec]
    partitions: List[Ident] = Field(default_factory=list)
    source: SourceSpec

    @property
    def fqn(self) -> str:
        """Nome totalmente qualificado (catalog.schema.table)."""
        return f"{self.catalog}.{self.schema}.{self.table}"

    # Alias legível
    @property
    def target_table_fqn(self) -> str:
        return self.fqn

    @validator("columns")
    def unique_and_not_empty_columns(cls, cols: List[ColumnSpec]) -> List[ColumnSpec]:
        if not cols:
            raise ValueError("é obrigatório informar ao menos uma coluna em 'columns'")
        names = [c.name for c in cols]
        dups = {n for n in names if names.count(n) > 1}
        if dups:
            raise ValueError(f"colunas duplicadas: {sorted(dups)}")
        return cols

    @validator("partitions")
    def partitions_subset(cls, parts: List[str], values: Dict[str, Any]) -> List[str]:
        # todas as partições devem existir em columns
        cols_by_name = {c.name: c for c in values.get("columns", [])}
        missing = [p for p in parts if p not in cols_by_name]
        if missing:
            raise ValueError(f"partições não existem nas colunas: {missing}")
        # proíbe tipos complexos em partição
        complex_in_partition: List[str] = []
        for p in parts:
            t = cols_by_name[p].dtype.strip().lower()
            if any(t.startswith(prefix) for prefix in ("array<", "map<", "struct<")):
                complex_in_partition.append(p)
        if complex_in_partition:
            raise ValueError(f"partições não podem ser complexas: {complex_in_partition}")
        return parts

    @property
    def effective_partitions(self) -> List[str]:
        """Partições únicas + 'ingestion_date' garantida ao final."""
        unique: List[str] = []
        for p in self.partitions:
            if p not in unique:
                unique.append(p)
        if "ingestion_date" not in unique:
            unique.append("ingestion_date")
        return unique


# ------------ helpers de dtype ------------

def _parse_dtype(dtype_str: str) -> DataType:
    """
    Converte string de tipo do contrato em DataType Spark.

    Aceita
    - Aliases simples (SUPPORTED_DTYPES).
    - decimal(p,s).
    - DDL Spark p/ complexos (array/map/struct) via parser interno.

    Retorna
    - DataType correspondente; fallback: StringType.

    Observações
    - Usa _parse_ddl_type se disponível; falha silenciosa cai no fallback.
    """
    s = dtype_str.strip().lower()

    # simples
    if s in SUPPORTED_DTYPES:
        return SUPPORTED_DTYPES[s]()  # instancia a classe

    # decimal(p,s)
    if s.startswith("decimal"):
        m = re.match(pattern=r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", string=s)
        if m:
            return DecimalType(precision=int(m.group(1)), scale=int(m.group(2)))

    # complexos via DDL
    if _parse_ddl_type is not None:
        try:
            return _parse_ddl_type(dtype_str)  # assinatura interna (posicional)
        except Exception:
            pass  # fallback

    # fallback
    return StringType()


# ------------ manager ------------

class DataContractManager:
    """
    Orquestra o uso do contrato (Pydantic) e expõe utilitários.

    Parâmetros
    - contract: dict | JSON string | TableContract.

    Propriedades
    - fqn / target_table_fqn, catalog, schema, table, partitions, effective_partitions, column_names.

    Métodos
    - column_comments(): mapeia comentários (inclui ingestion_ts/ingestion_date).
    - spark_schema_typed(): StructType com tipos (inclui complexos).
    - reader_kind(): formato real p/ Auto Loader ("csv" se txt).
    - reader_options(): options mescladas (defaults + contrato).
    - as_ingestion_payload(): dict pronto para fábrica de ingestors.

    Exceptions
    - ValueError para entradas inválidas.
    """

    def __init__(self, contract: Union[dict, str, TableContract]):
        if isinstance(contract, TableContract):
            self._model = contract
        elif isinstance(contract, dict):
            self._model = TableContract(**contract)
        elif isinstance(contract, str):
            self._model = TableContract(**json.loads(s=contract))
        else:
            raise ValueError("Use dict, JSON string ou TableContract.")

    # --- Acessores
    @property
    def fqn(self) -> str:
        return self._model.fqn

    @property
    def target_table_fqn(self) -> str:
        return self._model.target_table_fqn

    @property
    def catalog(self) -> str:
        return self._model.catalog

    @property
    def schema(self) -> str:
        return self._model.schema

    @property
    def table(self) -> str:
        return self._model.table

    @property
    def partitions(self) -> List[str]:
        return list(self._model.partitions)

    @property
    def effective_partitions(self) -> List[str]:
        return self._model.effective_partitions

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self._model.columns]

    def column_comments(self) -> Dict[str, str]:
        """
        Retorna comentários por coluna (inclui padrões de auditoria).

        Retorna
        - dict: { coluna -> comentário }
        """
        out: Dict[str, str] = {}
        for c in self._model.columns:
            if c.comment and c.comment.strip():
                out[c.name] = c.comment.strip()
        out.setdefault("ingestion_ts", "Ingestion timestamp (UTC)")
        out.setdefault("ingestion_date", "Ingestion date (UTC)")
        return out

    def _default_reader_options(self, fmt: str) -> Dict[str, Any]:
        """
        Defaults por formato para o Auto Loader.

        Retorna
        - dict de opções (ex.: header/delimiter/nullValue para csv).
        """
        if fmt == "csv":
            return {"header": True, "delimiter": ",", "nullValue": ""}
        if fmt == "json":
            return {"multiline": False}
        if fmt == "txt":
            return {"header": False}  # txt -> csv + delimiter obrigatório
        return {}

    def spark_schema_typed(self) -> StructType:
        """
        Constrói o StructType a partir do contrato (suporta tipos complexos).

        Retorna
        - pyspark.sql.types.StructType
        """
        fields: List[StructField] = []
        for c in self._model.columns:
            dt = _parse_dtype(dtype_str=c.dtype)
            fields.append(StructField(name=c.name, dataType=dt, nullable=True))
        return StructType(fields=fields)

    def reader_kind(self) -> str:
        """
        Formato efetivo para Auto Loader.

        Retorna
        - "csv" | "json" (txt mapeia para "csv").
        """
        fmt = self._model.source.format
        return "csv" if fmt == "txt" else fmt

    def reader_options(self) -> Dict[str, Any]:
        """
        Mescla defaults + opções do contrato e valida exigências por formato.

        Retorna
        - dict de opções para o leitor.

        Exceptions
        - ValueError quando 'txt' sem 'delimiter'.
        """
        fmt = self._model.source.format
        merged = {**self._default_reader_options(fmt=fmt), **self._model.source.options}
        if fmt == "txt" and "delimiter" not in self._model.source.options:
            raise ValueError("Para 'txt', options.delimiter é obrigatório.")
        return merged

    def as_ingestion_payload(self) -> Dict[str, Any]:
        """
        Gera payload para a fábrica de ingestors.

        Retorna
        - dict com: fqn, schema_struct, format, reader_options, partitions, column_comments.
        """
        return {
            "fqn": self.fqn,
            "schema_struct": self.spark_schema_typed(),
            "format": self.reader_kind(),
            "reader_options": self.reader_options(),
            "partitions": self.effective_partitions,
            "column_comments": self.column_comments(),
        }
