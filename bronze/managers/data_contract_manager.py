# managers/data_contract_manager.py
from __future__ import annotations
from typing import List, Optional, Dict, Union, Any
from pydantic import BaseModel, Field, validator, constr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType
)
import json, re

_IDENT_REGEX = r"^[A-Za-z_][A-Za-z0-9_]*$"
try:
    Ident = constr(strip_whitespace=True, min_length=1, pattern=_IDENT_REGEX)
except TypeError:
    Ident = constr(strip_whitespace=True, min_length=1, regex=_IDENT_REGEX)

SUPPORTED_DTYPES = {
    "string": StringType,
    "int": IntegerType, "integer": IntegerType,
    "bigint": LongType, "long": LongType,
    "double": DoubleType, "float64": DoubleType,
    "boolean": BooleanType, "bool": BooleanType,
    "date": DateType,
    "timestamp": TimestampType, "timestamptz": TimestampType,
}

class ColumnSpec(BaseModel):
    name: Ident
    dtype: constr(strip_whitespace=True, min_length=1) = "string"
    comment: Optional[str] = None

    @validator("dtype")
    def normalize_dtype(cls, v: str) -> str:
        v = v.strip().lower()
        aliases = {"integer": "int", "long": "bigint", "bool": "boolean", "float64": "double"}
        return aliases.get(v, v)

class SourceSpec(BaseModel):
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
    version: Optional[str] = "1.0"
    catalog: Ident = "bronze"
    schema: Ident
    table: Ident
    columns: List[ColumnSpec]
    partitions: List[Ident] = Field(default_factory=list)
    source: SourceSpec

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

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
        cols = {c.name for c in values.get("columns", [])}
        missing = [p for p in parts if p not in cols]
        if missing:
            raise ValueError(f"partições não existem nas colunas: {missing}")
        return parts

    @property
    def effective_partitions(self) -> List[str]:
        unique: List[str] = []
        for p in self.partitions:
            if p not in unique:
                unique.append(p)
        if "ingestion_date" not in unique:
            unique.append("ingestion_date")
        return unique

class DataContractManager:
    def __init__(self, contract: Union[dict, str, TableContract]):
        if isinstance(contract, TableContract):
            self._model = contract
        elif isinstance(contract, dict):
            self._model = TableContract(**contract)
        elif isinstance(contract, str):
            self._model = TableContract(**json.loads(contract))
        else:
            raise ValueError("Use dict, JSON string ou TableContract.")

    # --- Acessores
    @property
    def fqn(self) -> str: return self._model.fqn
    @property
    def catalog(self) -> str: return self._model.catalog
    @property
    def schema(self) -> str: return self._model.schema
    @property
    def table(self) -> str: return self._model.table
    @property
    def partitions(self) -> List[str]: return list(self._model.partitions)
    @property
    def effective_partitions(self) -> List[str]: return self._model.effective_partitions
    @property
    def column_names(self) -> List[str]: return [c.name for c in self._model.columns]

    # 🔎 NOVO: comentários de coluna a partir do contrato
    def column_comments(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for c in self._model.columns:
            if c.comment and c.comment.strip():
                out[c.name] = c.comment.strip()
        
        out.setdefault("ingestion_ts", "Ingestion timestamp (UTC)")
        out.setdefault("ingestion_date", "Ingestion date (UTC)")
        return out

    def _default_reader_options(self, fmt: str) -> Dict[str, Any]:
        if fmt == "csv": return {"header": True, "delimiter": ",", "nullValue": ""}
        if fmt == "json": return {"multiline": False}
        if fmt == "txt":  return {"header": False}
        return {}

    def spark_schema_typed(self) -> StructType:
        fields = []
        for c in self._model.columns:
            dt = c.dtype
            if dt.startswith("decimal"):
                m = re.match(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", dt)
                if not m:
                    raise ValueError(f"dtype inválido em {c.name}: {dt}")
                p, s = int(m.group(1)), int(m.group(2))
                fields.append(StructField(c.name, DecimalType(p, s), True))
                continue
            typ = SUPPORTED_DTYPES.get(dt, StringType)
            fields.append(StructField(c.name, typ(), True))
        return StructType(fields)

    def reader_kind(self) -> str:
        fmt = self._model.source.format
        return "csv" if fmt == "txt" else fmt

    def reader_options(self) -> Dict[str, Any]:
        fmt = self._model.source.format
        merged = {**self._default_reader_options(fmt), **self._model.source.options}
        if fmt == "txt" and "delimiter" not in self._model.source.options:
            raise ValueError("Para 'txt', options.delimiter é obrigatório.")
        return merged

    def as_ingestion_payload(self) -> Dict[str, Any]:
        return {
            "fqn": self.fqn,
            "schema_struct": self.spark_schema_typed(),
            "format": self.reader_kind(),
            "reader_options": self.reader_options(),
            "partitions": self.effective_partitions,
            "column_comments": self.column_comments(),  
        }
