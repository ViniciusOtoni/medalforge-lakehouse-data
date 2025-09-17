from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

# ---------------- Target ----------------

class TargetWriteCfg(BaseModel):
    mode: Literal["merge", "append", "overwrite"] = "merge"
    merge_keys: List[str] = Field(default_factory=list)
    partition_by: List[str] = Field(default_factory=list)
    zorder_by: List[str] = Field(default_factory=list)

class TargetCfg(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  
    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    write: TargetWriteCfg

# ---------------- DQX ----------------
# DQX nativo pede: { name, criticality, check: { function, arguments } }

AllowedFn = Literal["is_not_null", "is_unique", "is_in_range", "sql_expression"]

class DQInnerCheck(BaseModel):
    function: AllowedFn
    arguments: Dict[str, Any] = Field(default_factory=dict)

class DQCheck(BaseModel):
    # Aceita ambos formatos:
    #  A) Nativo:   { name, criticality, check: { function, arguments } }
    #  B) Achatado: { name, criticality, function, arguments }
    name: str
    criticality: Literal["error", "warning"] = "error"

    # Nativo
    check: Optional[DQInnerCheck] = None

    # Achatado (retrocompatibilidade)
    function: Optional[str] = None
    arguments: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    @staticmethod
    def _alias_function(fn: str) -> str:
        # Compatibilidade: 'unique' → 'is_unique'
        return {"unique": "is_unique"}.get(fn, fn)

    @staticmethod
    def _normalize_args(fn: str, args: Dict[str, Any]) -> Dict[str, Any]:
        args = dict(args or {})

        # col_name → column
        if "col_name" in args and "column" not in args:
            args["column"] = args.pop("col_name")

        # is_in_range: tipos aceitos p/ min/max
        if fn == "is_in_range":
            for k in ("min_limit", "max_limit"):
                if k in args:
                    v = args[k]
                    if isinstance(v, float):
                        args[k] = int(v) if float(v).is_integer() else str(v)

        # is_unique: requer 'columns' (lista)
        if fn == "is_unique":
            # se vier 'column', converte para 'columns'
            if "column" in args and "columns" not in args:
                args["columns"] = [args.pop("column")]
            # se vier como string, vira lista
            if "columns" in args and isinstance(args["columns"], str):
                args["columns"] = [args["columns"]]
            # default p/ nulls_distinct (opcional)
            if "nulls_distinct" in args:
                args["nulls_distinct"] = bool(args["nulls_distinct"])

        return args

    @model_validator(mode="after")
    def _coalesce_to_native(self):
        # Se veio no formato achatado, criar self.check
        if self.check is None:
            if not self.function:
                raise ValueError("DQCheck requer 'check' ou 'function'")
            fn = self._alias_function(self.function)
            fn_norm = fn  # validação do literal acontecerá ao construir DQInnerCheck
            args_norm = self._normalize_args(fn_norm, self.arguments)
            self.check = DQInnerCheck(function=fn_norm, arguments=args_norm)

        else:
            # Se já veio no formato nativo, ainda normaliza alias e argumentos
            fn = self._alias_function(self.check.function)
            args_norm = self._normalize_args(fn, self.check.arguments)
            self.check = DQInnerCheck(function=fn, arguments=args_norm)

        # Zera campos achatados para evitar dump duplicado
        self.function = None
        self.arguments = {}
        return self

class DQXCfg(BaseModel):
    criticality_default: Literal["error", "warning"] = "error"
    checks: List[DQCheck] = Field(default_factory=list)
    custom: List[DQCheck] = Field(default_factory=list)

# ---------------- ETL / Quarentena / Customs ----------------

class Step(BaseModel):
    method: str
    args: Dict[str, Any] = Field(default_factory=dict)
    stage: Optional[Literal["standard", "quarantine"]] = None  # usado em customs.use_in

class QuarantineCfg(BaseModel):
    remediate: List[Step] = Field(default_factory=list)
    sink: Optional[Dict[str, str]] = None  # {"table": "monitoring.quarantine.sales_bronze_teste"}

class CustomDecl(BaseModel):
    name: str
    module: str
    method: str
    args_schema: Dict[str, Any] = Field(default_factory=dict)

class CustomsCfg(BaseModel):
    allow: bool = False
    registry: List[CustomDecl] = Field(default_factory=list)
    use_in: List[Step] = Field(default_factory=list)  # steps com {stage, method, args}

# ---------------- Contrato Silver ----------------

class SilverYaml(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    source: Dict[str, str]              # {"bronze_table": "bronze.schema.table", ...}
    target: TargetCfg
    dqx: DQXCfg
    etl: Dict[str, List[Step]]          # {"standard": [Step, ...]}
    quarantine: QuarantineCfg
    customs: CustomsCfg

    @field_validator("version")
    @classmethod
    def v1_only(cls, v: str) -> str:
        if v.split(".")[0] != "1":
            raise ValueError("Somente versão 1.x suportada por enquanto.")
        return v

    @property
    def target_fqn(self) -> str:
        return f"{self.target.catalog}.{self.target.schema_name}.{self.target.table}"
