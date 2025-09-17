from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

# ---------------- Target ----------------

class TargetWriteCfg(BaseModel):
    mode: Literal["merge", "append", "overwrite"] = "merge"
    merge_keys: List[str] = Field(default_factory=list)
    partition_by: List[str] = Field(default_factory=list)
    zorder_by: List[str] = Field(default_factory=list)

class TargetCfg(BaseModel):
    # evita warning por campo 'schema' colidir com metodo BaseModel.schema()
    model_config = ConfigDict(populate_by_name=True)
    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    write: TargetWriteCfg

# ---------------- DQX ----------------
# Formato nativo DQX: { name, criticality, check: { function, arguments } }
# Permitimos QUALQUER função (str) para não engessar os checks.

class DQInnerCheck(BaseModel):
    function: str
    arguments: Dict[str, Any] = Field(default_factory=dict)

    # se vier None, vira {}
    @field_validator("arguments", mode="before")
    @classmethod
    def _args_none_to_empty(cls, v):
        return {} if v is None else v

class DQCheck(BaseModel):
    name: str
    criticality: Literal["error", "warning"] = "error"
    check: DQInnerCheck

    model_config = ConfigDict(extra="forbid")

    # -------- helpers de normalização (genéricos, sem 'xunbar' regra específica) --------
    @staticmethod
    def _alias_function(fn: str) -> str:
        # aliases amistosos que apareceram no campo:
        return {
            "unique": "is_unique",
            "not_null": "is_not_null",
        }.get(fn, fn)

    @staticmethod
    def _normalize_args(fn: str, args: Dict[str, Any]) -> Dict[str, Any]:
        args = dict(args or {})

        # retrocompat: col_name -> column
        if "col_name" in args and "column" not in args:
            args["column"] = args.pop("col_name")

        # is_in_range: DQX não curte float puro p/ limites; int se integral, senão str
        if fn == "is_in_range":
            for k in ("min_limit", "max_limit"):
                if k in args:
                    v = args[k]
                    if isinstance(v, float):
                        args[k] = int(v) if float(v).is_integer() else str(v)

        # is_unique: aceitar column/columns (str|list) e achatar listas aninhadas
        if fn == "is_unique":
            # mover column -> columns, preservando lista se já for lista
            if "column" in args and "columns" not in args:
                col_val = args.pop("column")
                if isinstance(col_val, list):
                    args["columns"] = col_val
                else:
                    args["columns"] = [col_val]
            # garantir lista simples
            if "columns" in args:
                cols = args["columns"]
                if isinstance(cols, str):
                    cols = [cols]
                # flatten defensivo (evita [['id']])
                flat = []
                for c in cols:
                    if isinstance(c, list):
                        flat.extend(c)
                    else:
                        flat.append(c)
                args["columns"] = flat
            # normaliza nulls_distinct se vier como string
            if "nulls_distinct" in args and isinstance(args["nulls_distinct"], str):
                args["nulls_distinct"] = args["nulls_distinct"].lower() in ("1", "true", "yes")

        return args

    # -------- normalização estrutural ANTES da validação de campos --------
    @model_validator(mode="before")
    @classmethod
    def _coerce_shape_before(cls, data: Any):
        """
        Aceita:
          A) Nativo:   { name, criticality, check: { function, arguments } }
          B) Achatado: { name, criticality, function, arguments }
        Converte tudo para (A) antes da validação.
        Também aplica normalizações leves de argumentos comuns.
        """
        if not isinstance(data, dict):
            return data

        # extrai formato atual
        name = data.get("name")
        criticality = data.get("criticality", "error")

        # coleta function/arguments de onde estiverem
        check = data.get("check") or {}
        fn = check.get("function", data.get("function"))
        args = check.get("arguments", data.get("arguments", {}))

        if fn is None:
            # mantém erro claro, mas só depois de tentar coagir
            raise ValueError(f"DQCheck '{name}' sem 'function' definido (check.function ou function)")

        # aliases + normalizações
        fn = cls._alias_function(str(fn))
        args = cls._normalize_args(fn, args)

        # reconstrói shape nativo e remove campos achatados
        data = {
            "name": name,
            "criticality": criticality,
            "check": {
                "function": fn,
                "arguments": args or {},
            },
        }
        return data

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
