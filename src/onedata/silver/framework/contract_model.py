"""
Modelos Pydantic do contrato da camada Silver.

Define:
- TargetWriteCfg / TargetCfg: destino e política de escrita.
- DQX (DQInnerCheck, DQCheck, DQXCfg): checks de qualidade no formato do DQX.
- Step / QuarantineCfg / CustomDecl / CustomsCfg: ETL, quarentena e customs.
- SilverYaml: contrato completo, com normalizações e helpers.

Notas
- As normalizações em DQCheck aceitam formas alternativas (ex.: 'unique' → 'is_unique').
- O validador estrutural converte formatos achatados para o nativo do DQX.
"""

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


# ---------------- Target ----------------

class TargetWriteCfg(BaseModel):
    """
    Configuração de escrita da tabela Silver.

    Atributos
    - mode: "merge" | "append" | "overwrite" (padrão: "merge").
    - merge_keys: chaves do MERGE (quando mode="merge").
    - partition_by: colunas de particionamento físico.
    - zorder_by: colunas para OPTIMIZE ZORDER BY (quando aplicável).
    """
    mode: Literal["merge", "append", "overwrite"] = "merge"
    merge_keys: List[str] = Field(default_factory=list)
    partition_by: List[str] = Field(default_factory=list)
    zorder_by: List[str] = Field(default_factory=list)


class TargetCfg(BaseModel):
    """
    Destino da tabela Silver (Unity Catalog) e política de escrita.

    Atributos
    - catalog: catálogo UC de destino.
    - schema_name: nome do schema (campo 'schema' no YAML; usamos alias para evitar conflito com BaseModel.schema()).
    - table: nome da tabela.
    - write: TargetWriteCfg com parâmetros de gravação.
    """
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
    """
    Nó interno de um check DQX.

    Atributos
    - function: nome da função do DQX (ex.: "is_not_null", "is_unique", "is_in_range").
    - arguments: dicionário de argumentos da função.
    """
    function: str
    arguments: Dict[str, Any] = Field(default_factory=dict)

    # se vier None, vira {}
    @field_validator("arguments", mode="before")
    @classmethod
    def _args_none_to_empty(cls, v):
        """
        Normaliza 'arguments': quando None, converte para {} para simplificar o consumo.
        """
        return {} if v is None else v


class DQCheck(BaseModel):
    """
    Check DQX com nome, criticidade e especificação da função.

    Atributos
    - name: identificador do check.
    - criticality: "error" | "warning" (padrão: "error").
    - check: DQInnerCheck com function e arguments.

    Regras
    - extra="forbid": rejeita campos não declarados.
    - Normalizações:
        * aliases de function (ex.: "unique"→"is_unique", "not_null"→"is_not_null").
        * retrocompat de argumentos (ex.: col_name→column).
        * ajustes específicos (ex.: is_in_range converte float integral para int, senão str).
        * is_unique aceita column|columns e achata listas aninhadas; normaliza nulls_distinct str→bool.
    """
    name: str
    criticality: Literal["error", "warning"] = "error"
    check: DQInnerCheck

    model_config = ConfigDict(extra="forbid")

    # -------- helpers de normalização --------
    @staticmethod
    def _alias_function(fn: str) -> str:
        """
        Aplica aliases amistosos para nomes de função.
        """
        return {
            "unique": "is_unique",
            "not_null": "is_not_null",
        }.get(fn, fn)

    @staticmethod
    def _normalize_args(fn: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliza argumentos comuns por função, mantendo retrocompatibilidade.

        Regras principais
        - col_name -> column
        - is_in_range: min/max como int quando integral; senão string.
        - is_unique:
            * move column -> columns (lista)
            * achata listas aninhadas
            * nulls_distinct: "true"/"1"/"yes" -> True
        """
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
        Aceita dois formatos e converte para o nativo antes de validar:

          A) Nativo:
             { "name": ..., "criticality": ..., "check": { "function": ..., "arguments": {...} } }

          B) Achatado:
             { "name": ..., "criticality": ..., "function": ..., "arguments": {...} }

        Também aplica normalizações leves de argumentos.
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
    """
    Bloco de configuração do DQX no contrato.

    Atributos
    - criticality_default: default global de criticidade ("error"|"warning").
    - checks: lista de DQCheck padrão.
    - custom: lista de DQCheck adicionais.
    """
    criticality_default: Literal["error", "warning"] = "error"
    checks: List[DQCheck] = Field(default_factory=list)
    custom: List[DQCheck] = Field(default_factory=list)


# ---------------- ETL / Quarentena / Customs ----------------

class Step(BaseModel):
    """
    Passo/etapa de ETL ou custom.

    Atributos
    - method: nome do método (core/custom) a invocar.
    - args: dicionário de argumentos para o método.
    - stage: "standard" | "quarantine" (usado apenas em customs.use_in).
    """
    method: str
    args: Dict[str, Any] = Field(default_factory=dict)
    stage: Optional[Literal["standard", "quarantine"]] = None  # usado em customs.use_in


class QuarantineCfg(BaseModel):
    """
    Configuração da quarentena.

    Atributos
    - remediate: lista de Steps a aplicar nos dados quarentenados.
    - sink: destino opcional para persistência da quarentena (ex.: {"table": "silver.quarantine.xyz"}).
    """
    remediate: List[Step] = Field(default_factory=list)
    sink: Optional[Dict[str, str]] = None  # {"table": "monitoring.quarantine.sales_bronze_teste"}


class CustomDecl(BaseModel):
    """
    Declaração de um método custom disponível no registro.

    Atributos
    - name: identificador do custom (usado em customs.use_in.method).
    - module: módulo Python onde a função está definida.
    - method: nome da função dentro do módulo.
    - args_schema: schema de validação dos argumentos (livre, interpretado pela engine).
    """
    name: str
    module: str
    method: str
    args_schema: Dict[str, Any] = Field(default_factory=dict)


class CustomsCfg(BaseModel):
    """
    Configuração dos customs no contrato.

    Atributos
    - allow: habilita/desabilita execução de customs.
    - registry: lista de CustomDecl (catálogo de métodos disponíveis).
    - use_in: Steps que aplicam customs por estágio (ex.: stage="standard").
    """
    allow: bool = False
    registry: List[CustomDecl] = Field(default_factory=list)
    use_in: List[Step] = Field(default_factory=list)  # steps com {stage, method, args}


# ---------------- Contrato Silver ----------------

class SilverYaml(BaseModel):
    """
    Contrato completo da camada Silver.

    Atributos
    - version: versão do contrato (somente 1.x suportado).
    - source: {"bronze_table": "bronze.schema.table", ...}.
    - target: TargetCfg com destino e write.
    - dqx: DQXCfg com checks de qualidade.
    - etl: {"standard": [Step, ...]} para core ETL.
    - quarantine: QuarantineCfg com remediação/persistência.
    - customs: CustomsCfg (registro e uso por estágio).

    Regras
    - extra="forbid": rejeita campos não declarados no YAML.
    """
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
        """
        Garante que a versão principal do contrato seja 1.x.
        """
        if v.split(".")[0] != "1":
            raise ValueError("Somente versão 1.x suportada por enquanto.")
        return v

    @property
    def target_fqn(self) -> str:
        """
        Retorna o FQN de destino (catalog.schema.table) para facilitar o consumo.
        """
        return f"{self.target.catalog}.{self.target.schema_name}.{self.target.table}"
