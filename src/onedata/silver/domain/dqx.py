"""
Modelos Pydantic para validação de dados com o Databricks Labs DQX.

Suporta:
- Formato nativo do DQX (check: {function, arguments})
- Formato achatado retrocompatível (function/arguments na raiz)
- Aliases de funções (ex.: "unique"→"is_unique")
- Normalizações de argumentos (ex.: col_name→column; lista de columns)
"""

from typing import Any, Dict, List, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


class DQInnerCheck(BaseModel):
    """
    Nó interno de um check DQX.

    Atributos
    ----------
    function : str
        Nome da função do DQX (ex.: "is_not_null", "is_unique", "is_in_range").
    arguments : dict
        Dicionário de argumentos da função (padrão: {}).
    """
    function: str
    arguments: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("arguments", mode="before")
    @classmethod
    def _args_none_to_empty(cls, argument_value):
        """
        Normaliza 'arguments': quando None, converte para {} para simplificar o consumo.
        """
        return {} if argument_value is None else argument_value


class DQCheck(BaseModel):
    """
    Check DQX com nome, criticidade e especificação da função.

    Regras e normalizações
    ----------------------
    • extra="forbid": rejeita campos não declarados.
    • aliases de function (ex.: "unique"→"is_unique", "not_null"→"is_not_null").
    • retrocompat de argumentos (ex.: col_name→column).
    • is_in_range: min/max como int quando integral; senão string.
    • is_unique: aceita 'column' ou 'columns', achata listas e normaliza 'nulls_distinct'.

    Atributos
    ----------
    name : str
        Identificador do check.
    criticality : {"error","warning"}
        Severidade do check (padrão: "error").
    check : DQInnerCheck
        Especificação interna (function + arguments).
    """
    name: str
    criticality: Literal["error", "warning"] = "error"
    check: DQInnerCheck

    model_config = ConfigDict(extra="forbid")

    # -------- helpers de normalização --------
    @staticmethod
    def _alias_function(fn: str) -> str:
        """Aplica aliases amistosos para nomes de função."""
        return {
            "unique": "is_unique",
            "not_null": "is_not_null",
        }.get(fn, fn)

    @staticmethod
    def _normalize_args(fn: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliza argumentos comuns por função, mantendo retrocompatibilidade.

        Regras principais
        -----------------
        • col_name -> column
        • is_in_range: min/max como int quando integral; senão string.
        • is_unique:
            – move 'column' -> 'columns' (lista)
            – achata listas aninhadas
            – 'nulls_distinct': "true"/"1"/"yes" -> True
        """
        args = dict(args or {})

        # col_name -> column
        if "col_name" in args and "column" not in args:
            args["column"] = args.pop("col_name")

        # is_in_range: int se integral; senão string (evita float binário impreciso)
        if fn == "is_in_range":
            for key in ("min_limit", "max_limit"):
                if key in args:
                    value = args[key]
                    if isinstance(value, float):
                        args[key] = int(value) if float(value).is_integer() else str(value)

        # is_unique: unifica column/columns e achata
        if fn == "is_unique":
            if "column" in args and "columns" not in args:
                col_val = args.pop("column")
                args["columns"] = [col_val] if not isinstance(col_val, list) else col_val
            if "columns" in args:
                cols = args["columns"]
                if isinstance(cols, str):
                    cols = [cols]
                flat = []
                for column in cols:
                    flat.extend(column if isinstance(column, list) else [column])
                args["columns"] = flat
            if "nulls_distinct" in args and isinstance(args["nulls_distinct"], str):
                args["nulls_distinct"] = args["nulls_distinct"].lower() in ("1", "true", "yes")

        return args

    # -------- normalização estrutural ANTES da validação --------
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

        name = data.get("name")
        criticality = data.get("criticality", "error")
        check = data.get("check") or {}
        fn = check.get("function", data.get("function"))
        args = check.get("arguments", data.get("arguments", {}))

        if fn is None:
            raise ValueError(f"DQCheck '{name}' sem 'function' definido (check.function ou function)")

        fn = cls._alias_function(str(fn))
        args = cls._normalize_args(fn, args)

        return {
            "name": name,
            "criticality": criticality,
            "check": {"function": fn, "arguments": args or {}},
        }


class DQXCfg(BaseModel):
    """
    Bloco de configuração do DQX no contrato.

    Atributos
    ----------
    criticality_default : {"error","warning"}
        Default global de criticidade (padrão: "error").
    checks : list[DQCheck]
        Checks padrão.
    custom : list[DQCheck]
        Checks adicionais (além dos padrão).
    """
    criticality_default: Literal["error", "warning"] = "error"
    checks: List[DQCheck] = Field(default_factory=list)
    custom: List[DQCheck] = Field(default_factory=list)
