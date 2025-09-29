from __future__ import annotations
import re
from typing import Any, Dict

_BOOL_TRUE = {"1","true","yes","y","t"}
_BOOL_FALSE = {"0","false","no","n","f"}

def coerce_boolean(v: Any) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _BOOL_TRUE: return True
        if s in _BOOL_FALSE: return False
    raise TypeError("valor booleano inválido")

def coerce_number(v: Any) -> Any:
    if isinstance(v, (int, float)): return v
    if isinstance(v, str):
        s = v.strip()
        if re.fullmatch(r"[+-]?\d+", s): return int(s)
        if re.fullmatch(r"[+-]?\d+(\.\d+)?", s): return float(s)
    raise TypeError("valor numérico inválido")

def coerce_integer(v: Any) -> int:
    if isinstance(v, int): return v
    if isinstance(v, float) and float(v).is_integer(): return int(v)
    if isinstance(v, str) and re.fullmatch(r"[+-]?\d+", v.strip()):
        return int(v.strip())
    raise TypeError("valor inteiro inválido")

def _validate_numeric_range(x: float, rule: Dict[str, Any], key: str) -> None:
    if "exclusive_min" in rule and not (x > rule["exclusive_min"]): raise ValueError(f"{key}> {rule['exclusive_min']}")
    if "exclusive_max" in rule and not (x < rule["exclusive_max"]): raise ValueError(f"{key}< {rule['exclusive_max']}")
    if "min" in rule and x < rule["min"]: raise ValueError(f"{key}>= {rule['min']}")
    if "max" in rule and x > rule["max"]: raise ValueError(f"{key}<= {rule['max']}")
    if "multiple_of" in rule:
        m = rule["multiple_of"]
        if m == 0: raise ValueError("multiple_of não pode ser 0")
        if abs((x/m) - round(x/m)) > 1e-9: raise ValueError(f"{key} múltiplo de {m}")

def _validate_string(val: str, rule: Dict[str, Any], key: str) -> str:
    s = val.strip() if rule.get("trim") else val
    if not rule.get("allow_blank", True) and s == "": raise ValueError(f"{key} vazio")
    if "min_len" in rule and len(s) < int(rule["min_len"]): raise ValueError(f"{key} min_len {rule['min_len']}")
    if "max_len" in rule and len(s) > int(rule["max_len"]): raise ValueError(f"{key} max_len {rule['max_len']}")
    return s

def _validate_enum(val: Any, rule: Dict[str, Any], key: str) -> None:
    choices = rule.get("choices")
    if not choices: raise ValueError(f"{key}: enum sem 'choices'")
    if val not in choices: raise ValueError(f"{key} ∉ {choices!r}")

def _validate_regex(val: Any, rule: Dict[str, Any], key: str) -> None:
    pat = rule.get("pattern")
    if not pat: raise ValueError(f"{key}: regex sem 'pattern'")
    flags = re.IGNORECASE if rule.get("ignore_case") else 0
    rx = re.compile(pat, flags)
    sval = str(val)
    ok = (rx.search(sval) is not None) if rule.get("partial") else (rx.fullmatch(sval) is not None)
    if not ok: raise ValueError(f"{key} não casa /{pat}/")

def validate_and_normalize_args(
    args: Dict[str, Any],
    schema: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Valida e normaliza args conforme schema simples.
    Tipos: number, integer, boolean, string, enum, regex. Suporta defaults e ranges.
    """
    args = dict(args or {})
    out: Dict[str, Any] = {}

    for key, rule in (schema or {}).items():
        required = rule.get("required", True)
        present = key in args

        if not present:
            if not required and "default" in rule:
                out[key] = rule["default"]
            elif required:
                raise ValueError(f"falta argumento obrigatório: '{key}'")
            continue

        val = args[key]
        rtype = rule.get("type")

        if rtype == "number":
            if rule.get("coerce"): val = coerce_number(val)
            if not isinstance(val, (int, float)): raise TypeError(f"{key} numérico")
            _validate_numeric_range(float(val), rule, key)

        elif rtype == "integer":
            if rule.get("coerce"): val = coerce_integer(val)
            if not isinstance(val, int): raise TypeError(f"{key} inteiro")
            _validate_numeric_range(float(val), rule, key)

        elif rtype == "boolean":
            if rule.get("coerce"): val = coerce_boolean(val)
            if not isinstance(val, bool): raise TypeError(f"{key} booleano")

        elif rtype == "string":
            if not isinstance(val, str):
                if rule.get("coerce"): val = str(val)
                else: raise TypeError(f"{key} string")
            val = _validate_string(val, rule, key)

        elif rtype == "enum":
            _validate_enum(val, rule, key)

        elif rtype == "regex":
            _validate_regex(val, rule, key)

        elif rtype is None:
            pass
        else:
            raise ValueError(f"{key}: tipo desconhecido {rtype!r}")

        out[key] = val

    # Rejeita qualquer argumento fora do schema
    extras = [k for k in args.keys() if k not in (schema or {})]
    if extras:
        raise ValueError(f"args não permitidos: {extras}")

    return out
