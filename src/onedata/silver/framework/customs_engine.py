"""
Engine de execução de customs na Silver.

Funções principais:
- _load_custom: importa e valida a assinatura de uma função custom (df primeiro arg).
- _validate_and_normalize_args: valida/coerce argumentos conforme um schema simples.
- apply_customs_stage: aplica customs declarados para um determinado 'stage' (standard/quarantine).

Recursos:
- Coerções (number/integer/boolean/string), validação (enum/regex/range/len).
- allow_extra_args (args fora do schema são permitidos ou não).
- dev_reload (reload do módulo em cada passo, útil em notebooks).
- Logger opcional para depuração.
"""

import importlib
import inspect
import re
import sys
from typing import Dict, Any, Callable, Optional
from pyspark.sql import DataFrame

from framework.contract_model import CustomsCfg, CustomDecl, Step


# --------------------------- utils de log ---------------------------

def _log(logger: Optional[Callable[[str], None]], msg: str) -> None:
    """
    Emite mensagem via logger opcional.

    Parâmetros
    - logger: callable (ex.: print) ou None.
    - msg: texto a ser emitido.

    Retorno
    - None
    """
    if logger:
        logger(msg)


# --------------------------- carga do custom ---------------------------

def _load_custom(module: str, method: str, *, dev_reload: bool = False) -> Callable[..., DataFrame]:
    """
    Importa `module.method` e valida a assinatura.

    Regras
    - Primeiro parâmetro deve se chamar 'df'.
    - Retorno esperado em runtime: pyspark.sql.DataFrame.

    Parâmetros
    - module: nome do módulo Python.
    - method: nome da função no módulo.
    - dev_reload: se True, faz importlib.reload(module) (útil em dev/notebook).

    Retorno
    - Função carregada (callable).

    Exceptions
    - ImportError, AttributeError, TypeError: problemas de import/assinatura.
    """
    try:
        mod = importlib.import_module(name=module)
        if dev_reload and module in sys.modules:
            mod = importlib.reload(mod)
    except Exception as e:
        raise ImportError(f"Falha ao importar módulo '{module}': {e}") from e

    try:
        fn = getattr(mod, method)
    except AttributeError as e:
        raise AttributeError(f"Método '{method}' não encontrado no módulo '{module}'.") from e

    if not callable(fn):
        raise TypeError(f"'{module}.{method}' não é chamável.")

    sig = inspect.signature(obj=fn)
    params = list(sig.parameters.values())
    if not params or params[0].name != "df":
        raise TypeError(
            f"Custom '{module}.{method}' deve ter assinatura: fn(df: DataFrame, **kwargs) -> DataFrame"
        )
    return fn


# --------------------------- validação/normalização de args ---------------------------

_BOOL_TRUE = {"1", "true", "yes", "y", "t"}
_BOOL_FALSE = {"0", "false", "no", "n", "f"}


def _coerce_boolean(val: Any) -> bool:
    """
    Converte valores diversos em boolean.

    Aceita
    - bool, int/float (0 = False, !=0 = True), strings ('true'/'false', etc.)

    Exceptions
    - TypeError: quando não é possível converter.
    """
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        s = val.strip().lower()
        if s in _BOOL_TRUE:
            return True
        if s in _BOOL_FALSE:
            return False
    raise TypeError("valor booleano inválido")


def _coerce_number(val: Any) -> Any:
    """
    Coerce para número (int ou float).

    Regras
    - Strings inteiras -> int; decimais -> float.

    Exceptions
    - TypeError: quando não é número válido.
    """
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if re.fullmatch(pattern=r"[+-]?\d+", string=s):
            return int(s)
        if re.fullmatch(pattern=r"[+-]?\d+(\.\d+)?", string=s):
            return float(s)
    raise TypeError("valor numérico inválido")


def _coerce_integer(val: Any) -> int:
    """
    Coerce para inteiro (aceita float integral e string dígitos).

    Exceptions
    - TypeError: quando não é inteiro válido.
    """
    if isinstance(val, int):
        return val
    if isinstance(val, float) and float(val).is_integer():
        return int(val)
    if isinstance(val, str):
        s = val.strip()
        if re.fullmatch(pattern=r"[+-]?\d+", string=s):
            return int(s)
    raise TypeError("valor inteiro inválido")


def _validate_numeric_range(x: float, rule: Dict[str, Any], *, key: str) -> None:
    """
    Valida faixas numéricas conforme o schema.

    Suporta
    - min/max, exclusive_min/exclusive_max, multiple_of.

    Exceptions
    - ValueError: quando fora dos limites.
    """
    if "exclusive_min" in rule and not (x > rule["exclusive_min"]):
        raise ValueError(f"'{key}' deve ser > {rule['exclusive_min']}")
    if "exclusive_max" in rule and not (x < rule["exclusive_max"]):
        raise ValueError(f"'{key}' deve ser < {rule['exclusive_max']}")
    if "min" in rule and x < rule["min"]:
        raise ValueError(f"'{key}' deve ser >= {rule['min']}")
    if "max" in rule and x > rule["max"]:
        raise ValueError(f"'{key}' deve ser <= {rule['max']}")
    if "multiple_of" in rule:
        m = rule["multiple_of"]
        if m == 0:
            raise ValueError(f"'{key}' multiple_of não pode ser 0")
        if abs((x / m) - round(x / m)) > 1e-9:  # tolerância p/ float
            raise ValueError(f"'{key}' deve ser múltiplo de {m}")


def _validate_string(val: str, rule: Dict[str, Any], *, key: str) -> str:
    """
    Valida string (trim, allow_blank, min_len, max_len).

    Retorna
    - string possivelmente 'trimada'.

    Exceptions
    - ValueError: violações de tamanho/blank.
    """
    s = val
    if rule.get("trim"):
        s = s.strip()
    if not rule.get("allow_blank", True) and s == "":
        raise ValueError(f"'{key}' não pode ser vazio")
    if "min_len" in rule and len(s) < int(rule["min_len"]):
        raise ValueError(f"'{key}' tamanho mínimo é {rule['min_len']}")
    if "max_len" in rule and len(s) > int(rule["max_len"]):
        raise ValueError(f"'{key}' tamanho máximo é {rule['max_len']}")
    return s


def _validate_enum(val: Any, rule: Dict[str, Any], *, key: str) -> None:
    """
    Valida pertencimento a um conjunto (choices).
    """
    choices = rule.get("choices")
    if not isinstance(choices, (list, tuple)) or not choices:
        raise ValueError(f"'{key}': schema enum sem 'choices' não-vazio")
    if val not in choices:
        raise ValueError(f"'{key}' deve ser um de {choices!r}")


def _validate_regex(val: Any, rule: Dict[str, Any], *, key: str) -> None:
    """
    Valida padrões com regex (fullmatch por padrão; search se partial=True).

    Suporta
    - ignore_case, partial.
    """
    pattern = rule.get("pattern")
    if not isinstance(pattern, str) or pattern == "":
        raise ValueError(f"'{key}': schema regex sem 'pattern'")
    flags = 0
    if rule.get("ignore_case"):
        flags |= re.IGNORECASE
    rx = re.compile(pattern=pattern, flags=flags)
    sval = str(val)
    ok = rx.search(sval) is not None if rule.get("partial") else rx.fullmatch(sval) is not None
    if not ok:
        how = "conter" if rule.get("partial") else "corresponder a"
        raise ValueError(f"'{key}' deve {how} /{pattern}/")


def _validate_and_normalize_args(
    args: Dict[str, Any],
    schema: Dict[str, Any],
    *,
    allow_extra_args: bool = True,
) -> Dict[str, Any]:
    """
    Valida e NORMALIZA argumentos com base em um schema simples.

    Regras
    - Preenche 'default' quando 'required' é False.
    - Aplica coerce (number/integer/boolean/string) quando coerce=True.
    - Valida enum/regex/range/len.
    - Controla aceitação de args extras.

    Parâmetros
    - args: argumentos declarados no contrato (customs.use_in[].args).
    - schema: definição por chave (type, min/max, enum, regex, default, required, coerce...).
    - allow_extra_args: permite args fora do schema (não validados).

    Retorno
    - dict normalizado (novos objetos, sem mutar 'args' original).

    Exceptions
    - ValueError/TypeError conforme regras de validação/coerção.
    """
    args = dict(args or {})
    normalized: Dict[str, Any] = {}

    # 1) valida/normaliza os que estão no schema
    for key, rule in (schema or {}).items():
        required = rule.get("required", True)
        present = key in args

        if not present:
            if not required and "default" in rule:
                normalized[key] = rule["default"]
                continue
            if required:
                raise ValueError(f"Argumento obrigatório ausente: '{key}'")
            continue  # não requerido e sem default

        val = args[key]
        rtype = rule.get("type")

        if rtype == "number":
            if rule.get("coerce"):
                try:
                    val = _coerce_number(val=val)
                except Exception as e:
                    raise TypeError(f"'{key}' deve ser numérico: {e}")
            if not isinstance(val, (int, float)):
                raise TypeError(f"'{key}' deve ser numérico")
            _validate_numeric_range(x=float(val), rule=rule, key=key)

        elif rtype == "integer":
            if rule.get("coerce"):
                try:
                    val = _coerce_integer(val=val)
                except Exception as e:
                    raise TypeError(f"'{key}' deve ser inteiro: {e}")
            if not isinstance(val, int):
                raise TypeError(f"'{key}' deve ser inteiro")
            _validate_numeric_range(x=float(val), rule=rule, key=key)

        elif rtype == "boolean":
            if rule.get("coerce"):
                try:
                    val = _coerce_boolean(val=val)
                except Exception as e:
                    raise TypeError(f"'{key}' deve ser booleano: {e}")
            if not isinstance(val, bool):
                raise TypeError(f"'{key}' deve ser booleano")

        elif rtype == "string":
            if not isinstance(val, str):
                if rule.get("coerce"):
                    val = str(val)
                else:
                    raise TypeError(f"'{key}' deve ser string")
            val = _validate_string(val=val, rule=rule, key=key)

        elif rtype == "enum":
            _validate_enum(val=val, rule=rule, key=key)

        elif rtype == "regex":
            _validate_regex(val=val, rule=rule, key=key)

        elif rtype is None:
            pass  # tipo livre
        else:
            raise ValueError(f"'{key}': tipo desconhecido no schema: {rtype!r}")

        normalized[key] = val

    # 2) args extras (não descritos no schema)
    if allow_extra_args:
        for k, v in args.items():
            if k not in normalized and (schema is None or k not in schema):
                normalized[k] = v
    else:
        extras = [k for k in args.keys() if k not in (schema or {})]
        if extras:
            raise ValueError(f"Argumentos não permitidos: {extras}")

    return normalized


# --------------------------- checagens auxiliares ---------------------------

def _ensure_unique_registry_names(customs_cfg: CustomsCfg) -> None:
    """
    Garante que não haja nomes duplicados em customs.registry.

    Exceptions
    - ValueError: quando há duplicatas.
    """
    names = [c.name for c in (customs_cfg.registry or [])]
    if len(names) != len(set(names)):
        dup = sorted({n for n in names if names.count(n) > 1})
        raise ValueError(f"customs.registry contém nomes duplicados: {dup}")


# --------------------------- ponto de entrada ---------------------------

def apply_customs_stage(
    df: DataFrame,
    customs_cfg: CustomsCfg,
    stage_name: str,
    *,
    dev_reload: bool = False,
    allow_extra_args: bool = True,
    logger: Optional[Callable[[str], None]] = None,
) -> DataFrame:
    """
    Aplica customs declarados em `customs_cfg.use_in` filtrando por `stage_name`.

    Parâmetros
    - df: DataFrame de entrada.
    - customs_cfg: bloco de configuração dos customs.
    - stage_name: "standard" ou "quarantine".
    - dev_reload: recarrega o módulo a cada passo (para desenvolvimento).
    - allow_extra_args: permite args fora do schema (não validados).
    - logger: callable opcional (ex.: print) para logs de execução.

    Retorno
    - DataFrame resultante após a aplicação sequencial dos customs.

    Exceptions
    - ValueError/TypeError: problemas de schema/args/assinatura.
    - RuntimeError: falhas na execução do custom específico (encadeia exceção original).
    """
    if not customs_cfg or not customs_cfg.allow or not customs_cfg.use_in:
        return df

    _ensure_unique_registry_names(customs_cfg=customs_cfg)

    registry_index: Dict[str, CustomDecl] = {c.name: c for c in customs_cfg.registry}
    _log(logger=logger, msg=f"[customs] stage={stage_name} steps={len(customs_cfg.use_in)} regs={len(registry_index)}")

    current: DataFrame = df
    for idx, step in enumerate(customs_cfg.use_in, start=1):
        if step.stage != stage_name:
            continue

        decl = registry_index.get(step.method)
        if not decl:
            known = ", ".join(sorted(registry_index.keys()))
            raise ValueError(
                f"Custom '{step.method}' não registrado em customs.registry "
                f"(stage='{stage_name}', conhecidos=[{known}])"
            )

        # valida/normaliza args conforme schema do registro
        try:
            norm_args = _validate_and_normalize_args(
                args=step.args or {},
                schema=decl.args_schema or {},
                allow_extra_args=allow_extra_args,
            )
        except Exception as e:
            raise type(e)(
                f"Erro de argumentos no custom '{decl.name}' "
                f"(module='{decl.module}', method='{decl.method}', stage='{stage_name}'): {e}"
            )

        # carrega a função (com hot-reload opcional)
        fn = _load_custom(module=decl.module, method=decl.method, dev_reload=dev_reload)

        _log(logger=logger, msg=f"[customs] ({idx}) {decl.name} -> {decl.module}.{decl.method} args={norm_args}")
        try:
            current = fn(current, **norm_args)
            if not isinstance(current, DataFrame):
                raise TypeError(
                    f"Custom '{decl.name}' retornou tipo inválido ({type(current)}). "
                    f"Esperado pyspark.sql.DataFrame."
                )
        except Exception as e:
            raise RuntimeError(
                f"Falha na execução do custom '{decl.name}' "
                f"(module='{decl.module}', method='{decl.method}', stage='{stage_name}'): {e}"
            ) from e

    return current
