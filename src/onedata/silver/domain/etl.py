"""
Modelos Pydantic para etapas de ETL, quarentena e customs da camada Silver.
"""

from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field


class Step(BaseModel):
    """
    Passo/etapa de ETL ou custom.

    Atributos
    ----------
    method : str
        Nome do método (core/custom) a invocar.
    args : dict
        Argumentos do método.
    stage : {"standard","quarantine"} | None
        Usado apenas em customs.use_in (stage de aplicação).
    """
    method: str
    args: Dict[str, Any] = Field(default_factory=dict)
    stage: Optional[Literal["standard", "quarantine"]] = None


class QuarantineCfg(BaseModel):
    """
    Configuração da quarentena.

    Atributos
    ----------
    remediate : list[Step]
        Etapas de remediação aplicadas aos dados quarentenados.
    sink : dict[str,str] | None
        Destino opcional para persistência da quarentena (ex.: {"table": "monitoring.quarantine.foo"}).
    """
    remediate: List[Step] = Field(default_factory=list)
    sink: Optional[Dict[str, str]] = None


class CustomDecl(BaseModel):
    """
    Declaração de um método custom disponível no registro.

    Atributos
    ----------
    name : str
        Identificador do custom (referenciado em customs.use_in.method).
    module : str
        Módulo Python onde a função está definida.
    method : str
        Nome da função dentro do módulo.
    args_schema : dict
        Schema livre interpretado pela engine de customs para validar/coagir args.
    """
    name: str
    module: str
    method: str
    args_schema: Dict[str, Any] = Field(default_factory=dict)


class CustomsCfg(BaseModel):
    """
    Configuração dos customs no contrato.

    Atributos
    ----------
    allow : bool
        Habilita/desabilita execução de customs.
    registry : list[CustomDecl]
        Catálogo de métodos disponíveis.
    use_in : list[Step]
        Steps que aplicam customs por estágio (ex.: stage="standard").
    """
    allow: bool = False
    registry: List[CustomDecl] = Field(default_factory=list)
    use_in: List[Step] = Field(default_factory=list)
