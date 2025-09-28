"""
Contrato completo da camada Silver.

Este módulo agrega os submodelos (target, dqx, etl) e fornece a
estrutura principal `SilverYaml`, incluindo helpers e validações globais.
"""

from typing import Dict, List
from pydantic import BaseModel, ConfigDict, Field, field_validator

from onedata.silver.domain.types import TableFQN
from onedata.silver.domain.target import TargetCfg
from onedata.silver.domain.dqx import DQXCfg
from onedata.silver.domain.etl import Step, QuarantineCfg, CustomsCfg


class SilverYaml(BaseModel):
    """
    Contrato completo da camada Silver.

    Atributos
    ----------
    version : str
        Versão do contrato (suporta apenas 1.x).
    source : dict[str,str]
        Origem dos dados (ex.: {"bronze_table": "bronze.schema.table"}).
    target : TargetCfg
        Destino (Unity Catalog) e política de escrita.
    dqx : DQXCfg
        Configuração de checks de qualidade (DQX).
    etl : dict[str, list[Step]]
        Etapas de ETL padrão (ex.: {"standard": [...] }).
    quarantine : QuarantineCfg
        Regras de remediação e persistência da quarentena.
    customs : CustomsCfg
        Registro e uso de customs por estágio.

    Regras
    ------
    • extra="forbid": rejeita campos não declarados no YAML.
    • version: apenas 1.x suportado no momento.

    Helpers
    -------
    • target_fqn: FQN do destino (catalog.schema.table).
    """
    model_config = ConfigDict(extra="forbid")

    version: str
    source: Dict[str, str] = Field(default_factory=dict)
    target: TargetCfg
    dqx: DQXCfg
    etl: Dict[str, List[Step]] = Field(default_factory=dict)
    quarantine: QuarantineCfg
    customs: CustomsCfg

    @field_validator("version")
    @classmethod
    def v1_only(cls, v: str) -> str:
        """Garante que a versão principal do contrato seja 1.x."""
        if v.split(".")[0] != "1":
            raise ValueError("Somente versão 1.x suportada por enquanto.")
        return v

    @property
    def target_fqn(self) -> TableFQN:
        """Retorna o FQN de destino (catalog.schema.table)."""
        return TableFQN(f"{self.target.catalog}.{self.target.schema_name}.{self.target.table}")
