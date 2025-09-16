from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator

class TargetWriteCfg(BaseModel):
    mode: Literal["merge", "append", "overwrite"] = "merge"
    merge_keys: List[str] = Field(default_factory=list)
    partition_by: List[str] = Field(default_factory=list)
    zorder_by: List[str] = Field(default_factory=list)

class TargetCfg(BaseModel):
    catalog: str
    schema: str
    table: str
    write: TargetWriteCfg

class DQCheck(BaseModel):
    name: str
    function: Literal["is_not_null", "unique", "is_in_range", "sql_expression"]
    arguments: Dict[str, Any] = Field(default_factory=dict)
    criticality: Literal["error", "warning"] = "error"

class DQXCfg(BaseModel):
    criticality_default: Literal["error", "warning"] = "error"
    checks: List[DQCheck] = Field(default_factory=list)
    custom: List[DQCheck] = Field(default_factory=list)

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
        return f"{self.target.catalog}.{self.target.schema}.{self.target.table}"
