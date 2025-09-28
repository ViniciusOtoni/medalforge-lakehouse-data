"""
Modelos Pydantic para o destino (Unity Catalog) e a política de escrita
da camada Silver.
"""

from typing import List, Literal
from pydantic import BaseModel, Field, ConfigDict

from onedata.silver.domain.types import ColumnName, MergeKeys

class TargetWriteCfg(BaseModel):
    """
    Configuração de escrita da tabela Silver.

    Atributos
    ----------
    mode : {"merge","append","overwrite"}
        Estratégia de gravação (padrão: "merge").
    merge_keys : list[str]
        Chaves do MERGE quando mode="merge".
    partition_by : list[str]
        Colunas de particionamento físico.
    zorder_by : list[str]
        Colunas para OPTIMIZE ZORDER BY (quando aplicável).
    """
    mode: Literal["merge", "append", "overwrite"] = "merge"
    merge_keys: MergeKeys = Field(default_factory=list)
    partition_by: List[ColumnName] = Field(default_factory=list)
    zorder_by: List[ColumnName] = Field(default_factory=list)


class TargetCfg(BaseModel):
    """
    Destino da tabela Silver (Unity Catalog) e política de escrita.

    Notas
    -----
    • Usamos 'schema_name' com alias 'schema' para evitar colisão com BaseModel.schema().

    Atributos
    ----------
    catalog : str
        Catálogo UC de destino.
    schema_name : str
        Nome do schema (YAML: usa chave 'schema').
    table : str
        Nome da tabela.
    write : TargetWriteCfg
        Parâmetros de gravação.
    """
    model_config = ConfigDict(populate_by_name=True)

    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    write: TargetWriteCfg
