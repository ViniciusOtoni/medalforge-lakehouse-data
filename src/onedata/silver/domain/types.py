"""
Tipos e aliases usados nos modelos de contrato da camada Silver.

Fornece apelidos semânticos para aumentar legibilidade e estabilidade
de assinaturas (ex.: FQN de tabela, nomes de colunas, chaves de merge).
"""

from typing import NewType, List

# FQN completo no Unity Catalog: catalog.schema.table
TableFQN = NewType("TableFQN", str)

# Nome de coluna simples (sem qualificador)
ColumnName = NewType("ColumnName", str)

# Lista de colunas usadas como chaves (ex.: MERGE)
MergeKeys = List[ColumnName]
