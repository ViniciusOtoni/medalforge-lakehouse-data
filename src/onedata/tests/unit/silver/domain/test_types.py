"""
Módulo: test_types
Finalidade: checar o comportamento básico dos aliases NewType usados no contrato.
NewType é identidade em runtime: o valor é um str comum, apenas com anotação de tipo estática.
"""

from onedata.silver.domain.types import TableFQN, ColumnName


def test_newtype_tablefqn_e_columnname_comportam_como_str():
    """Propósito: confirmar que NewType mantém o valor como str em runtime."""
    fqn = TableFQN("silver.sales.orders")
    col = ColumnName("id")
    assert isinstance(fqn, str)
    assert isinstance(col, str)
    assert fqn.split(".")[-1] == "orders"
    assert col.upper() == "ID"
