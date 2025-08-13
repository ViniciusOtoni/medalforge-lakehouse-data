from pydantic import BaseModel
from typing import List

class ColumnDef(BaseModel):
    name: str
    comment: str = ""

class BronzeContract(BaseModel):
    target_schema: str
    target_table: str
    source_format: str
    columns: List[ColumnDef]
