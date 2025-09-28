"""
Configurações do pipeline Silver (flags e paths por ambiente).
"""

from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("ENV", "dev")
    customs_strict: bool = os.getenv("CUSTOMS_STRICT", "0") == "1"
    customs_prefixes: tuple[str, ...] | None = (
        tuple(os.getenv("CUSTOMS_PREFIXES", "onedata.silver.customs,custom_").split(","))
        if os.getenv("CUSTOMS_STRICT", "0") == "1" else None
    )
    silver_external_base: str = os.getenv(
        "SILVER_EXTERNAL_BASE",
        "abfss://silver@medalforgestorage.dfs.core.windows.net",
    )

SETTINGS = Settings()
