"""
SDK mínimo para customs: decorator de marcação e tipo Logger.
"""

from typing import Callable, Optional

def custom(fn: Callable) -> Callable:
    """
    Marca uma função como custom permitido.
    Exigido quando require_marked_decorator=True no runner.
    """
    fn.__onedata_custom__ = True
    return fn

Logger = Optional[Callable[[str], None]]
