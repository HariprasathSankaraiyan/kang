"""kang: bitemporal fact storage"""

from .core import FactStore
from .db import SchemaNotInitializedError

__version__ = "0.1.0"
__all__ = [
    "FactStore",
    "SchemaNotInitializedError"
]
