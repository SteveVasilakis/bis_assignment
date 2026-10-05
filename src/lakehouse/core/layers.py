"""Layer, BRONZE / SILVER / GOLD"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Layer:
    """A medallion layer: a database and the tables registered in it."""

    database: str
    tables: dict[str, type] = field(default_factory=dict)

    def register(self, table_cls: type) -> None:
        """Add a table class to this layer."""
        self.tables[table_cls.table_name] = table_cls


BRONZE = Layer("bronze")
SILVER = Layer("silver")
GOLD = Layer("gold")

LAYERS = (BRONZE, SILVER, GOLD)
