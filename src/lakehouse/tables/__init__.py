"""Our domain. Importing this package defines every table, which registers it in its layer."""

from lakehouse.tables import bronze, gold, silver  # noqa: F401
