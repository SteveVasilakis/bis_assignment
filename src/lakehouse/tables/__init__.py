"""Our domain. Importing this package defines every table, which registers it in its layer."""

from lakehouse.tables import bronze, silver  # noqa: F401
