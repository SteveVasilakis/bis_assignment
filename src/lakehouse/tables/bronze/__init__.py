"""Bronze layer: the source files as delivered. Importing it registers its tables."""

from lakehouse.tables.bronze import customers, orders, products  # noqa: F401
