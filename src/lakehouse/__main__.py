"""Entrypoint: python -m lakehouse --env local"""

from __future__ import annotations

import argparse
import logging

import lakehouse.tables  # noqa: F401  (importing the tables registers them in their layers)
from lakehouse.config import ENVIRONMENTS, get_config
from lakehouse.core.pipeline import Pipeline
from lakehouse.spark import create_spark


def main() -> None:
    """Run the pipeline for the environment given by `--env`."""
    parser = argparse.ArgumentParser(prog="lakehouse", description="Run the medallion pipeline")
    parser.add_argument("--env", default="local", choices=sorted(ENVIRONMENTS))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)

    config = get_config(args.env)
    spark = create_spark(config)
    try:
        pipeline = Pipeline(spark, config)
        logging.getLogger("lakehouse").info(
            f"Execution order: {[t.qualified_name() for t in pipeline.execution_order()]}"
        )
        pipeline.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
