# AI-assisted: written with the help of an AI coding assistant (Claude Code).
from pyspark.sql.types import LongType, StringType, StructField, StructType

from lakehouse.core.dq import FAILED, QUARANTINE, Rule, evaluate

SCHEMA = StructType([StructField("id", LongType()), StructField("email", StringType())])


def test_evaluate_tags_failures_and_quarantines_only_error_rules(spark):
    df = spark.createDataFrame([(1, "a@b.c"), (2, "nope"), (None, None)], SCHEMA)
    rules = (
        Rule("id_not_null", "id IS NOT NULL"),
        Rule("email_has_at", "email LIKE '%@%'", "warn"),
    )
    rows = {r["id"]: r for r in evaluate(df, rules).collect()}

    assert rows[1][FAILED] == [] and rows[1][QUARANTINE] is False
    # A warn rule is recorded but doesn't quarantine.
    assert rows[2][FAILED] == ["email_has_at"] and rows[2][QUARANTINE] is False
    # NULL makes a predicate NULL, which counts as a failure.
    assert rows[None][FAILED] == ["id_not_null", "email_has_at"]
    assert rows[None][QUARANTINE] is True
