"""Task 04: Data Quality & Late-Reporting Flags"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

_LATE_DAYS_THRESHOLD = 60   # days after period end


def _deduplicate(df: DataFrame) -> DataFrame:
    # Exact duplicates: identical on all business keys + value
    df = df.dropDuplicates(["dataElement", "period", "orgUnit", "categoryOptionCombo", "value"])

    # Near-duplicates (same keys, different value/timestamp): keep the most recently updated
    w = Window.partitionBy(
        "dataElement", "period", "orgUnit", "categoryOptionCombo"
    ).orderBy(
        F.coalesce(
            F.to_timestamp(F.col("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            F.to_timestamp(F.col("lastUpdated"), "yyyy-MM-dd"),
        ).desc()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    return df


def _add_value_flags(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "is_explicit_zero",
        F.col("value") == F.lit("0"),
    ).withColumn(
        "is_missing_value",
        F.col("value").isNull() | (F.trim(F.col("value")) == ""),
    ).withColumn(
        "typed_value",
        F.col("value").cast(DoubleType()),
    )
    return df


def _flag_late_reporting(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "_submit_ts",
        F.coalesce(
            F.to_timestamp(F.col("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            F.to_timestamp(F.col("lastUpdated"), "yyyy-MM-dd"),
            F.to_timestamp(F.col("created"),     "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            F.to_timestamp(F.col("created"),     "yyyy-MM-dd"),
        ),
    )

    df = (
        df.withColumn("_yr", F.substring(F.col("period"), 1, 4).cast("int"))
          .withColumn("_mo", F.substring(F.col("period"), 5, 2).cast("int"))
          .withColumn("_period_end", F.last_day(F.make_date(F.col("_yr"), F.col("_mo"), F.lit(1))))
    )

    df = df.withColumn(
        "is_late_reported",
        F.when(
            F.col("_submit_ts").isNotNull() & F.col("_period_end").isNotNull(),
            F.datediff(F.col("_submit_ts").cast("date"), F.col("_period_end")) > _LATE_DAYS_THRESHOLD,
        ).otherwise(F.lit(False)),
    ).drop("_submit_ts", "_yr", "_mo", "_period_end")

    return df


def _flag_outliers(df: DataFrame) -> DataFrame:
    quantiles = df.groupBy("dataElement").agg(
        F.percentile_approx("typed_value", 0.25).alias("_q1"),
        F.percentile_approx("typed_value", 0.75).alias("_q3"),
    ).withColumn("_iqr", F.col("_q3") - F.col("_q1"))

    df = df.join(F.broadcast(quantiles), on="dataElement", how="left")
    df = df.withColumn(
        "is_outlier",
        F.when(
            F.col("_iqr").isNotNull() & (F.col("_iqr") > 0) & F.col("typed_value").isNotNull(),
            (F.col("typed_value") < (F.col("_q1") - 3 * F.col("_iqr"))) |
            (F.col("typed_value") > (F.col("_q3") + 3 * F.col("_iqr"))),
        ).otherwise(F.lit(False)),
    ).drop("_q1", "_q3", "_iqr")

    return df


def _compute_completeness(df: DataFrame) -> DataFrame:
    # Expected = number of distinct DEs per facility × period per program
    # Use country_name + health_area from hierarchy to group facilities
    expected = (
        df.select("dataElement", "country_name").distinct()
          .groupBy("country_name")
          .agg(F.count("dataElement").alias("expected_de_count"))
    )

    actual = df.groupBy("orgUnit", "country_name", "period").agg(
        F.countDistinct("dataElement").alias("actual_de_count"),
        F.count("*").alias("row_count"),
    )

    return actual.join(expected, on="country_name", how="left").withColumn(
        "completeness_pct",
        F.round(F.col("actual_de_count") / F.col("expected_de_count") * 100, 2),
    )


def run_dq_pipeline(resolved: DataFrame, programs: DataFrame) -> dict:
    logger.info("  Task 04: Data Quality & Late-Reporting Flags")

    cleaned = _deduplicate(resolved)
    cleaned = _add_value_flags(cleaned)
    cleaned = _flag_late_reporting(cleaned)
    cleaned = _flag_outliers(cleaned)

    late_count    = cleaned.filter(F.col("is_late_reported")).count()
    outlier_count = cleaned.filter(F.col("is_outlier")).count()
    zero_count    = cleaned.filter(F.col("is_explicit_zero")).count()
    null_count    = cleaned.filter(F.col("is_missing_value")).count()
    logger.info(
        f"  Late: {late_count}, Outliers: {outlier_count}, "
        f"Explicit zeros: {zero_count}, Missing values: {null_count}"
    )

    completeness = _compute_completeness(cleaned)
    avg_row = completeness.agg(F.avg("completeness_pct")).collect()[0][0]
    logger.info(f"  Avg completeness: {avg_row:.1f}%" if avg_row else "  Avg completeness: N/A")

    return {"cleaned": cleaned, "completeness": completeness}
