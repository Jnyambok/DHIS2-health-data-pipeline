"""Bonus B2: Incremental Load Filter"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def get_existing_periods(spark: SparkSession, output_dir: str) -> set:
    fact_path = f"{output_dir}/fact_service_delivery"
    if not os.path.exists(fact_path):
        logger.info("  No existing fact table — all periods treated as new")
        return set()
    try:
        existing = spark.read.parquet(fact_path)
        periods = {row["period"] for row in existing.select("period").distinct().collect()}
        logger.info(f"  Found {len(periods)} existing period(s): {sorted(periods)}")
        return periods
    except Exception as exc:
        logger.warning(f"  Could not read existing fact table: {exc}")
        return set()


def filter_new_periods(data_values: DataFrame, existing_periods: set) -> DataFrame:
    if not existing_periods:
        return data_values
    filtered = data_values.filter(~F.col("period").isin(list(existing_periods)))
    logger.info(f"  After incremental filter: {filtered.count()} rows remain")
    return filtered
