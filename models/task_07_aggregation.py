"""Task 07: Cross-Country Aggregation"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def run_cross_country(fact_df: DataFrame, completeness_df: DataFrame, output_dir: str) -> dict:
    logger.info("  Task 07: Cross-Country Aggregation")

    # Cross-country KPIs by data element
    country_kpis = fact_df.groupBy(
        "country_name", "program_id", "health_area",
        "data_element_id", "data_element_name",
    ).agg(
        F.sum("typed_value").alias("total"),
        F.avg("typed_value").alias("mean_per_report"),
        F.countDistinct("org_unit_id").alias("n_facilities"),
        F.countDistinct("period").alias("n_periods"),
    )
    country_kpis.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/cross_country_kpis_csv"
    )

    # Quarterly aggregated volumes by health area / country
    quarterly_volumes = fact_df.groupBy(
        "health_area", "country_name", "year", "quarter"
    ).agg(
        F.sum("typed_value").alias("quarterly_total"),
        F.count("*").alias("record_count"),
        F.countDistinct("org_unit_id").alias("n_facilities"),
    ).orderBy("health_area", "country_name", "year", "quarter")
    quarterly_volumes.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/cross_country_quarterly_csv"
    )

    # Coverage matrix: pivot on country × data element
    # One row per country, one column per data element showing total reported values
    coverage_matrix = (
        fact_df
        .groupBy("country_name")
        .pivot("data_element_name")
        .agg(F.sum("typed_value"))
        .orderBy("country_name")
    )
    coverage_matrix.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/cross_country_coverage_matrix_csv"
    )

    # Country-level completeness over time
    country_completeness = completeness_df.groupBy(
        "country_name", "period"
    ).agg(
        F.avg("completeness_pct").alias("avg_completeness_pct"),
        F.count("*").alias("n_facilities"),
    )
    country_completeness.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/cross_country_completeness_csv"
    )

    # Consecutive low-completeness: facilities with 3+ consecutive periods below 80%
    # Islands-and-gaps: use row_number difference to identify consecutive sequences
    w_rn = Window.partitionBy("orgUnit", "country_name").orderBy("period")
    w_grp = Window.partitionBy("orgUnit", "country_name").orderBy("period")

    low_comp = completeness_df.filter(F.col("completeness_pct") < 80)

    consecutive_low = (
        low_comp
        .withColumn("_rn", F.row_number().over(w_rn))
        # Subtract row_number from a dense rank over all periods to get a group key
        # Rows in the same consecutive run share the same (_period_rank - _rn) value
        .withColumn(
            "_period_int",
            (F.substring(F.col("period"), 1, 4).cast("int") * 100 +
             F.substring(F.col("period"), 5, 2).cast("int"))
        )
        .withColumn(
            "_dense",
            F.dense_rank().over(
                Window.partitionBy("orgUnit", "country_name").orderBy("_period_int")
            )
        )
        .withColumn("_group_key", F.col("_dense") - F.col("_rn"))
        .groupBy("orgUnit", "country_name", "_group_key")
        .agg(
            F.count("*").alias("consecutive_low_periods"),
            F.min("period").alias("streak_start"),
            F.max("period").alias("streak_end"),
            F.avg("completeness_pct").alias("avg_completeness_pct"),
        )
        .filter(F.col("consecutive_low_periods") >= 3)
        .drop("_group_key")
        .orderBy(F.desc("consecutive_low_periods"))
    )
    consecutive_low.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/consecutive_low_completeness_csv"
    )

    # Global aggregation across all countries
    global_agg = fact_df.groupBy(
        "program_id", "health_area", "data_element_id", "data_element_name", "period"
    ).agg(
        F.sum("typed_value").alias("global_total"),
        F.countDistinct("country_name").alias("n_countries"),
        F.countDistinct("org_unit_id").alias("n_facilities"),
    )
    global_agg.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/global_aggregation_csv"
    )

    logger.info(
        "  Aggregations written: cross_country_kpis, quarterly_volumes, coverage_matrix, "
        "completeness, consecutive_low_completeness, global_aggregation"
    )

    return {
        "country_kpis":       country_kpis,
        "quarterly_volumes":  quarterly_volumes,
        "coverage_matrix":    coverage_matrix,
        "country_completeness": country_completeness,
        "consecutive_low":    consecutive_low,
        "global_agg":         global_agg,
    }
