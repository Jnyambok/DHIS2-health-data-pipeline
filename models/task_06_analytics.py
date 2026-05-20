"""Task 06: Program Analytics"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def run_analytics(fact_df: DataFrame, org_units: DataFrame, output_dir: str) -> dict:
    logger.info("  Task 06: Program Analytics")

    # Monthly volume by program / country
    monthly_volume = fact_df.groupBy(
        "program_id", "health_area", "country_name", "period", "year_month"
    ).agg(
        F.sum("typed_value").alias("total_value"),
        F.count("*").alias("record_count"),
        F.countDistinct("org_unit_id").alias("reporting_facilities"),
    )
    monthly_volume.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_monthly_volume_csv"
    )

    # KPI summary per data element / country
    kpi_summary = fact_df.groupBy(
        "data_element_id", "data_element_name", "country_name", "program_id"
    ).agg(
        F.sum("typed_value").alias("total"),
        F.avg("typed_value").alias("mean"),
        F.min("typed_value").alias("min"),
        F.max("typed_value").alias("max"),
        F.stddev("typed_value").alias("stddev"),
        F.count("*").alias("n_records"),
    )
    kpi_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_kpi_summary_csv"
    )

    # MoM % change per indicator per district (aggregate to district level first)
    district_monthly = fact_df.groupBy(
        "district_name", "country_name", "data_element_id", "data_element_name", "period"
    ).agg(F.sum("typed_value").alias("district_total"))

    w_pop = Window.partitionBy("district_name", "data_element_id").orderBy("period")
    pop_growth = (
        district_monthly
        .withColumn("prev_value", F.lag("district_total", 1).over(w_pop))
        .withColumn(
            "pct_change",
            F.when(
                F.col("prev_value").isNotNull() & (F.col("prev_value") != 0),
                F.round(
                    (F.col("district_total") - F.col("prev_value")) / F.col("prev_value") * 100, 2
                ),
            ),
        )
    )
    pop_growth.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_period_over_period_csv"
    )

    # 3-month rolling average per facility / data element
    w_roll = (
        Window.partitionBy("org_unit_id", "data_element_id")
              .orderBy("period")
              .rowsBetween(-2, 0)
    )
    rolling_avg = fact_df.withColumn(
        "rolling_3m_avg", F.round(F.avg("typed_value").over(w_roll), 2)
    ).select(
        "org_unit_id", "facility_name", "country_name",
        "data_element_id", "data_element_name",
        "period", "year_month",
        "typed_value", "rolling_3m_avg",
    )
    rolling_avg.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_rolling_avg_csv"
    )

    # Country reporting rate: facilities reporting / total known facilities in country
    total_facilities = (
        org_units.filter(F.col("level") == 4)
                 .groupBy("path")
                 .agg(F.count("*").alias("_dummy"))
    )
    # Derive country UID from path (first segment after leading /)
    facilities_per_country = (
        org_units.filter(F.col("level") == 4)
                 .withColumn("_l1", F.split(F.col("path"), "/")[1])
    )
    # Join against country names via L1 UID
    l1_names = org_units.filter(F.col("level") == 1).select(
        F.col("id").alias("_l1_id"), F.col("name").alias("_country")
    )
    fac_counts = (
        facilities_per_country
        .join(F.broadcast(l1_names), F.col("_l1") == F.col("_l1_id"), "left")
        .groupBy(F.col("_country").alias("country_name"))
        .agg(F.count("*").alias("total_known_facilities"))
    )

    reporting_rate = (
        fact_df.groupBy("country_name", "period")
               .agg(F.countDistinct("org_unit_id").alias("reporting_facilities"))
               .join(F.broadcast(fac_counts), on="country_name", how="left")
               .withColumn(
                   "reporting_rate_pct",
                   F.round(F.col("reporting_facilities") / F.col("total_known_facilities") * 100, 2),
               )
    )
    reporting_rate.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_reporting_rate_csv"
    )

    # Top-5 underreporters per health area: ranked by periods with zero or missing data
    w_under = Window.partitionBy("health_area").orderBy(F.desc("zero_or_missing_periods"))
    underreporters = (
        fact_df
        .withColumn("_is_zero_or_null", F.col("is_explicit_zero") | F.col("is_missing_value"))
        .groupBy("org_unit_id", "facility_name", "country_name", "health_area")
        .agg(
            F.sum(F.col("_is_zero_or_null").cast("int")).alias("zero_or_missing_periods"),
            F.count("*").alias("total_periods"),
        )
        .withColumn("rank", F.dense_rank().over(w_under))
        .filter(F.col("rank") <= 5)
    )
    underreporters.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/analytics_underreporters_csv"
    )

    logger.info(
        "  Analytics written: monthly_volume, kpi_summary, period_over_period, "
        "rolling_avg, reporting_rate, underreporters"
    )

    return {
        "monthly_volume":  monthly_volume,
        "kpi_summary":     kpi_summary,
        "pop_growth":      pop_growth,
        "rolling_avg":     rolling_avg,
        "reporting_rate":  reporting_rate,
        "underreporters":  underreporters,
    }
