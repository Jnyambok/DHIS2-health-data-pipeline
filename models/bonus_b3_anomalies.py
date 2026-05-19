"""Bonus B3: Anomaly Detection"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def detect_anomalies(fact_df: DataFrame, output_dir: str) -> DataFrame:
    logger.info("  Bonus B3: Anomaly Detection")

    stats = fact_df.groupBy("data_element_id").agg(
        F.avg("typed_value").alias("_mean"),
        F.stddev("typed_value").alias("_std"),
    )

    zscore_df = fact_df.join(F.broadcast(stats), on="data_element_id", how="left").withColumn(
        "z_score",
        F.when(
            F.col("_std").isNotNull() & (F.col("_std") > 0) & F.col("typed_value").isNotNull(),
            F.abs(F.col("typed_value") - F.col("_mean")) / F.col("_std"),
        ).otherwise(F.lit(0.0)),
    ).drop("_mean", "_std")

    zscore_anomalies = zscore_df.filter(F.col("z_score") > 3.0).select(
        "fact_id", "period", "country_name", "facility_name",
        "data_element_name", "health_area", "typed_value", "z_score",
        F.lit("z_score_outlier").alias("anomaly_type"),
    )

    w = Window.partitionBy("org_unit_id", "data_element_id").orderBy("period")
    spike_df = fact_df.withColumn("_prev", F.lag("typed_value", 1).over(w)).withColumn(
        "z_score",
        F.when(
            F.col("_prev").isNotNull() & (F.col("_prev") > 0),
            (F.col("typed_value") - F.col("_prev")) / F.col("_prev") * 100,
        ).otherwise(F.lit(0.0)),
    ).drop("_prev")

    spike_anomalies = spike_df.filter(F.col("z_score") > 300).select(
        "fact_id", "period", "country_name", "facility_name",
        "data_element_name", "health_area", "typed_value", "z_score",
        F.lit("period_spike").alias("anomaly_type"),
    )

    anomalies = zscore_anomalies.unionByName(spike_anomalies)
    anomaly_count = anomalies.count()
    logger.info(f"  Detected {anomaly_count} anomalies")

    if anomaly_count > 0:
        anomalies.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{output_dir}/anomalies_csv"
        )

    return anomalies
