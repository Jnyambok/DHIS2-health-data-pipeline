"""Bonus B4: Metadata Drift Detection"""
import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)

_DRIFT_SCHEMA = StructType([
    StructField("element_id",   StringType(), True),
    StructField("element_name", StringType(), True),
    StructField("drift_type",   StringType(), True),
])


def _load_current_des(data_dir: str) -> dict:
    path = os.path.join(data_dir, "metadata.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return {de["id"]: de.get("name", de["id"]) for de in doc.get("dataElements", [])}


def detect_drift(spark: SparkSession, data_dir: str, output_dir: str):
    logger.info("  Bonus B4: Metadata Drift Detection")

    snapshot_path = os.path.join(output_dir, "_metadata_snapshot.json")
    current = _load_current_des(data_dir)
    drift_rows = []

    if not os.path.exists(snapshot_path):
        with open(snapshot_path, "w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2)
        logger.info(f"  Snapshot created with {len(current)} data element(s) (no prior baseline)")
        drift_rows = [{"element_id": k, "element_name": v, "drift_type": "new_baseline"} for k, v in current.items()]
    else:
        with open(snapshot_path, encoding="utf-8") as fh:
            previous = json.load(fh)

        for uid, name in current.items():
            if uid not in previous:
                drift_rows.append({"element_id": uid, "element_name": name, "drift_type": "added"})

        for uid, name in previous.items():
            if uid not in current:
                drift_rows.append({"element_id": uid, "element_name": name, "drift_type": "removed"})

        for uid in set(current) & set(previous):
            if current[uid] != previous[uid]:
                drift_rows.append({
                    "element_id": uid,
                    "element_name": f"{previous[uid]} -> {current[uid]}",
                    "drift_type": "renamed",
                })

        added   = sum(1 for r in drift_rows if r["drift_type"] == "added")
        removed = sum(1 for r in drift_rows if r["drift_type"] == "removed")
        renamed = sum(1 for r in drift_rows if r["drift_type"] == "renamed")
        logger.info(f"  Drift: {added} added, {removed} removed, {renamed} renamed")

        with open(snapshot_path, "w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2)

    if drift_rows:
        drift_df = spark.createDataFrame(drift_rows, _DRIFT_SCHEMA)
        drift_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{output_dir}/metadata_drift_csv"
        )
    else:
        drift_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), _DRIFT_SCHEMA)

    return drift_df
