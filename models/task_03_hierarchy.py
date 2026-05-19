"""Task 03: Org Unit Hierarchy Resolution"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def resolve_org_units(resolved: DataFrame, org_units: DataFrame) -> dict:
    logger.info("  Task 03: Org Unit Hierarchy Resolution")

    # Facility-level join: attach facility name, level, and path from the path column
    fac_lookup = F.broadcast(
        org_units.select(
            F.col("id").alias("_fac_id"),
            F.col("name").alias("facility_name"),
            F.col("level").alias("facility_level"),
            F.col("path").alias("ou_path"),
        )
    )

    # Anti-join first to capture orphaned org units (UID not in org_units table)
    orphaned = resolved.join(
        fac_lookup, resolved.orgUnit == F.col("_fac_id"), "left_anti"
    )

    # Inner join to keep only resolvable facilities
    joined = resolved.join(
        fac_lookup, resolved.orgUnit == F.col("_fac_id"), "inner"
    ).drop("_fac_id")

    # Parse path column to extract L1/L2/L3 UIDs
    # Path format: /OU_KE/OU_KE_R1/OU_KE_D1/OU_KE_F1 → parts[1]=country, [2]=region, [3]=district
    joined = (
        joined
        .withColumn("_parts",    F.split(F.col("ou_path"), "/"))
        .withColumn("_l1_uid",   F.col("_parts")[1])   # country
        .withColumn("_l2_uid",   F.col("_parts")[2])   # region
        .withColumn("_l3_uid",   F.col("_parts")[3])   # district
        .drop("_parts")
    )

    # Self-join org_units three times to resolve each hierarchy level name
    l1 = F.broadcast(
        org_units.filter(F.col("level") == 1).select(
            F.col("id").alias("_l1_id"),
            F.col("name").alias("country_name"),
        )
    )
    l2 = F.broadcast(
        org_units.filter(F.col("level") == 2).select(
            F.col("id").alias("_l2_id"),
            F.col("name").alias("region_name"),
        )
    )
    l3 = F.broadcast(
        org_units.filter(F.col("level") == 3).select(
            F.col("id").alias("_l3_id"),
            F.col("name").alias("district_name"),
        )
    )

    joined = (
        joined
        .join(l1, joined._l1_uid == l1._l1_id, "left").drop("_l1_id")
        .join(l2, joined._l2_uid == l2._l2_id, "left").drop("_l2_id")
        .join(l3, joined._l3_uid == l3._l3_id, "left").drop("_l3_id")
        .drop("_l1_uid", "_l2_uid", "_l3_uid")
    )

    logger.info(
        f"  Resolved: {joined.count()}, Orphaned org units: {orphaned.count()}"
    )

    return {
        "resolved":           joined,
        "orphaned_org_units": orphaned,
    }
