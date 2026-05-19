"""Task 02: Metadata UID Resolution"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def resolve_metadata(
    data_values: DataFrame,
    data_elements: DataFrame,
    category_option_combos: DataFrame,
) -> dict:
    logger.info("  Task 02: Metadata UID Resolution")

    # Broadcast small metadata tables — they're tiny lookups, never shuffled
    de_lookup = F.broadcast(
        data_elements.select(
            F.col("id").alias("_de_id"),
            F.col("name").alias("data_element_name"),
            F.col("valueType"),
            F.col("domainType"),
        )
    )

    # Anti-join isolates rows whose DE UID has no match — removed from pipeline
    unresolvable_de = data_values.join(
        de_lookup, data_values.dataElement == F.col("_de_id"), "left_anti"
    )

    # Inner join keeps only resolvable rows
    resolved_de = data_values.join(
        de_lookup, data_values.dataElement == F.col("_de_id"), "inner"
    ).drop("_de_id")

    coc_lookup = F.broadcast(
        category_option_combos.select(
            F.col("id").alias("_coc_id"),
            F.col("name").alias("coc_name"),
        )
    )

    # Left join COC — orphaned COCs are flagged but kept (value is still usable)
    joined_coc = resolved_de.join(
        coc_lookup, resolved_de.categoryOptionCombo == F.col("_coc_id"), "left"
    )

    resolved = (
        joined_coc
        .withColumn("is_orphaned_coc", F.col("_coc_id").isNull())
        .withColumn(
            "coc_name",
            F.when(F.col("coc_name").isNull(), F.lit("Default")).otherwise(F.col("coc_name")),
        )
        .drop("_coc_id")
    )

    # Separate view of orphaned COC rows for reporting (rows are still in resolved)
    unresolvable_coc = resolved.filter(F.col("is_orphaned_coc"))

    logger.info(
        f"  Resolved: {resolved.count()}, "
        f"Unresolvable DE: {unresolvable_de.count()}, "
        f"Orphaned COC (flagged, kept): {unresolvable_coc.count()}"
    )

    return {
        "resolved":         resolved,
        "unresolvable_de":  unresolvable_de,
        "unresolvable_coc": unresolvable_coc,
    }
