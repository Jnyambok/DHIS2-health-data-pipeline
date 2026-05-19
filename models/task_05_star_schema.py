"""Task 05: Dimensional Model Build"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_star_schema(
    cleaned: DataFrame,
    data_elements: DataFrame,
    org_units: DataFrame,
    programs: DataFrame,
    de_program: DataFrame,
    output_dir: str,
) -> dict:
    logger.info("  Task 05: Building Star Schema")

    # Join DE→program mapping on (dataElement == de_id AND country_name == country)
    # This is needed because the same DE (e.g. DE_CD4) exists in programs for multiple countries
    dep_lookup = F.broadcast(
        de_program.select("de_id", "program_id", "health_area", "country")
    )

    fact = cleaned.join(
        dep_lookup,
        (cleaned.dataElement == dep_lookup.de_id) & (cleaned.country_name == dep_lookup.country),
        "left",
    ).drop("de_id", "country")

    # Surrogate key
    fact = fact.withColumn(
        "fact_id",
        F.md5(F.concat_ws("|",
            F.col("dataElement"), F.col("period"),
            F.col("orgUnit"), F.col("categoryOptionCombo")
        )),
    )

    # Time columns
    fact = (
        fact
        .withColumn("year",       F.substring(F.col("period"), 1, 4).cast("int"))
        .withColumn("month",      F.substring(F.col("period"), 5, 2).cast("int"))
        .withColumn("quarter",
            F.when(F.col("month") <= 3,  F.lit("Q1"))
             .when(F.col("month") <= 6,  F.lit("Q2"))
             .when(F.col("month") <= 9,  F.lit("Q3"))
             .otherwise(F.lit("Q4"))
        )
        .withColumn("year_month",
            F.concat(F.substring(F.col("period"), 1, 4), F.lit("-"), F.substring(F.col("period"), 5, 2))
        )
        .withColumn("date_key",
            F.concat(F.substring(F.col("period"), 1, 4), F.lit("-"), F.substring(F.col("period"), 5, 2))
        )
    )

    # Rename columns to warehouse convention
    fact = (
        fact
        .withColumnRenamed("dataElement",         "data_element_id")
        .withColumnRenamed("orgUnit",              "org_unit_id")
        .withColumnRenamed("categoryOptionCombo",  "coc_id")
        .withColumnRenamed("lastUpdated",          "last_updated")
    )

    fact = fact.select(
        "fact_id", "period", "year", "month", "quarter", "year_month", "date_key",
        "org_unit_id", "facility_name", "facility_level", "ou_path",
        "country_name", "region_name", "district_name",
        "data_element_id", "data_element_name", "valueType", "domainType",
        "program_id", "health_area",
        "coc_id", "coc_name", "is_orphaned_coc",
        "value", "typed_value",
        "is_explicit_zero", "is_missing_value",
        "is_late_reported", "is_outlier",
        "created", "last_updated", "storedBy",
    )

    # Partition by health_area + year_month (not country/period)
    fact.write.mode("overwrite").partitionBy("health_area", "year_month").parquet(
        f"{output_dir}/fact_service_delivery"
    )
    logger.info(f"  fact_service_delivery written → partitioned by health_area, year_month")

    # Dimension tables
    dim_de = (
        data_elements
        .withColumnRenamed("id",           "data_element_id")
        .withColumnRenamed("name",         "data_element_name")
        .withColumnRenamed("valueType",    "value_type")
        .withColumnRenamed("domainType",   "domain_type")
        .withColumnRenamed("categoryComboId", "category_combo_id")
    )

    dim_ou = (
        org_units
        .withColumnRenamed("id",       "org_unit_id")
        .withColumnRenamed("name",     "org_unit_name")
        .withColumnRenamed("level",    "ou_level")
        .withColumnRenamed("path",     "ou_path")
        .withColumnRenamed("parentId", "parent_id")
    )

    dim_prog = (
        programs
        .withColumnRenamed("id",                 "program_id")
        .withColumnRenamed("name",               "program_name")
        .withColumnRenamed("healthArea",         "health_area")
        .withColumnRenamed("reportingFrequency", "reporting_frequency")
    )

    # dim_period: one row per distinct yyyyMM period with year/month/quarter/year_month
    dim_period = (
        fact.select("period", "year", "month", "quarter", "year_month").distinct()
            .orderBy("period")
    )

    for name, df in [
        ("dim_data_element", dim_de),
        ("dim_org_unit",     dim_ou),
        ("dim_program",      dim_prog),
        ("dim_period",       dim_period),
    ]:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{output_dir}/{name}_csv"
        )

    logger.info("  Dimensions written: dim_data_element, dim_org_unit, dim_program, dim_period")

    return {
        "fact":             fact,
        "dim_data_element": dim_de,
        "dim_org_unit":     dim_ou,
        "dim_program":      dim_prog,
        "dim_period":       dim_period,
    }
