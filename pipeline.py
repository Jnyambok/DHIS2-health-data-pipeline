"""
pipeline.py
-----------
PSI DISC DHIS2 Health Data Pipeline

Entry point that runs Tasks 1-7 in dependency order with stage-level
logging and DQ check exit codes. Single-command execution.

Usage:
    python pipeline.py --data-dir ./data --output-dir ./output
    python pipeline.py --data-dir ./data --output-dir ./output --incremental

Exit codes:
    0 = Success
    1 = Critical DQ failure (quarantine rate > 10%, 0 rows in fact table, contract violation)
    2 = Runtime error
"""

import argparse
import logging
import os
import sys
import time

# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Configure structured logging for the pipeline."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PSI DISC DHIS2 Health Data Pipeline"
    )
    parser.add_argument(
        "--data-dir", type=str, default="./data",
        help="Path to input DHIS2 JSON files (default: ./data)"
    )
    parser.add_argument(
        "--output-dir", type=str, default="./output",
        help="Path for pipeline outputs (default: ./output)"
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="Run in incremental mode: only process new periods"
    )
    return parser.parse_args()


def create_spark_session():
    """Create a local SparkSession for the pipeline."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PSI_DISC_DHIS2_Pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Reduce Spark log noise
    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    logger = setup_logging()
    args = parse_args()

    logger.info("=" * 65)
    logger.info("  PSI DISC DHIS2 Health Data Pipeline")
    logger.info("=" * 65)
    logger.info(f"  Data directory:   {os.path.abspath(args.data_dir)}")
    logger.info(f"  Output directory: {os.path.abspath(args.output_dir)}")
    logger.info(f"  Mode:             {'incremental' if args.incremental else 'full'}")
    logger.info("")

    pipeline_start = time.time()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Create Spark session
    spark = create_spark_session()
    logger.info("  SparkSession created (local[*])")

    try:
        # ── TASK 01: JSON Ingestion & Schema Flattening ───────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_01_ingest import load_and_flatten
        ingested = load_and_flatten(spark, args.data_dir)

        # DQ check: quarantine rate
        quarantine_count = ingested["quarantine"].count()
        total_dv = ingested["data_values"].count() + quarantine_count
        quarantine_rate = (quarantine_count / total_dv * 100) if total_dv > 0 else 0

        logger.info(f"  Quarantine rate: {quarantine_rate:.2f}%")
        if quarantine_rate > 10:
            logger.error(f"  CRITICAL: Quarantine rate {quarantine_rate:.2f}% exceeds 10% threshold")
            spark.stop()
            sys.exit(1)

        # Write quarantine
        if quarantine_count > 0:
            ingested["quarantine"].coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(f"{args.output_dir}/quarantine_csv")

        elapsed = time.time() - stage_start
        logger.info(f"  Task 01 complete ({elapsed:.1f}s)")

        # ── BONUS B2: Incremental Load Filter ────────────────────────────
        data_values = ingested["data_values"]

        if args.incremental:
            stage_start = time.time()
            logger.info("")
            logger.info("-" * 50)

            from models.bonus_b2_incremental import get_existing_periods, filter_new_periods
            existing_periods = get_existing_periods(spark, args.output_dir)
            data_values = filter_new_periods(data_values, existing_periods)

            if data_values.count() == 0:
                logger.info("  No new data to process. Pipeline complete.")
                spark.stop()
                sys.exit(0)

            elapsed = time.time() - stage_start
            logger.info(f"  Bonus B2 incremental filter complete ({elapsed:.1f}s)")

        # ── TASK 02: Metadata UID Resolution ──────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_02_metadata import resolve_metadata
        resolved_meta = resolve_metadata(
            data_values,
            ingested["data_elements"],
            ingested["category_option_combos"],
        )

        # Write unresolvable rows
        unresolvable_de_count = resolved_meta["unresolvable_de"].count()
        if unresolvable_de_count > 0:
            resolved_meta["unresolvable_de"].coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(f"{args.output_dir}/unresolvable_data_elements_csv")

        unresolvable_coc_count = resolved_meta["unresolvable_coc"].count()
        if unresolvable_coc_count > 0:
            resolved_meta["unresolvable_coc"].coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(f"{args.output_dir}/unresolvable_coc_csv")

        elapsed = time.time() - stage_start
        logger.info(f"  Task 02 complete ({elapsed:.1f}s)")

        # ── TASK 03: Org Unit Hierarchy Resolution ────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_03_hierarchy import resolve_org_units
        resolved_ou = resolve_org_units(
            resolved_meta["resolved"],
            ingested["org_units"],
        )

        # Write orphaned org unit rows
        orphaned_count = resolved_ou["orphaned_org_units"].count()
        if orphaned_count > 0:
            resolved_ou["orphaned_org_units"].coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(f"{args.output_dir}/orphaned_org_units_csv")

        elapsed = time.time() - stage_start
        logger.info(f"  Task 03 complete ({elapsed:.1f}s)")

        # ── TASK 04: Data Quality & Late-Reporting Flags ──────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_04_dq import run_dq_pipeline
        dq_results = run_dq_pipeline(
            resolved_ou["resolved"],
            ingested["programs"],
        )

        # Write completeness scores
        dq_results["completeness"].coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(f"{args.output_dir}/completeness_scores_csv")

        # Checkpoint cleaned data to disk to break the Spark DAG lineage.
        # Without this, Spark tries to hold the entire pipeline DAG in memory
        # from ingestion through to the fact table write, which causes OOM.
        logger.info("  Checkpointing cleaned data to Parquet...")
        dq_results["cleaned"].write.mode("overwrite").parquet(
            f"{args.output_dir}/_checkpoint_cleaned"
        )
        dq_results["completeness"].write.mode("overwrite").parquet(
            f"{args.output_dir}/_checkpoint_completeness"
        )

        elapsed = time.time() - stage_start
        logger.info(f"  Task 04 complete ({elapsed:.1f}s)")

        # ── TASK 05: Dimensional Model Build ──────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        # Read back from checkpoint to start a fresh DAG
        cleaned_df = spark.read.parquet(f"{args.output_dir}/_checkpoint_cleaned")

        from models.task_05_star_schema import build_star_schema
        star = build_star_schema(
            cleaned_df,
            ingested["data_elements"],
            ingested["org_units"],
            ingested["programs"],
            ingested["de_program"],
            args.output_dir,
        )

        elapsed = time.time() - stage_start
        logger.info(f"  Task 05 complete ({elapsed:.1f}s)")

        # Re-read fact from Parquet (avoids recomputing the full DAG downstream)
        fact_df = spark.read.parquet(f"{args.output_dir}/fact_service_delivery")

        # DQ check: fact table must have rows
        fact_count = fact_df.count()
        logger.info(f"  fact_service_delivery rows: {fact_count:,}")
        if fact_count == 0:
            logger.error("  CRITICAL: fact_service_delivery has 0 rows")
            spark.stop()
            sys.exit(1)

        # ── BONUS B1: Data Contract Validation ────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.bonus_b1_contract import validate_contract, ContractViolationError
        contract_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "contracts",
            "fact_service_delivery.yaml"
        )

        try:
            validate_contract(fact_df, contract_path)
        except ContractViolationError as e:
            logger.error(f"  CRITICAL: {e}")
            spark.stop()
            sys.exit(1)

        elapsed = time.time() - stage_start
        logger.info(f"  Bonus B1 contract validation complete ({elapsed:.1f}s)")

        # ── TASK 06: Program Analytics ────────────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_06_analytics import run_analytics
        analytics = run_analytics(
            fact_df,
            ingested["org_units"],
            args.output_dir,
        )

        elapsed = time.time() - stage_start
        logger.info(f"  Task 06 complete ({elapsed:.1f}s)")

        # ── TASK 07: Cross-Country Aggregation ────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.task_07_aggregation import run_cross_country
        completeness_df = spark.read.parquet(f"{args.output_dir}/_checkpoint_completeness")
        aggregations = run_cross_country(
            fact_df,
            completeness_df,
            args.output_dir,
        )

        elapsed = time.time() - stage_start
        logger.info(f"  Task 07 complete ({elapsed:.1f}s)")

        # ── BONUS B3: Anomaly Detection ───────────────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.bonus_b3_anomalies import detect_anomalies
        anomalies = detect_anomalies(fact_df, args.output_dir)

        elapsed = time.time() - stage_start
        logger.info(f"  Bonus B3 anomaly detection complete ({elapsed:.1f}s)")

        # ── BONUS B4: Metadata Drift Detection ───────────────────────────
        stage_start = time.time()
        logger.info("")
        logger.info("-" * 50)

        from models.bonus_b4_drift import detect_drift
        drift = detect_drift(spark, args.data_dir, args.output_dir)

        elapsed = time.time() - stage_start
        logger.info(f"  Bonus B4 metadata drift detection complete ({elapsed:.1f}s)")

        # ── Summary ──────────────────────────────────────────────────────
        total_elapsed = time.time() - pipeline_start
        logger.info("")
        logger.info("=" * 65)
        logger.info("  Pipeline Summary")
        logger.info("=" * 65)
        logger.info(f"  Total data values ingested:    {total_dv:,}")
        logger.info(f"  Quarantined:                   {quarantine_count:,} ({quarantine_rate:.1f}%)")
        logger.info(f"  Unresolvable dataElements:     {unresolvable_de_count:,}")
        logger.info(f"  Orphaned orgUnits:             {orphaned_count:,}")
        logger.info(f"  Fact table rows:               {fact_count:,}")
        logger.info(f"  Anomalies detected:            {anomalies.count():,}")
        logger.info(f"  Total time:                    {total_elapsed:.1f}s")
        logger.info("=" * 65)
        logger.info("  Pipeline completed successfully")
        logger.info("=" * 65)

    except Exception as e:
        logger.error(f"  Pipeline failed with error: {e}", exc_info=True)
        spark.stop()
        sys.exit(2)

    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
