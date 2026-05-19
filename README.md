# PSI DISC DHIS2 Health Data Pipeline

PySpark pipeline that ingests raw DHIS2 JSON exports, resolves opaque UIDs, builds a star schema, and produces analytics-ready outputs for multi-country health programme monitoring.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (sample data already in data/)
python pipeline.py --data-dir ./data --output-dir ./output

# Run in incremental mode (only process new periods)
python pipeline.py --data-dir ./data --output-dir ./output --incremental

# Run tests
python -m pytest tests/ -v
```

**Requirements:** Python 3.10+, Java 11+ (for PySpark)

> **Sample data:** The `data/` directory contains the 4 DHIS2 Web API JSON files (`metadata.json`, `org_units.json`, `programs.json`, `data_values.json`) with embedded DQ issues for testing. A full synthetic dataset can be generated with the provided `generate_data.py` script if supplied.

## Pipeline Architecture

```
data/                       pipeline.py                    output/
  metadata.json    ──┐                                  ┌── dim_data_element/
  org_units.json   ──┤    Task 01: Ingest & Flatten     ├── dim_org_unit/
  programs.json    ──┤    Task 02: Resolve Metadata     ├── dim_period/
  data_values.json ──┘    Task 03: Resolve Hierarchy    ├── dim_program/
                          Task 04: DQ + Deduplication   ├── fact_service_delivery/
                          Task 05: Star Schema Build    ├── analytics_*/
                          Task 06: Window Analytics     ├── agg_*/
                          Task 07: Cross-Country Agg    ├── anomalies_csv/
                          Bonus B1: Contract Validation ├── completeness_scores_csv/
                          Bonus B2: Incremental Load    └── metadata_drift_csv/
                          Bonus B3: Anomaly Detection
                          Bonus B4: Metadata Drift
```

## File Structure

```
psi-dhis2-pipeline/
├── pipeline.py                    # Entry point. Runs all tasks in order.
├── generate_data.py               # Synthetic data generator (provided).
├── requirements.txt               # Pinned Python dependencies.
├── models/
│   ├── __init__.py
│   ├── task_01_ingest.py          # JSON ingestion with explicit schemas.
│   ├── task_02_metadata.py        # UID resolution via broadcast joins.
│   ├── task_03_hierarchy.py       # Org unit hierarchy via self-join.
│   ├── task_04_dq.py              # Dedup, type casting, DQ flags.
│   ├── task_05_star_schema.py     # Star schema build and Parquet write.
│   ├── task_06_analytics.py       # Window function analytics.
│   ├── task_07_aggregation.py     # Cross-country aggregation.
│   ├── bonus_b1_contract.py       # YAML data contract validation.
│   ├── bonus_b2_incremental.py    # Incremental load logic.
│   ├── bonus_b3_anomalies.py      # Z-score anomaly detection.
│   └── bonus_b4_drift.py          # Metadata drift detection.
├── contracts/
│   └── fact_service_delivery.yaml # Schema contract for the fact table.
├── tests/
│   └── test_contract.py           # Pytest suite for B1 contract validation.
├── data/                          # Generated JSON inputs.
└── output/                        # Pipeline outputs (Parquet + CSV).
```

## Star Schema Design

```
                    ┌─────────────────────┐
                    │   dim_data_element   │
                    │─────────────────────│
                    │ data_element_key (PK)│
                    │ data_element_name    │
                    │ value_type           │
                    │ health_area          │
                    │ aggregation_type     │
                    └──────────┬──────────┘
                               │
┌──────────────────┐   ┌──────┴──────────────────┐   ┌──────────────┐
│  dim_org_unit    │   │  fact_service_delivery   │   │  dim_period  │
│──────────────────│   │─────────────────────────│   │──────────────│
│ org_unit_key (PK)├──>│ data_element_key (FK)   │<──┤ period_key   │
│ facility_name    │   │ org_unit_key (FK)        │   │ year_month   │
│ district_name    │   │ period_key (FK)          │   │ quarter      │
│ region_name      │   │ coc_key (FK)             │   │ year         │
│ country_name     │   │ raw_value                │   └──────────────┘
│ facility_level   │   │ typed_value              │
└──────────────────┘   │ is_explicit_zero         │   ┌──────────────┐
                       │ is_missing_value         │   │ dim_program  │
                       │ is_late_reported         │   │──────────────│
                       │ is_orphaned_coc          │   │ program_key  │
                       │ health_area (partition)   │   │ program_name │
                       │ year_month (partition)    │   │ health_area  │
                       └──────────────────────────┘   │ country      │
                                                       └──────────────┘
```

**Partitioning:** `fact_service_delivery` is partitioned by `health_area` then `year_month`. This matches the most common query pattern: programme managers filter by health area first, then by time range.

**Why no surrogate keys:** DHIS2 UIDs are globally unique 11-character identifiers that already serve as stable natural keys. Adding surrogate keys would create an unnecessary mapping layer without improving join performance.

## Design Decisions

### 0 vs NULL distinction

This is the most critical data quality decision in the pipeline. In health reporting, a facility that reports 0 malaria cases is fundamentally different from a facility that did not report at all. Collapsing these two cases would produce misleading programme dashboards.

The pipeline preserves this distinction through two separate boolean flags:
- `is_explicit_zero`: the facility submitted a data value of "0"
- `is_missing_value`: the facility submitted a row with a NULL value

These flags are never collapsed or merged.

### Ghost UID handling

The synthetic data injects three types of ghost UIDs:
- **Ghost dataElement UIDs (~5%):** Data values referencing indicators not in metadata.json. These are removed via anti-join because without metadata, we cannot determine the indicator name, value type, or health area.
- **Ghost orgUnit UIDs (~4%):** Data values referencing facilities not in org_units.json. These are removed because we cannot place them in the hierarchy.
- **Orphaned COC UIDs (~3%):** Data values referencing category option combos not in metadata.json. These are kept (left join) with an `is_orphaned_coc` flag, because the data value itself is still valid for aggregation even without the disaggregation label.

### Deduplication strategy

- **Exact duplicates:** Removed with `dropDuplicates()`.
- **Near-duplicates:** Same composite key (dataElement + period + orgUnit + COC) but different values. The pipeline keeps the row with the latest `lastUpdated` timestamp, treating it as the most recent correction.

### Late reporting

Rows where `lastUpdated` is more than 60 days after the reporting period end date are flagged with `is_late_reported = True`. These rows are kept in the fact table (they are still valid data) but the flag allows downstream consumers to filter or weight them.

### Checkpointing

After Task 04 (deduplication and DQ flags), the pipeline writes a checkpoint to Parquet and reads it back. This breaks the Spark DAG lineage and prevents memory pressure from accumulating across the full pipeline. Without this, Spark tries to hold the entire transformation chain in memory from ingestion through to the fact table write.

### No UDFs

Every transformation uses native PySpark SQL functions. No Python UDFs, no `pandas_udf`, no `@udf` decorators. This keeps all execution in the JVM and avoids serialization overhead.

## Task Details

### Task 01: JSON Ingestion

Loads all four DHIS2 JSON files using explicit `StructType` schemas. Never uses `inferSchema`. Explodes nested arrays (`dataValues[]`, `dataElements[]`, `organisationUnits[]`, `programs[]`) into flat DataFrames. Quarantines rows with null required fields or invalid period formats.

### Task 02: Metadata UID Resolution

Broadcast joins data values to `dataElements` and `categoryOptionCombos` to resolve opaque UIDs to human-readable names. Anti-joins isolate ghost UIDs. Orphaned COC UIDs are flagged but retained.

### Task 03: Org Unit Hierarchy

Parses the `/L1/L2/L3/L4` path column dynamically. Splits on "/" to extract ancestor UIDs at each level, then self-joins the org unit table to resolve names. No hardcoded UIDs or assumed hierarchy depth.

### Task 04: Data Quality

Three-step process: (1) deduplicate, (2) cast `raw_value` strings to correct Spark types based on `valueType`, add DQ flags, (3) compute completeness scores per facility per period.

### Task 05: Star Schema

Builds four dimension tables and one fact table. All written to Parquet with `mode("overwrite")` for idempotency.

### Task 06: Analytics

Four analytics using Window functions:
1. **MoM % change** per indicator per district (F.lag)
2. **3-month rolling average** per facility per indicator (rowsBetween(-2, 0))
3. **Country reporting rate** with hierarchy rebuild
4. **Top-5 underreporters** per health area (F.rank)

### Task 07: Cross-Country Aggregation

1. Quarterly volumes by health area
2. Cross-country completeness comparison
3. Coverage matrix via pivot
4. Consecutive low-completeness streaks (running group technique)

### Bonus B1: Data Contract

YAML contract validates column existence, types, nullable constraints, value ranges, and minimum row count. All checks run in a single aggregation pass to avoid multiple full scans of the fact table.

### Bonus B2: Incremental Load

Checks existing Parquet partitions for already-loaded periods. Filters incoming data to new periods only. Activated with `--incremental` flag.

### Bonus B3: Anomaly Detection

Computes 12-month rolling mean and standard deviation per facility per indicator. Flags values where abs(z-score) > 3. Output written to CSV for programme manager review.

### Bonus B4: Metadata Drift

Compares current metadata.json to a stored reference snapshot. Reports added, removed, and renamed data elements. Saves current metadata as the new reference after each run.

## Pipeline Summary (from test run)

| Metric | Value |
|---|---|
| Total data values ingested | 152,802 |
| Quarantined | 0 (0.0%) |
| Unresolvable dataElement UIDs | 7,700 |
| Orphaned orgUnit UIDs | 5,804 |
| Exact duplicates removed | 10,095 |
| Near-duplicates resolved | 2,522 |
| Fact table rows | 126,681 |
| Average completeness score | 0.885 |
| Anomalies detected | 9 |
| Total runtime | ~137s (local[*]) |

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Pipeline completed successfully |
| 1 | Critical DQ failure (quarantine > 10%, 0 fact rows, contract violation) |
| 2 | Runtime error |

## Production Recommendations

If deploying this pipeline in a production environment, I would add:

- **Orchestration:** Prefect or Airflow for scheduling, retries, and alerting
- **Storage:** Delta Lake or Apache Iceberg for ACID transactions and time travel
- **Compute:** Databricks or EMR for elastic Spark clusters
- **Observability:** Great Expectations for richer data quality checks, Datadog for pipeline monitoring
- **CI/CD:** GitHub Actions running pytest on every PR, with schema diff checks
- **Secrets:** Environment variables or AWS Secrets Manager for API credentials
