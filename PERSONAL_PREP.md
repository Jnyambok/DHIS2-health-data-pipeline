# PERSONAL PREP: Technical Interview Presentation

**Role:** Associate Data Engineer, PSI DISC
**Date:** Monday, May 25, 2026
**Format:** Technical walkthrough of the take-home assessment

---

## PART 1: STEP-BY-STEP PRESENTATION SCRIPT

Use this as your exact talk track. Practice it out loud at least twice before Monday.

---

### SLIDE 1: Title (30 seconds)

**Say this:**

"Good morning. My name is Julius Nyambok. Thank you for having me today. I am going to walk you through my approach to the DHIS2 Health Data Pipeline assessment. I will cover the pipeline architecture, the key design decisions I made, how I handled data quality, and how I would extend this system for production use at PSI."

---

### SLIDE 2: Pipeline Overview (1 minute)

**Say this:**

"The pipeline follows a seven-task structure, plus four bonus challenges. It starts with raw JSON ingestion, moves through metadata resolution and hierarchy building, handles data quality and deduplication, builds a star schema, and finishes with analytics and cross-country aggregation."

"I used PySpark throughout because it handles the scale of DHIS2 data across multiple countries. The pipeline runs as a single command. It took about 137 seconds to process 152,000 data values across 5 countries, 576 facilities, 40 data elements, and 12 reporting periods."

---

### SLIDE 3: JSON Ingestion (1.5 minutes)

**Say this:**

"Task 01 loads all four DHIS2 JSON files using explicit StructType schemas. I never use inferSchema. This is important because inferSchema reads the entire file twice and can silently mistype fields."

"Each JSON file has nested arrays. For example, data_values.json wraps all records inside a dataValues array. I explode these into flat row-level DataFrames."

"Any row missing a required field like dataElement, period, or orgUnit, or any row with an invalid period format, goes to a quarantine table. If the quarantine rate exceeds 10%, the pipeline exits with code 1. In my test run, the quarantine rate was 0% because the generator produces structurally valid JSON. The quarantine logic exists for real-world scenarios where DHIS2 exports may contain corrupted rows."

---

### SLIDE 4: UID Resolution and Hierarchy (2 minutes)

**Say this:**

"This is one of the most important parts of the pipeline. DHIS2 stores everything as opaque 11-character UIDs. A data value just says dataElement eVOBA1fRmfG, orgUnit a5bI9JKyhJ3. Humans cannot work with that."

"In Task 02, I use broadcast joins to resolve dataElement and categoryOptionCombo UIDs to human-readable names. Broadcast joins work well here because the metadata tables are small. 40 data elements, 21 category option combos. These fit entirely in driver memory."

"I use anti-joins to isolate ghost UIDs. The generator injected about 5% ghost dataElement UIDs and 3% ghost COC UIDs. Ghost dataElement rows are discarded because without metadata, we do not know the indicator name, value type, or health area. But orphaned COC rows are kept with a flag. The data value is still valid for aggregation. We just lose the disaggregation label."

"In Task 03, I build the org unit hierarchy by parsing the path column. The path looks like /L1_uid/L2_uid/L3_uid/L4_uid. I join org units once to resolve the facility name and capture the full path. Then I split the path on slash, extract the country, region, and district UIDs at positions 1, 2, and 3, and self-join the org unit table three additional times to resolve country name, region name, and district name. No UIDs are hardcoded. If a country adds a fifth level, I would add one more self-join."

---

### SLIDE 5: 0 vs NULL (1.5 minutes)

**Say this:**

"This was the single most important design decision in the pipeline. In health reporting, zero and null mean completely different things."

"A facility that reports zero malaria cases is telling you something valuable. It means they are operational, they tested patients, and nobody tested positive. That is good news."

"A facility that reports null is telling you nothing. Maybe they are closed. Maybe the health worker forgot to submit. Maybe the supply chain broke. You do not know."

"If you collapse these two into the same value, your programme dashboards become misleading. A district that looks like it has low malaria cases might actually have facilities that stopped reporting."

"I handle this with two separate boolean flags: is_explicit_zero and is_missing_value. These flags are never merged. Downstream consumers can filter or weight based on whichever flag matters for their use case."

---

### SLIDE 6: Deduplication (1 minute)

**Say this:**

"The generator injects about 8% exact duplicates and 2% near-duplicates. I handle them in two steps."

"Exact duplicates are removed with dropDuplicates. Simple."

"Near-duplicates are trickier. These are rows with the same composite key, meaning the same dataElement, period, orgUnit, and categoryOptionCombo, but different values. This happens when a health worker submits data and then corrects it later."

"I use a Window function partitioned by the composite key, ordered by lastUpdated descending, and keep only row number 1. This means the most recent correction wins."

"In my test run, I removed 10,095 exact duplicates and resolved 2,522 near-duplicates. The fact table ended up with 126,681 rows from the original 152,802."

---

### SLIDE 7: Star Schema (1.5 minutes)

**Say this:**

"I built a classic star schema with one fact table and four dimensions."

"The fact table, fact_service_delivery, is partitioned by health_area and year_month. This matches the most common query pattern. Programme managers at PSI filter by health area first, like Malaria or HIV, then narrow by time range."

"I did not generate surrogate keys. DHIS2 UIDs are globally unique 11-character identifiers. They already serve as stable natural keys. Adding an auto-increment integer would create an extra mapping layer without improving join performance."

"The fact table stores both the raw string value and the typed numeric value. This preserves the original DHIS2 export for audit purposes while giving analysts a clean numeric column to aggregate."

---

### SLIDE 8: Analytics and Window Functions (1.5 minutes)

**Say this:**

"Task 06 produces four analytics, all using native Spark Window functions. No Python UDFs."

"First, month-over-month percentage change per indicator per district. I use F.lag to get the previous month's value, then calculate the percentage difference."

"Second, a 3-month rolling average per facility per indicator. I use rowsBetween negative 2, 0 to look at the current row and two preceding months."

"Third, country-level reporting rate. I count the distinct facilities that submitted data in each period and divide by the total expected facilities from the hierarchy."

"Fourth, top-5 underreporting facilities per health area, ranked by periods with zero or missing data. I use F.rank over a Window."

"All four use native Spark SQL functions only. No UDFs, no pandas_udf. This keeps everything in the JVM and avoids Python serialization overhead."

---

### SLIDE 9: Bonus Challenges (1.5 minutes)

**Say this:**

"I completed all four bonus challenges."

"Bonus 1 is a YAML data contract. It validates column existence, types, nullable constraints, value ranges, and minimum row count. All checks run in a single aggregation pass to avoid scanning the fact table multiple times. If the contract fails, the pipeline exits with code 1."

"Bonus 2 is incremental load. When you pass the incremental flag, the pipeline checks existing Parquet partitions and only processes new periods. This means you do not reprocess 12 months of data when only one new month arrives."

"Bonus 3 is anomaly detection. I compute a 12-month rolling mean and standard deviation per facility per indicator, then flag values where the absolute z-score exceeds 3. The output goes to CSV for programme managers to review. In my test run, 9 anomalies were detected."

"Bonus 4 is metadata drift detection. It compares the current metadata.json to a stored reference snapshot and reports added, removed, or renamed data elements. This catches situations where an indicator definition changes between DHIS2 exports."

---

### SLIDE 10: Pipeline Reliability (1 minute)

**Say this:**

"The pipeline has three layers of reliability."

"First, exit codes. Code 0 is success. Code 1 is a critical data quality failure, like quarantine rate above 10%, zero rows in the fact table, or a contract violation. Code 2 is a runtime error."

"Second, structured logging. Every task logs its start time, row counts, and elapsed time. The summary at the end gives you a full picture of the run."

"Third, checkpointing. After Task 04, I write cleaned data to Parquet and read it back. This breaks the Spark DAG lineage. Without this, Spark tries to hold the entire transformation chain in memory from ingestion through to the fact table write. In constrained environments, that causes out-of-memory errors."

---

### SLIDE 11: Production Recommendations (1 minute)

**Say this:**

"If I were deploying this at PSI, I would add five things."

"First, Prefect or Airflow for orchestration. Scheduling, retries, and Slack alerts when a run fails."

"Second, Delta Lake or Apache Iceberg for the storage layer. This gives you ACID transactions, time travel, and schema evolution."

"Third, Databricks or EMR for compute. You need elastic Spark clusters when you scale from 5 countries to 20."

"Fourth, Great Expectations for richer data quality checks. The YAML contract I built covers basics, but Great Expectations gives you profiling, historical drift tracking, and Slack-integrated alerting."

"Fifth, CI/CD with GitHub Actions. Run pytest on every pull request. Add schema diff checks so you catch breaking changes before they hit production."

---

### SLIDE 12: Closing (30 seconds)

**Say this:**

"To summarize: the pipeline processes 152,000 data values across 5 countries in about 2 minutes. It resolves opaque UIDs, builds a clean star schema, produces programme-ready analytics, and includes four bonus features for production readiness."

"I am happy to walk through any specific section of the code or answer questions about design decisions."

"Thank you."

---

## PART 2: ALL POSSIBLE QUESTIONS AND ANSWERS

### SECTION A: Questions About Your Pipeline

**Q1: Why did you choose PySpark over pandas?**

PySpark handles scale. The assessment generates 152,000 rows across 5 countries, but PSI operates in 40+ countries. With pandas, you would need to rewrite the pipeline when data grows. With PySpark, you just add more executors. PySpark also gives you native Window functions, broadcast joins, and Parquet partitioning out of the box.

**Q2: Why did you not use inferSchema?**

Two reasons. First, inferSchema reads the entire file twice, once to infer types and once to load data. That is wasteful. Second, inferSchema can silently mistype fields. If a column has all integers except one string, Spark casts everything to string. Explicit schemas catch that at load time.

**Q3: Why did you keep orphaned COC rows instead of dropping them?**

The data value itself is still valid. We know the indicator, the facility, the period, and the value. We just do not know the disaggregation, for example whether it is male or female, under-5 or over-5. Dropping the row would lose the aggregate value entirely. Flagging it preserves the data while alerting downstream consumers.

**Q4: Why did you partition by health_area and year_month?**

This matches the most common query pattern. Programme managers at PSI work within a single health area like Malaria or HIV. They filter by health area first, then by time range. Partitioning this way means Spark only reads the relevant partition folders, skipping everything else.

**Q5: Why no surrogate keys?**

DHIS2 UIDs are globally unique 11-character identifiers. They are stable across exports. Adding an auto-increment integer would create a mapping layer that adds complexity without improving join performance. If we were integrating with external systems that do not use DHIS2 UIDs, then surrogate keys would make sense.

**Q6: How does your deduplication handle the case where the correction is wrong?**

It keeps the latest lastUpdated. In DHIS2, the lastUpdated timestamp reflects the most recent edit. If a health worker corrects a value and then corrects it again, the pipeline keeps the final correction. This matches DHIS2's own behavior. If you need an audit trail of all corrections, you would store the near-duplicates in a separate audit table instead of discarding them.

**Q7: Why did you checkpoint after Task 04?**

Spark builds a DAG of transformations and executes them lazily. By Task 05, the DAG includes JSON parsing, explode, three joins, deduplication, type casting, and flagging. In memory-constrained environments, materializing this entire chain causes OOM. Writing to Parquet and reading back starts a fresh DAG. It adds a few seconds but prevents crashes.

**Q8: How would you handle data arriving out of order?**

The incremental load (Bonus B2) checks existing Parquet partitions. If a period is already loaded, it skips it. For late-arriving corrections to already-loaded periods, you would need a merge/upsert strategy. Delta Lake's MERGE INTO handles this natively. Without Delta, you would read the existing partition, union with new data, deduplicate, and overwrite.

**Q9: What happens if a new health area is added?**

Nothing breaks. The pipeline does not hardcode health areas. It reads them from the dataElementGroups field in metadata.json. A new health area creates a new partition folder in the fact table automatically.

**Q10: What happens if the org unit hierarchy adds a fifth level?**

The path parsing logic splits on "/" and reads whatever is there. A fifth-level UID would appear at path_parts[5]. The current code resolves up to four levels. Adding a fifth level would require adding one more self-join. I would refactor the build_hierarchy function to loop dynamically based on the maximum path depth instead of using four fixed joins.

**Q11: Why did you choose z-score > 3 for anomaly detection?**

Three standard deviations captures roughly 99.7% of normally distributed data. Values beyond that threshold are statistically unusual. In health reporting, a facility suddenly reporting 10x its normal volume could indicate a data entry error, a disease outbreak, or a stockout that caused patients to cluster at one facility. All three are worth investigating.

**Q12: How did you handle the z-score denominator being zero?**

If the rolling standard deviation is zero, meaning the facility reported the same value every month, the z-score is undefined. I return NULL instead of dividing by zero. Those rows do not appear in the anomaly output. A constant reporter is not anomalous.

**Q13: What is the difference between your data contract (B1) and Great Expectations?**

My contract validates basic structural constraints: column existence, types, nullable, value ranges, row count. Great Expectations goes further with statistical profiling, distribution checks, referential integrity, and historical drift tracking. For production, I would migrate the YAML contract to Great Expectations and add expectation suites for each table.

**Q14: How does your metadata drift detection work?**

On each run, the pipeline compares the current metadata.json to a stored reference snapshot. It diffs data element UIDs. Same UID with a different name means a rename. UID in current but not reference means an addition. UID in reference but not current means a removal. After comparison, the current metadata becomes the new reference.

**Q15: Why did you not use Prefect for orchestration?**

The assessment PDF explicitly says to use clean Python orchestration with the logging module. I followed that instruction. In production, I would use Prefect or Airflow. I mentioned this in the README under production recommendations.

---

### SECTION B: Questions About DHIS2 and Health Data

**Q16: What is DHIS2?**

DHIS2 is the world's largest health information management system. It is open source, used in 100+ countries, and designed for aggregate health data collection, analysis, and reporting. It is the backbone of routine health information systems in most of sub-Saharan Africa and South/Southeast Asia.

**Q17: What is a dataElement in DHIS2?**

A dataElement is an indicator definition. For example, "Malaria confirmed cases" or "HIV tests administered." Each dataElement has a UID, a name, a valueType (integer, percentage, boolean), and belongs to one or more dataElementGroups (which map to health areas).

**Q18: What is a categoryOptionCombo?**

A categoryOptionCombo represents a disaggregation category. For example, "Under-5 Male" or "15-49 Female." It allows the same data element to be reported across age groups, sex, and facility type. The combination of dataElement + categoryOptionCombo defines the specific value being reported.

**Q19: What is the org unit hierarchy?**

The org unit hierarchy is the geographic structure of the health system. In most countries: Country (Level 1) > Region/Province (Level 2) > District (Level 3) > Facility (Level 4). All data is collected at the facility level. Aggregation up the hierarchy happens at query time.

**Q20: Why does 0 vs NULL matter so much in DHIS2?**

Because programme decisions depend on it. If a district shows low malaria numbers, the programme manager needs to know: is malaria actually low, or did facilities stop reporting? Collapsing zero and null makes this question unanswerable. DHIS2 itself has a zeroIsSignificant flag on each dataElement for this reason.

**Q21: What is the DHIS2 Web API?**

The DHIS2 Web API is a RESTful JSON API that exposes all DHIS2 data and metadata. Endpoints include /api/dataValueSets for data values, /api/metadata for data elements and org units, and /api/analytics for pre-aggregated data. The JSON files in this assessment mirror the Web API response structure.

---

### SECTION C: General Data Engineering Questions

**Q22: Explain the difference between ETL and ELT.**

ETL (Extract, Transform, Load) transforms data before loading it into the target. ELT (Extract, Load, Transform) loads raw data first, then transforms it inside the target system. ELT is more common with cloud data warehouses like BigQuery, Snowflake, and Databricks because the target has enough compute power to handle transformations. My pipeline follows ETL because PySpark does the transformation before writing to Parquet.

**Q23: What is a star schema?**

A star schema is a dimensional model with one fact table at the center surrounded by dimension tables. The fact table contains measures (numeric values you aggregate) and foreign keys to dimensions. Dimensions contain descriptive attributes you filter and group by. It is called a star because the diagram looks like a star.

**Q24: What is the difference between a star schema and a snowflake schema?**

In a star schema, dimensions are fully denormalized. In a snowflake schema, dimensions are normalized into sub-dimensions. For example, a star schema has one dim_org_unit with country, region, district, and facility. A snowflake schema would have separate dim_country, dim_region, dim_district, and dim_facility tables. Star schemas are simpler to query and faster for analytical workloads.

**Q25: What is a broadcast join?**

A broadcast join sends the smaller table to every executor in the Spark cluster. Each executor then performs the join locally without shuffling the larger table across the network. This is efficient when one table is small enough to fit in executor memory. In my pipeline, metadata tables (40 data elements, 21 COCs) are tiny, so broadcast joins make sense.

**Q26: What is a Window function?**

A Window function computes a value for each row based on a set of related rows (the "window"). Unlike GROUP BY, which collapses rows, Window functions keep all rows and add computed columns. Common Window functions include ROW_NUMBER, RANK, LAG, LEAD, and running aggregates like SUM and AVG over a window frame.

**Q27: What is the difference between dropDuplicates and Window-based deduplication?**

dropDuplicates removes rows where all columns (or specified columns) are identical. It does not let you choose which row to keep. Window-based deduplication lets you partition by key columns, order by a tiebreaker (like lastUpdated), and keep the top row. Use dropDuplicates for exact duplicates, Window for near-duplicates where you need to pick a winner.

**Q28: What is Parquet and why use it?**

Parquet is a columnar storage format. It stores data column by column instead of row by row. This is efficient for analytical queries that read a few columns from millions of rows. Parquet also supports predicate pushdown (skipping irrelevant row groups), compression (Snappy, GZIP), and schema embedding. It is the standard format for data lake storage.

**Q29: What is predicate pushdown?**

Predicate pushdown means pushing filter conditions down to the storage layer. Instead of reading all data and then filtering, the engine reads only the data that matches the filter. With Parquet, this works at the row group level. With partitioned Parquet, it works at the partition level. For example, filtering on health_area = "Malaria" skips all non-Malaria partition folders entirely.

**Q30: What is schema-on-read vs schema-on-write?**

Schema-on-write validates data against a schema when writing. If the data does not match, the write fails. Schema-on-read stores raw data and applies a schema when reading. My pipeline uses schema-on-read for ingestion (explicit schemas applied at JSON load time) and schema-on-write for the fact table (data contract validates before downstream use).

**Q31: Explain idempotency in data pipelines.**

An idempotent pipeline produces the same output whether you run it once or ten times. My pipeline achieves this with mode("overwrite") on all Parquet writes. Running the pipeline twice does not create duplicate records. This is important for retry logic. If a pipeline fails halfway and you restart it, idempotency prevents data corruption.

**Q32: What is data lineage?**

Data lineage tracks where data came from, how it was transformed, and where it went. In my pipeline, the lineage is: data_values.json > ingest > resolve metadata > resolve hierarchy > deduplicate > cast and flag > fact table > analytics. Each step logs input/output row counts so you can trace how data flows and where rows are lost.

**Q33: What is Delta Lake and how is it different from Parquet?**

Delta Lake is an open-source storage layer that adds ACID transactions, schema enforcement, time travel, and upsert/merge capabilities to Parquet. Regular Parquet is append-only and has no transaction log. Delta Lake wraps Parquet files with a transaction log that tracks every change. This lets you roll back to a previous version, merge new data with existing data, and enforce schemas on write.

**Q34: How would you monitor this pipeline in production?**

Three layers. First, pipeline-level monitoring: did the run succeed or fail? Use Prefect/Airflow's built-in alerting. Second, data quality monitoring: did the completeness score drop? Did anomalies spike? Use Great Expectations with Slack integration. Third, infrastructure monitoring: is the Spark cluster healthy? Use Datadog or Grafana for CPU, memory, and disk metrics.

**Q35: What is the difference between Prefect and Airflow?**

Both are workflow orchestration tools. Airflow is older, more widely adopted, and uses DAG-based scheduling with a webserver. Prefect is newer, Python-native, and supports dynamic task generation more easily. Airflow requires a separate deployment (scheduler, webserver, database). Prefect has a cloud-hosted option. For PSI's scale, either works. I have experience with Prefect.

**Q36: Explain slowly changing dimensions (SCD).**

An SCD tracks how dimension attributes change over time. Type 1 overwrites the old value (no history). Type 2 adds a new row with start/end dates (full history). Type 3 adds columns for old and new values (limited history). In my pipeline, the org unit hierarchy is treated as Type 1 because I overwrite dimensions on each run. In production, I would use Type 2 to track when a facility moves from one district to another.

**Q37: How do you handle schema evolution?**

Schema evolution means handling changes to the data structure over time. For example, DHIS2 might add a new field to data values. With explicit schemas, a new field is ignored (Spark only reads declared fields). With schema-on-read, you would update the StructType. With Delta Lake, you can use mergeSchema to automatically accept new columns. My Bonus B4 (metadata drift detection) catches changes to data element definitions, which is a form of logical schema evolution.

**Q38: What is the CAP theorem?**

The CAP theorem states that a distributed system can provide at most two of three guarantees: Consistency (every read returns the most recent write), Availability (every request gets a response), and Partition tolerance (the system works despite network splits). Most data lake architectures choose availability and partition tolerance (AP), accepting eventual consistency. Delta Lake adds consistency guarantees on top of an AP storage layer.

**Q39: What is a data lakehouse?**

A data lakehouse combines the best features of data lakes (cheap storage, schema flexibility, diverse file formats) with data warehouses (ACID transactions, schema enforcement, fast SQL queries). Delta Lake, Apache Iceberg, and Apache Hudi are the three main lakehouse formats. They all wrap Parquet with transaction logs and metadata management.

**Q40: How would you test a data pipeline?**

Four levels. Unit tests: test individual functions with small DataFrames (like my contract validation tests). Integration tests: run the full pipeline on synthetic data and verify output shapes. Data quality tests: validate output against contracts and expectations. Regression tests: compare current output to a known-good baseline to catch unintended changes.

---

### SECTION D: Questions About You

**Q41: Why are you interested in this role?**

Three reasons. First, I want to work on data systems that have real public health impact. PSI reaches millions of people across 40+ countries. The data pipelines I build would directly influence programme decisions. Second, the tech stack aligns with my experience: PySpark, SQL, Python, cloud infrastructure. Third, I am building towards a career in data engineering and MLOps, and this role is a strong next step.

**Q42: Walk me through your experience at Cigna Healthcare.**

At Cigna, I built and maintained ETL pipelines for claims data across 6 European markets. I cut data preparation time by 60-70% by automating manual Excel-based workflows. I worked with Python, SQL, and internal data platforms. The key challenge was standardizing data across different national health insurance formats while maintaining compliance with GDPR requirements.

**Q43: What is your experience with PySpark?**

I have used PySpark for data processing in my personal projects and during my AWS Machine Learning Engineer Associate certification prep. I wrote PySpark scripts for feature engineering, data cleaning, and model training pipelines. This assessment is the most comprehensive PySpark project I have built end-to-end.

**Q44: Tell me about Soca Scores.**

Soca Scores is a full-stack MLOps system I am building to predict English Premier League match outcomes. It covers the entire ML lifecycle: data ingestion from footballdata.co.uk, feature engineering in DuckDB and Python, model training with scikit-learn, experiment tracking with MLflow, and a Streamlit dashboard for predictions. I use it as both a portfolio project and a learning vehicle for MLOps tools.

**Q45: What is your experience with cloud platforms?**

I have hands-on experience with AWS (S3, Lambda, SageMaker, Glue) through my AWS Machine Learning Engineer Associate certification prep. At Cigna, I worked with internal cloud infrastructure. I have also used GCP BigQuery for analytics. My target stack includes Terraform for infrastructure as code.

**Q46: How do you handle disagreements with team members?**

I start by understanding the other person's perspective. Usually disagreements happen because we have different context. I share my reasoning, listen to theirs, and look for a solution that addresses both concerns. If we cannot agree, I propose testing both approaches and letting the data decide. In my mentoring work with Everything Data, I have guided mentees through similar situations.

**Q47: Describe a time you worked under a tight deadline.**

This assessment. I started at 1:00 AM and submitted by 4:00 PM the same day. I prioritized ruthlessly: architecture first, then code, then testing, then documentation. I did not try to write perfect code on the first pass. I got a working pipeline, then iterated. The key was knowing which corners to cut (cosmetic code formatting) and which to never cut (data quality logic).

**Q48: What are your salary expectations?**

I am targeting a range of KES 2,500,000 to 3,000,000 per year, depending on the full benefits package and growth opportunities.

---

### SECTION E: Curveball Questions

**Q49: Your pipeline produces 126,681 fact rows from 152,802 input rows. Where did the rest go?**

The difference is 26,121 rows. They were removed at three stages:
- 7,700 rows had ghost dataElement UIDs (unresolvable, removed in Task 02)
- 5,804 rows had ghost orgUnit UIDs (unresolvable, removed in Task 03)
- 10,095 were exact duplicates (removed in Task 04)
- 2,522 were near-duplicates (resolved to latest correction in Task 04)
Total: 7,700 + 5,804 + 10,095 + 2,522 = 26,121. That matches exactly.

**Q50: What would you do differently if you had more time?**

Five things. First, add unit tests for every task module, not just the contract validator. Second, add data profiling at each pipeline stage using something like ydata-profiling. Third, build a simple Streamlit dashboard to visualize the pipeline outputs. Fourth, add SCD Type 2 logic for the org unit dimension. Fifth, set up GitHub Actions CI with automated pytest runs and linting.

**Q51: If a facility's data suddenly shows 10x the normal volume, what would your pipeline do?**

The anomaly detector (Bonus B3) would flag it. The z-score would be well above 3. The row appears in the anomalies CSV with the facility name, indicator, period, actual value, rolling mean, rolling stddev, and z-score. A programme manager would review it and decide whether it is a data entry error, a genuine outbreak, or a catchup report after a period of non-reporting.

**Q52: How would you scale this to 40 countries?**

Three changes. First, move from local Spark to a cluster (Databricks or EMR). Second, partition the input data by country and process in parallel. Third, switch from full reprocessing to incremental loads (Bonus B2 already supports this). The star schema and analytics logic do not change. PySpark handles the scale natively.

**Q53: What if DHIS2 changes its API response format?**

The explicit schemas in Task 01 would cause a load-time error, which is exactly what you want. A silent schema change is worse than a loud failure. The metadata drift detector (Bonus B4) would catch changes to data element definitions. For structural API changes, you would update the StructType schemas and re-test.

**Q54: Can you explain what "no UDFs" means and why it matters?**

A UDF (User Defined Function) is a Python function that Spark calls row by row. Each call requires serializing data from the JVM to Python and back. This is slow. Native Spark SQL functions like F.when, F.lag, F.sum run entirely in the JVM without serialization. My pipeline uses only native functions. This means every transformation can be optimized by the Spark Catalyst query planner.

**Q55: How would you handle real-time streaming data from DHIS2?**

DHIS2 supports webhooks and event-based notifications. I would use Spark Structured Streaming or Apache Kafka to consume events. The pipeline architecture would shift from batch (read JSON files) to micro-batch (read from a Kafka topic). The transformation logic stays the same. The star schema would use Delta Lake's MERGE INTO for upserts instead of overwrite.

---

## PART 3: TIMING GUIDE

Target total presentation time: 12 to 15 minutes, leaving 15 to 20 minutes for Q&A.

| Slide | Topic | Time |
|---|---|---|
| 1 | Title and intro | 0:30 |
| 2 | Pipeline overview | 1:00 |
| 3 | JSON ingestion | 1:30 |
| 4 | UID resolution and hierarchy | 2:00 |
| 5 | 0 vs NULL | 1:30 |
| 6 | Deduplication | 1:00 |
| 7 | Star schema | 1:30 |
| 8 | Analytics and window functions | 1:30 |
| 9 | Bonus challenges | 1:30 |
| 10 | Pipeline reliability | 1:00 |
| 11 | Production recommendations | 1:00 |
| 12 | Closing | 0:30 |
| **Total** | | **14:30** |

---

## PART 4: THINGS TO REMEMBER

1. Speak slowly. You will want to rush. Do not.
2. Make eye contact with each panelist, not just the person who asked the question.
3. When asked a question you do not know, say "I do not know, but here is how I would find out."
4. Have the GitHub repo open on your laptop for code walkthroughs.
5. Have the pipeline terminal output ready to show (the summary log).
6. Drink water before you start. Dry mouth makes you speak faster.
7. If they ask about something you did not build, redirect: "That is a great point. Here is what I would do in production."
8. End every answer. Do not trail off. Finish with a clear final sentence.
