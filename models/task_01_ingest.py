"""Task 01: JSON Ingestion & Schema Flattening"""
import json
import logging
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)

logger = logging.getLogger(__name__)

_PERIOD_RE = re.compile(r"^\d{6}$")

_DV_SCHEMA = StructType([
    StructField("dataElement",          StringType(), True),
    StructField("period",               StringType(), True),
    StructField("orgUnit",              StringType(), True),
    StructField("categoryOptionCombo",  StringType(), True),
    StructField("value",                StringType(), True),   # kept as STRING — 0 vs NULL matters
    StructField("created",              StringType(), True),
    StructField("lastUpdated",          StringType(), True),
    StructField("storedBy",             StringType(), True),
])

_Q_SCHEMA = StructType([
    StructField("dataElement",          StringType(), True),
    StructField("period",               StringType(), True),
    StructField("orgUnit",              StringType(), True),
    StructField("categoryOptionCombo",  StringType(), True),
    StructField("raw_value",            StringType(), True),
    StructField("quarantine_reason",    StringType(), True),
])

_DE_SCHEMA = StructType([
    StructField("id",           StringType(), True),
    StructField("name",         StringType(), True),
    StructField("valueType",    StringType(), True),
    StructField("domainType",   StringType(), True),
    StructField("categoryComboId", StringType(), True),
])

_OU_SCHEMA = StructType([
    StructField("id",       StringType(), True),
    StructField("name",     StringType(), True),
    StructField("level",    IntegerType(), True),
    StructField("path",     StringType(), True),
    StructField("parentId", StringType(), True),
])

_COC_SCHEMA = StructType([
    StructField("id",   StringType(), True),
    StructField("name", StringType(), True),
])

_PROG_SCHEMA = StructType([
    StructField("id",                   StringType(), True),
    StructField("name",                 StringType(), True),
    StructField("healthArea",           StringType(), True),
    StructField("country",              StringType(), True),
    StructField("reportingFrequency",   StringType(), True),
])

_DE_PROG_SCHEMA = StructType([
    StructField("de_id",        StringType(), True),
    StructField("program_id",   StringType(), True),
    StructField("health_area",  StringType(), True),
    StructField("country",      StringType(), True),
])


def _make_df(spark, rows, schema):
    if rows:
        return spark.createDataFrame(rows, schema)
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def load_and_flatten(spark: SparkSession, data_dir: str) -> dict:
    logger.info("  Task 01: JSON Ingestion & Schema Flattening")

    # Load each of the 4 DHIS2 Web API export files separately
    def _read(fname):
        path = os.path.join(data_dir, fname)
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    meta_doc   = _read("metadata.json")
    ou_doc     = _read("org_units.json")
    prog_doc   = _read("programs.json")
    dv_doc     = _read("data_values.json")

    raw_dvs    = dv_doc.get("dataValues", [])
    de_list    = meta_doc.get("dataElements", [])
    coc_list   = meta_doc.get("categoryOptionCombos", [])
    ou_list    = ou_doc.get("organisationUnits", [])
    prog_list  = prog_doc.get("programs", [])

    logger.info(f"  Raw data values: {len(raw_dvs)}, DEs: {len(de_list)}, OUs: {len(ou_list)}, Programs: {len(prog_list)}")

    # Quarantine only rows with structural invalids:
    # missing orgUnit, missing period, or period not matching yyyyMM
    valid, quarantine = [], []

    for row in raw_dvs:
        reasons = []
        if not row.get("orgUnit"):
            reasons.append("missing:orgUnit")
        if not row.get("period"):
            reasons.append("missing:period")
        elif not _PERIOD_RE.match(str(row["period"])):
            reasons.append(f"invalid_period:{row['period']}")
        if not row.get("dataElement"):
            reasons.append("missing:dataElement")

        if reasons:
            quarantine.append({
                "dataElement":         row.get("dataElement"),
                "period":              row.get("period"),
                "orgUnit":             row.get("orgUnit"),
                "categoryOptionCombo": row.get("categoryOptionCombo"),
                "raw_value":           str(row.get("value", "")),
                "quarantine_reason":   "|".join(reasons),
            })
        else:
            raw_val = row.get("value")
            valid.append({
                "dataElement":         row["dataElement"],
                "period":              str(row["period"]),
                "orgUnit":             row["orgUnit"],
                "categoryOptionCombo": row.get("categoryOptionCombo", "COC_DEF"),
                "value":               str(raw_val) if raw_val is not None else None,
                "created":             row.get("created"),
                "lastUpdated":         row.get("lastUpdated"),
                "storedBy":            row.get("storedBy"),
            })

    logger.info(f"  Valid: {len(valid)}, Quarantined: {len(quarantine)}")

    # Build data element rows
    de_rows = [
        {
            "id":             de["id"],
            "name":           de.get("name", de["id"]),
            "valueType":      de.get("valueType", "NUMBER"),
            "domainType":     de.get("domainType", "AGGREGATE"),
            "categoryComboId": de.get("categoryCombo", {}).get("id"),
        }
        for de in de_list
    ]

    # Build org unit rows — extract parentId from path
    ou_rows = []
    for ou in ou_list:
        path     = ou.get("path", f"/{ou['id']}")
        parts    = [p for p in path.split("/") if p]
        parent_field = ou.get("parent", {})
        parent_id    = parent_field.get("id") if parent_field else None
        if parent_id is None and len(parts) >= 2:
            parent_id = parts[-2]
        ou_rows.append({
            "id":       ou["id"],
            "name":     ou.get("name", ou["id"]),
            "level":    ou.get("level", len(parts)),
            "path":     path,
            "parentId": parent_id,
        })

    coc_rows = [{"id": c["id"], "name": c.get("name", c["id"])} for c in coc_list]

    prog_rows = [
        {
            "id":                 p["id"],
            "name":               p.get("name", p["id"]),
            "healthArea":         p.get("healthArea"),
            "country":            p.get("country"),
            "reportingFrequency": p.get("reportingFrequency"),
        }
        for p in prog_list
    ]

    # Build DE→program→healthArea mapping (used for star schema partitioning)
    de_prog_rows = []
    for p in prog_list:
        for de_id in p.get("dataElements", []):
            de_prog_rows.append({
                "de_id":       de_id,
                "program_id":  p["id"],
                "health_area": p.get("healthArea"),
                "country":     p.get("country"),
            })

    return {
        "data_values":           _make_df(spark, valid,        _DV_SCHEMA),
        "quarantine":            _make_df(spark, quarantine,   _Q_SCHEMA),
        "data_elements":         _make_df(spark, de_rows,      _DE_SCHEMA),
        "org_units":             _make_df(spark, ou_rows,      _OU_SCHEMA),
        "category_option_combos": _make_df(spark, coc_rows,   _COC_SCHEMA),
        "programs":              _make_df(spark, prog_rows,    _PROG_SCHEMA),
        "de_program":            _make_df(spark, de_prog_rows, _DE_PROG_SCHEMA),
    }
