"""Tests for Bonus B1: Data Contract Validation"""
import os
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from models.bonus_b1_contract import validate_contract, ContractViolationError


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_contract")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def contract_path():
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "contracts",
        "fact_service_delivery.yaml",
    )


_FACT_SCHEMA = StructType([
    StructField("fact_id",          StringType(),  False),
    StructField("period",           StringType(),  False),
    StructField("year",             IntegerType(), True),
    StructField("month",            IntegerType(), True),
    StructField("quarter",          StringType(),  True),
    StructField("year_month",       StringType(),  True),
    StructField("date_key",         StringType(),  True),
    StructField("org_unit_id",      StringType(),  False),
    StructField("data_element_id",  StringType(),  False),
    StructField("typed_value",      DoubleType(),  True),
    StructField("is_explicit_zero", BooleanType(), True),
    StructField("is_missing_value", BooleanType(), True),
    StructField("is_late_reported", BooleanType(), True),
    StructField("is_outlier",       BooleanType(), True),
    StructField("is_orphaned_coc",  BooleanType(), True),
    StructField("value",            StringType(),  True),
    StructField("coc_id",           StringType(),  True),
    StructField("coc_name",         StringType(),  True),
    StructField("health_area",      StringType(),  True),
    StructField("program_id",       StringType(),  True),
    StructField("facility_name",    StringType(),  True),
    StructField("country_name",     StringType(),  True),
    StructField("region_name",      StringType(),  True),
    StructField("district_name",    StringType(),  True),
    StructField("facility_level",   IntegerType(), True),
    StructField("ou_path",          StringType(),  True),
    StructField("data_element_name", StringType(), True),
    StructField("valueType",        StringType(),  True),
    StructField("domainType",       StringType(),  True),
    StructField("created",          StringType(),  True),
    StructField("last_updated",     StringType(),  True),
    StructField("storedBy",         StringType(),  True),
])


def _make_valid_row():
    return {
        "fact_id": "abc123", "period": "202301",
        "year": 2023, "month": 1, "quarter": "Q1", "year_month": "2023-01",
        "date_key": "2023-01", "org_unit_id": "OU_KE_F1",
        "data_element_id": "DE_CD4", "typed_value": 430.0,
        "is_explicit_zero": False, "is_missing_value": False,
        "is_late_reported": False, "is_outlier": False, "is_orphaned_coc": False,
        "value": "430", "coc_id": "COC_DEF", "coc_name": "Default",
        "health_area": "HIV", "program_id": "PROG_HIV_KE",
        "facility_name": "Kenyatta National Hospital", "country_name": "Kenya",
        "region_name": "Nairobi Region", "district_name": "Nairobi North District",
        "facility_level": 4, "ou_path": "/OU_KE/OU_KE_R1/OU_KE_D1/OU_KE_F1",
        "data_element_name": "CD4 Count", "valueType": "INTEGER_ZERO_OR_POSITIVE",
        "domainType": "AGGREGATE", "created": "2023-02-05",
        "last_updated": "2023-02-05", "storedBy": "admin",
    }


def test_valid_fact_passes(spark, contract_path):
    df = spark.createDataFrame([_make_valid_row()], schema=_FACT_SCHEMA)
    validate_contract(df, contract_path)  # should not raise


def test_null_fact_id_fails(spark, contract_path):
    row = _make_valid_row()
    row["fact_id"] = None
    df = spark.createDataFrame([row], schema=_FACT_SCHEMA)
    with pytest.raises(ContractViolationError, match="fact_id"):
        validate_contract(df, contract_path)


def test_null_period_fails(spark, contract_path):
    row = _make_valid_row()
    row["period"] = None
    df = spark.createDataFrame([row], schema=_FACT_SCHEMA)
    with pytest.raises(ContractViolationError, match="period"):
        validate_contract(df, contract_path)


def test_negative_typed_value_fails(spark, contract_path):
    row = _make_valid_row()
    row["typed_value"] = -1.0
    df = spark.createDataFrame([row], schema=_FACT_SCHEMA)
    with pytest.raises(ContractViolationError, match="typed_value_non_negative"):
        validate_contract(df, contract_path)


def test_null_typed_value_passes(spark, contract_path):
    row = _make_valid_row()
    row["typed_value"] = None
    df = spark.createDataFrame([row], schema=_FACT_SCHEMA)
    validate_contract(df, contract_path)  # NULL typed_value is allowed (missing value rows)


def test_invalid_period_format_fails(spark, contract_path):
    row = _make_valid_row()
    row["period"] = "2023-01"  # wrong format, should be yyyyMM
    df = spark.createDataFrame([row], schema=_FACT_SCHEMA)
    with pytest.raises(ContractViolationError, match="period_format"):
        validate_contract(df, contract_path)
