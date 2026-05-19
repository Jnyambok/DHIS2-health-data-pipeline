"""Bonus B1: Data Contract Validation"""
import logging

import yaml
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ContractViolationError(Exception):
    pass


def validate_contract(fact_df: DataFrame, contract_path: str) -> None:
    logger.info("  Bonus B1: Data Contract Validation")

    with open(contract_path, encoding="utf-8") as fh:
        contract = yaml.safe_load(fh)

    violations = []
    actual_columns = set(fact_df.columns)

    for col_spec in contract.get("columns", []):
        name = col_spec["name"]
        nullable = col_spec.get("nullable", True)

        if name not in actual_columns:
            violations.append(f"Missing column: {name}")
            continue

        if not nullable:
            null_count = fact_df.filter(fact_df[name].isNull()).count()
            if null_count > 0:
                violations.append(f"Non-nullable column '{name}' has {null_count} null(s)")

    for constraint in contract.get("constraints", []):
        c_name = constraint["name"]
        expr_str = constraint["expression"]
        try:
            fail_count = fact_df.filter(f"NOT ({expr_str})").count()
            if fail_count > 0:
                violations.append(f"Constraint '{c_name}' violated by {fail_count} row(s): {expr_str}")
        except Exception as exc:
            violations.append(f"Constraint '{c_name}' could not be evaluated: {exc}")

    if violations:
        raise ContractViolationError("; ".join(violations))

    logger.info(f"  Contract '{contract.get('name', contract_path)}' passed all checks")
