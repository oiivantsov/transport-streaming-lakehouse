from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def check_not_empty(df: DataFrame, table_name: str):
    # Ensure DataFrame is not empty
    if df.count() == 0:
        raise ValueError(f"QC FAILED: {table_name} is empty")

def check_columns_exist(df: DataFrame, expected_columns: list, table_name: str):
    # Ensure required columns exist
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise ValueError(f"QC FAILED: {table_name} missing columns: {missing}")

def check_no_nulls_in_column(df: DataFrame, column: str, table_name: str):
    # Ensure column has no null or empty values
    if df.filter(F.col(column).isNull() | (F.col(column) == "")).count() > 0:
        raise ValueError(f"QC FAILED: {table_name} has null/empty values in {column}")

def check_no_duplicates(df: DataFrame, key_columns: list, table_name: str):
    # Ensure no duplicate rows based on business key
    dup_count = df.groupBy(*key_columns).count().filter("count > 1").count()
    if dup_count > 0:
        raise ValueError(f"QC FAILED: {table_name} has {dup_count} duplicate keys: {key_columns}")

def check_values_in_set(df: DataFrame, column: str, allowed_values: list, table_name: str):
    # Ensure column values are within allowed set
    invalid_count = df.filter(~F.col(column).isin(allowed_values)).count()
    if invalid_count > 0:
        raise ValueError(f"QC FAILED: {table_name} has {invalid_count} invalid values in {column}")

def check_active_unique(df: DataFrame, key_columns: list, table_name: str):
    # Ensure active records are unique per business key
    active_dup_count = df.filter("active_flg = 1") \
        .groupBy(*key_columns).count().filter("count > 1").count()
    if active_dup_count > 0:
        raise ValueError(f"QC FAILED: {table_name} has {active_dup_count} duplicate active records")
