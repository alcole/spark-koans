"""
PySpark + Delta Lake Shim
Complete browser-based implementation using pandas

This is the main entry point that combines all shim modules.
"""

import pandas as pd

# Import core classes
from pyspark.core import (
    Row, Column, DataFrame, SparkSession, GroupedData, spark
)

# Import functions
from pyspark.functions import (
    # Column reference
    col, lit,
    # Aggregations
    avg, sum, count, min, max, round,
    # String functions
    upper, lower, initcap, concat, concat_ws,
    length, substring, trim, ltrim, rtrim, lpad, rpad,
    # Conditionals
    when,
    # Arrays
    explode, split,
    # Nulls
    isnull, isnan, coalesce,
)

# Import window functions
from pyspark.window import Window, WindowSpec, row_number, rank, dense_rank, lag, lead

# Note: For koans that need Delta Lake, import delta shim separately
# from delta.core import DeltaTable, _reset_delta_tables

print("✓ PySpark shim loaded successfully")
