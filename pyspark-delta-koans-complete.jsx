import React, { useState, useEffect, useRef } from 'react';

// ============================================================
// KOAN DEFINITIONS
// ============================================================

const KOANS = [
  // ==================== BASICS ====================
  {
    id: 1,
    title: "Creating a DataFrame",
    category: "Basics",
    description: "Create a DataFrame from a list of tuples. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
`,
    template: `# Create a DataFrame from the data and columns
df = spark.___(___, ___)

# The DataFrame should have 3 rows
assert df.count() == 3, f"Expected 3 rows, got {df.count()}"
print("✓ DataFrame created with correct row count")

# The DataFrame should have 2 columns
assert len(df.columns) == 2, f"Expected 2 columns, got {len(df.columns)}"
print("✓ DataFrame has correct number of columns")

print("\\n🎉 Koan complete! You've learned to create a DataFrame.")`,
    solution: `df = spark.createDataFrame(data, columns)`,
    hints: [
      "DataFrames are created from SparkSession",
      "The method name describes what you're doing: create + DataFrame",
      "Pass the data first, then the column names"
    ]
  },
  {
    id: 2,
    title: "Selecting Columns",
    category: "Basics",
    description: "Select specific columns from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34, "NYC"), ("Bob", 45, "LA"), ("Charlie", 29, "Chicago")]
df = spark.createDataFrame(data, ["name", "age", "city"])
`,
    template: `# Select only the 'name' and 'city' columns
result = df.___("name", "___")

# Result should have exactly 2 columns
assert len(result.columns) == 2, f"Expected 2 columns, got {len(result.columns)}"
print("✓ Correct number of columns selected")

# Result should contain 'name' and 'city'
assert "name" in result.columns, "Missing 'name' column"
assert "city" in result.columns, "Missing 'city' column"
print("✓ Correct columns selected")

print("\\n🎉 Koan complete! You've learned to select columns.")`,
    solution: `result = df.select("name", "city")`,
    hints: [
      "Think about what action you want: you want to 'select' columns",
      "The method takes column names as strings"
    ]
  },
  {
    id: 3,
    title: "Filtering Rows",
    category: "Basics",
    description: "Filter rows based on a condition. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 52)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Filter to only include people over 35
from pyspark.sql.functions import col

result = df.___(col("age") ___ 35)

# Should have 2 people over 35
assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
print("✓ Correct number of rows filtered")

# Collect and verify
rows = result.collect()
ages = [row["age"] for row in rows]
assert all(age > 35 for age in ages), "Some ages are not > 35"
print("✓ All remaining rows have age > 35")

print("\\n🎉 Koan complete! You've learned to filter rows.")`,
    solution: `result = df.filter(col("age") > 35)`,
    hints: [
      "You want to 'filter' the DataFrame",
      "Use a comparison operator to check if age is greater than 35",
      "The col() function references a column by name"
    ]
  },
  {
    id: 4,
    title: "Adding Columns",
    category: "Basics",
    description: "Add a new calculated column to a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Add a new column 'age_in_months' that multiplies age by 12
from pyspark.sql.functions import col

result = df.___("age_in_months", col("___") * 12)

# Should still have 3 rows
assert result.count() == 3
print("✓ Row count unchanged")

# Should now have 3 columns
assert len(result.columns) == 3, f"Expected 3 columns, got {len(result.columns)}"
print("✓ New column added")

# Check calculation is correct
first_row = result.filter(col("name") == "Alice").collect()[0]
assert first_row["age_in_months"] == 408, f"Expected 408, got {first_row['age_in_months']}"
print("✓ Calculation is correct (34 * 12 = 408)")

print("\\n🎉 Koan complete! You've learned to add columns.")`,
    solution: `result = df.withColumn("age_in_months", col("age") * 12)`,
    hints: [
      "The method name suggests adding 'with' a new 'Column'",
      "First argument is the new column name, second is the expression",
      "Reference the 'age' column to multiply it"
    ]
  },
  {
    id: 5,
    title: "Grouping and Aggregating",
    category: "Basics",
    description: "Group data and calculate aggregates. Replace ___ with the correct code.",
    setup: `
data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
`,
    template: `# Group by department and calculate average salary
from pyspark.sql.functions import avg, round

result = df.___("department").agg(
    round(___("salary"), 2).alias("avg_salary")
)

# Should have 2 departments
assert result.count() == 2, f"Expected 2 groups, got {result.count()}"
print("✓ Correct number of groups")

# Check Engineering average (6000 + 6500 + 5500) / 3 = 6000
eng_row = result.filter(col("department") == "Engineering").collect()[0]
assert eng_row["avg_salary"] == 6000.0, f"Expected 6000.0, got {eng_row['avg_salary']}"
print("✓ Engineering average salary is correct")

print("\\n🎉 Koan complete! You've learned to group and aggregate.")`,
    solution: `result = df.groupBy("department").agg(round(avg("salary"), 2).alias("avg_salary"))`,
    hints: [
      "First you need to group the data using 'groupBy'",
      "Then aggregate using 'avg' function for average",
      "The avg function takes a column name"
    ]
  },
  {
    id: 6,
    title: "Joining DataFrames",
    category: "Basics",
    description: "Join two DataFrames to combine related data. Replace ___ with the correct code.",
    setup: `
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Sales"),
    (103, "Marketing")
], ["dept_id", "dept_name"])
`,
    template: `# Join employees with departments on dept_id
result = employees.___(departments, ___, "inner")

# Should have 3 rows (all employees have matching departments)
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Correct number of joined rows")

# Should have columns from both DataFrames
assert "name" in result.columns, "Missing 'name' column"
assert "dept_name" in result.columns, "Missing 'dept_name' column"
print("✓ Columns from both DataFrames present")

# Alice should be in Engineering
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["dept_name"] == "Engineering", f"Expected Engineering, got {alice['dept_name']}"
print("✓ Join matched correctly")

print("\\n🎉 Koan complete! You've learned to join DataFrames.")`,
    solution: `result = employees.join(departments, "dept_id", "inner")`,
    hints: [
      "The method to combine DataFrames is called 'join'",
      "Specify the column to join on as a string",
      "The join type is already provided: 'inner'"
    ]
  },
  {
    id: 7,
    title: "Window Functions",
    category: "Basics",
    description: "Use window functions to calculate running totals. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 200),
    ("2024-01-04", 175)
]
df = spark.createDataFrame(data, ["date", "sales"])
`,
    template: `# Create a window that orders by date and includes all previous rows
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.___)

# Add running total column
result = df.withColumn("running_total", ___("sales").over(window_spec))

# Check the running totals
rows = result.orderBy("date").collect()

assert rows[0]["running_total"] == 100, "Day 1 should be 100"
print("✓ Day 1: 100")

assert rows[1]["running_total"] == 250, "Day 2 should be 250 (100+150)"
print("✓ Day 2: 250")

assert rows[3]["running_total"] == 625, "Day 4 should be 625"
print("✓ Day 4: 625 (cumulative)")

print("\\n🎉 Koan complete! You've learned window functions.")`,
    solution: `window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)\nresult = df.withColumn("running_total", spark_sum("sales").over(window_spec))`,
    hints: [
      "For a running total, you want from the start up to the 'currentRow'",
      "Use spark_sum (aliased from sum) to add up values",
      "The .over() method applies the function to the window"
    ]
  },

  // ==================== DELTA LAKE ====================
  {
    id: 101,
    title: "Creating a Delta Table",
    category: "Delta Lake",
    description: "Create a Delta table and write data to it. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()  # Clean slate

data = [("Alice", 34, "Engineering"), ("Bob", 45, "Sales"), ("Charlie", 29, "Engineering")]
df = spark.createDataFrame(data, ["name", "age", "department"])
`,
    template: `# Write the DataFrame as a Delta table
df.write.___("delta").mode("overwrite").save("/data/employees")

# Read it back
result = spark.read.format("___").load("/data/employees")

assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Delta table created and read successfully")

# Verify it's a Delta table
assert DeltaTable.___(spark, "/data/employees"), "Should be a Delta table"
print("✓ Confirmed as Delta table")

print("\\n🎉 Koan complete! You've created your first Delta table.")`,
    solution: `df.write.format("delta").mode("overwrite").save("/data/employees")\nresult = spark.read.format("delta").load("/data/employees")\nassert DeltaTable.isDeltaTable(spark, "/data/employees")`,
    hints: [
      "Use .format() to specify 'delta' as the format",
      "Reading uses the same format specification",
      "isDeltaTable() checks if a path contains a Delta table"
    ]
  },
  {
    id: 102,
    title: "Time Travel - Version",
    category: "Delta Lake",
    description: "Query a previous version of a Delta table. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create initial data (version 0)
data_v0 = [("Alice", 100), ("Bob", 200)]
df_v0 = spark.createDataFrame(data_v0, ["name", "balance"])
df_v0.write.format("delta").save("/data/accounts")

# Update data (version 1)
data_v1 = [("Alice", 150), ("Bob", 250), ("Charlie", 300)]
df_v1 = spark.createDataFrame(data_v1, ["name", "balance"])
df_v1.write.format("delta").mode("overwrite").save("/data/accounts")
`,
    template: `# Read the current version
current = spark.read.format("delta").load("/data/accounts")
assert current.count() == 3, "Current version should have 3 rows"
print(f"✓ Current version has {current.count()} rows")

# Read version 0 using time travel
historical = spark.read.format("delta").option("___", 0).load("/data/accounts")

assert historical.count() == 2, f"Version 0 should have 2 rows, got {historical.count()}"
print(f"✓ Version 0 has {historical.count()} rows")

# Check that Charlie wasn't in version 0
names_v0 = [row["name"] for row in historical.collect()]
assert "Charlie" not in names_v0, "Charlie should not be in version 0"
print("✓ Charlie correctly absent from version 0")

print("\\n🎉 Koan complete! You've mastered time travel queries.")`,
    solution: `historical = spark.read.format("delta").option("versionAsOf", 0).load("/data/accounts")`,
    hints: [
      "Use .option() to specify time travel parameters",
      "The option for version-based travel is 'versionAsOf'",
      "Version numbers start at 0"
    ]
  },
  {
    id: 103,
    title: "MERGE - Upsert Pattern",
    category: "Delta Lake",
    description: "Use MERGE to update existing rows and insert new ones. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create target table
target_data = [("Alice", 100), ("Bob", 200)]
target_df = spark.createDataFrame(target_data, ["name", "balance"])
target_df.write.format("delta").save("/data/accounts")

# Source data with updates and new records
source_data = [("Alice", 150), ("Charlie", 300)]  # Alice updated, Charlie is new
source_df = spark.createDataFrame(source_data, ["name", "balance"])
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Merge source into target
# When matched: update the balance
# When not matched: insert the new record
dt.___(
    source_df,
    "target.name = source.name"
).whenMatched___().whenNotMatched___().execute()

# Verify results
result = dt.toDF()
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Correct row count after merge")

# Check Alice was updated
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["balance"] == 150, f"Alice balance should be 150, got {alice['balance']}"
print("✓ Alice's balance updated to 150")

# Check Charlie was inserted
charlie = result.filter(col("name") == "Charlie").collect()[0]
assert charlie["balance"] == 300, f"Charlie balance should be 300"
print("✓ Charlie inserted with balance 300")

print("\\n🎉 Koan complete! You've mastered the MERGE upsert pattern.")`,
    solution: `dt.merge(source_df, "target.name = source.name").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,
    hints: [
      "Start with .merge(source, condition)",
      "Use whenMatchedUpdateAll() to update all columns",
      "Use whenNotMatchedInsertAll() to insert all columns",
      "Don't forget .execute() at the end"
    ]
  },
  {
    id: 104,
    title: "MERGE - Selective Update",
    category: "Delta Lake",
    description: "Use MERGE with specific column updates. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create target table with more columns
target_data = [("Alice", 100, "2024-01-01"), ("Bob", 200, "2024-01-01")]
target_df = spark.createDataFrame(target_data, ["name", "balance", "last_updated"])
target_df.write.format("delta").save("/data/accounts")

# Source only has name and new balance
source_data = [("Alice", 500)]
source_df = spark.createDataFrame(source_data, ["name", "new_balance"])
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Merge with specific column mapping
dt.merge(
    source_df,
    "target.name = source.name"
).whenMatchedUpdate(set={
    "___": "source.new_balance",
    "last_updated": "'2024-06-01'"
}).execute()

# Verify Alice's balance was updated
result = dt.toDF()
alice = result.filter(col("name") == "Alice").collect()[0]

assert alice["balance"] == 500, f"Expected balance 500, got {alice['balance']}"
print("✓ Alice's balance updated to 500")

# Verify Bob was not changed
bob = result.filter(col("name") == "Bob").collect()[0]
assert bob["balance"] == 200, f"Bob should still have 200, got {bob['balance']}"
print("✓ Bob's balance unchanged")

print("\\n🎉 Koan complete! You've learned selective MERGE updates.")`,
    solution: `dt.merge(source_df, "target.name = source.name").whenMatchedUpdate(set={"balance": "source.new_balance", "last_updated": "'2024-06-01'"}).execute()`,
    hints: [
      "whenMatchedUpdate takes a 'set' parameter with column mappings",
      "Map target column names to source expressions",
      "The target column to update is 'balance'"
    ]
  },
  {
    id: 105,
    title: "Table History",
    category: "Delta Lake",
    description: "View the history of operations on a Delta table. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create and modify table multiple times
data = [("Alice", 100)]
df = spark.createDataFrame(data, ["name", "balance"])
df.write.format("delta").save("/data/accounts")

# Make some updates
data2 = [("Alice", 100), ("Bob", 200)]
df2 = spark.createDataFrame(data2, ["name", "balance"])
df2.write.format("delta").mode("overwrite").save("/data/accounts")
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Get the full history
history_df = dt.___()

# History should show our operations
history_rows = history_df.collect()
assert len(history_rows) >= 2, f"Expected at least 2 history entries"
print(f"✓ Found {len(history_rows)} history entries")

# Check version numbers exist
versions = [row["version"] for row in history_rows]
assert 0 in versions, "Should have version 0"
print("✓ Version 0 present in history")

# Check operations are recorded
operations = [row["operation"] for row in history_rows]
print(f"✓ Operations recorded: {operations}")

print("\\n🎉 Koan complete! You can now audit Delta table changes.")`,
    solution: `history_df = dt.history()`,
    hints: [
      "The method to get history is simply .history()",
      "It returns a DataFrame with version info"
    ]
  },
  {
    id: 106,
    title: "OPTIMIZE and Z-ORDER",
    category: "Delta Lake",
    description: "Optimize a Delta table for better query performance. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create table with data
data = [(i, f"user_{i}", i % 10) for i in range(100)]
df = spark.createDataFrame(data, ["id", "name", "category"])
df.write.format("delta").save("/data/users")
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/users")

# Optimize the table (compacts small files)
dt.___()
print("✓ Table optimized (files compacted)")

# Check history shows the optimize operation
history = dt.history(1).collect()[0]
assert history["operation"] == "OPTIMIZE", f"Expected OPTIMIZE, got {history['operation']}"
print("✓ OPTIMIZE recorded in history")

# Optimize with Z-ORDER for faster queries on specific columns
dt.optimize().___("category")
print("✓ Z-ORDER optimization applied on 'category' column")

print("\\n🎉 Koan complete! You've learned table optimization.")`,
    solution: `dt.optimize()\ndt.optimize().zorderBy("category")`,
    hints: [
      "Use .optimize() to compact files",
      "Chain .zorderBy() for co-locating related data",
      "Z-ORDER improves query performance on filtered columns"
    ]
  },
  {
    id: 107,
    title: "Delete with Condition",
    category: "Delta Lake",
    description: "Delete rows from a Delta table based on a condition. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

data = [("Alice", 100, True), ("Bob", 200, False), ("Charlie", 150, True)]
df = spark.createDataFrame(data, ["name", "balance", "is_active"])
df.write.format("delta").save("/data/accounts")
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Delete all inactive accounts
dt.___(condition="is_active == False")

# Verify Bob was deleted
result = dt.toDF()
names = [row["name"] for row in result.collect()]

assert "Bob" not in names, "Bob should be deleted"
print("✓ Inactive account (Bob) deleted")

assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
print("✓ Only active accounts remain")

print("\\n🎉 Koan complete! You've learned Delta delete operations.")`,
    solution: `dt.delete(condition="is_active == False")`,
    hints: [
      "Use .delete() with a condition string",
      "The condition filters which rows to delete"
    ]
  },
  {
    id: 108,
    title: "Update with Condition",
    category: "Delta Lake",
    description: "Update rows in a Delta table based on a condition. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

data = [("Alice", 100, "basic"), ("Bob", 200, "premium"), ("Charlie", 50, "basic")]
df = spark.createDataFrame(data, ["name", "balance", "tier"])
df.write.format("delta").save("/data/accounts")
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Give all premium users a bonus: set their balance to balance + 100
dt.___(
    condition="tier == 'premium'",
    set_values={"___": 300}  # Bob's new balance
)

# Verify Bob got the bonus
result = dt.toDF()
bob = result.filter(col("name") == "Bob").collect()[0]

assert bob["balance"] == 300, f"Bob should have 300, got {bob['balance']}"
print("✓ Premium user (Bob) balance updated")

# Verify others unchanged
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["balance"] == 100, f"Alice should still have 100"
print("✓ Basic users unchanged")

print("\\n🎉 Koan complete! You've learned Delta update operations.")`,
    solution: `dt.update(condition="tier == 'premium'", set_values={"balance": 300})`,
    hints: [
      "Use .update() with condition and set_values",
      "set_values is a dict mapping column names to new values",
      "The column to update is 'balance'"
    ]
  },
  {
    id: 109,
    title: "Create Table with Builder",
    category: "Delta Lake",
    description: "Create a Delta table with an explicit schema using the builder API. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()
`,
    template: `# Create a Delta table with explicit schema
DeltaTable._____(spark) \\
    .tableName("products") \\
    .addColumn("id", "INT") \\
    .addColumn("___", "STRING") \\
    .addColumn("price", "DOUBLE") \\
    .execute()

# Verify table was created
assert DeltaTable.isDeltaTable(spark, "products"), "Table should exist"
print("✓ Table 'products' created")

# Get the table and verify schema
dt = DeltaTable.forPath(spark, "products")
df = dt.toDF()

assert "name" in df.columns, "Should have 'name' column"
assert "price" in df.columns, "Should have 'price' column"
print("✓ Table has correct columns")

assert df.count() == 0, "New table should be empty"
print("✓ Table is empty (ready for data)")

print("\\n🎉 Koan complete! You've learned the DeltaTable builder.")`,
    solution: `DeltaTable.create(spark).tableName("products").addColumn("id", "INT").addColumn("name", "STRING").addColumn("price", "DOUBLE").execute()`,
    hints: [
      "Use DeltaTable.create(spark) to start the builder",
      "The missing column name is 'name'",
      "Don't forget .execute() at the end"
    ]
  },
  {
    id: 110,
    title: "VACUUM Old Files",
    category: "Delta Lake",
    description: "Clean up old files from a Delta table. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create table and make several updates
for i in range(3):
    data = [(j, f"version_{i}") for j in range(10)]
    df = spark.createDataFrame(data, ["id", "data"])
    df.write.format("delta").mode("overwrite").save("/data/versions")
`,
    template: `# Get the Delta table  
dt = DeltaTable.forPath(spark, "/data/versions")

# Check we have multiple versions
history = dt.history()
version_count = history.count()
print(f"Table has {version_count} versions")

# Vacuum to remove old files
# Default retention is 168 hours (7 days)
result = dt.___(retention_hours=168)

print("✓ Vacuum completed")

# Note: After vacuum, time travel to old versions may not work!
# This is a trade-off between storage and history access
print("\\n⚠️  Warning: VACUUM removes old version files!")
print("   Time travel to vacuumed versions will fail.")

print("\\n🎉 Koan complete! You understand VACUUM and retention.")`,
    solution: `result = dt.vacuum(retention_hours=168)`,
    hints: [
      "Use .vacuum() to clean up old files",
      "retention_hours specifies the minimum age of files to keep",
      "Be careful: this breaks time travel to old versions!"
    ]
  }
];

// ============================================================
// PYSPARK + DELTA LAKE SHIM
// ============================================================

const FULL_SHIM = `
import pandas as pd
from typing import List, Any, Optional, Union, Dict
from dataclasses import dataclass
from datetime import datetime
import copy

# ============ DELTA TABLE STORAGE ============
_delta_tables = {}

def _reset_delta_tables():
    global _delta_tables
    _delta_tables = {}

class DeltaTableData:
    def __init__(self, path, initial_df=None):
        self.path = path
        self.versions = []
        self.history_log = []
        self.properties = {}
        self.schema = []
        if initial_df is not None:
            self._add_version(initial_df, "CREATE TABLE")
    
    def _add_version(self, df, operation, metrics=None):
        self.versions.append(df.copy())
        self.history_log.append({
            "version": len(self.versions) - 1,
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "operationMetrics": metrics or {}
        })
    
    @property
    def current_version(self):
        return len(self.versions) - 1
    
    def get_df(self, version=None):
        if version is None:
            version = self.current_version
        if version < 0 or version > self.current_version:
            raise ValueError(f"Version {version} not found")
        return self.versions[version].copy()

# ============ ROW CLASS ============
class Row(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self.__dict__.update(kwargs)
    
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'Row' object has no attribute '{name}'")
    
    def asDict(self):
        return dict(self)

# ============ COLUMN CLASS ============
class Column:
    def __init__(self, name, expr=None):
        self.name = name
        self.expr = expr or name
        self._alias = None
    
    def __gt__(self, other): return Column(self.name, f"({self.expr}) > {repr(other)}")
    def __lt__(self, other): return Column(self.name, f"({self.expr}) < {repr(other)}")
    def __ge__(self, other): return Column(self.name, f"({self.expr}) >= {repr(other)}")
    def __le__(self, other): return Column(self.name, f"({self.expr}) <= {repr(other)}")
    def __eq__(self, other):
        if other is None: return Column(self.name, f"({self.expr}).isna()")
        return Column(self.name, f"({self.expr}) == {repr(other)}")
    def __ne__(self, other):
        if other is None: return Column(self.name, f"({self.expr}).notna()")
        return Column(self.name, f"({self.expr}) != {repr(other)}")
    def __mul__(self, other):
        if isinstance(other, Column): return Column(self.name, f"({self.expr}) * ({other.expr})")
        return Column(self.name, f"({self.expr}) * {repr(other)}")
    def __add__(self, other):
        if isinstance(other, Column): return Column(self.name, f"({self.expr}) + ({other.expr})")
        return Column(self.name, f"({self.expr}) + {repr(other)}")
    def __sub__(self, other):
        if isinstance(other, Column): return Column(self.name, f"({self.expr}) - ({other.expr})")
        return Column(self.name, f"({self.expr}) - {repr(other)}")
    def __and__(self, other):
        if isinstance(other, Column): return Column(self.name, f"({self.expr}) & ({other.expr})")
        return Column(self.name, f"({self.expr}) & {repr(other)}")
    def __or__(self, other):
        if isinstance(other, Column): return Column(self.name, f"({self.expr}) | ({other.expr})")
        return Column(self.name, f"({self.expr}) | {repr(other)}")
    
    def alias(self, name):
        new_col = Column(self.name, self.expr)
        new_col._alias = name
        for attr in ['_agg_func', '_source_col', '_round_decimals', '_transform_func', '_is_window_func', '_window']:
            if hasattr(self, attr): setattr(new_col, attr, getattr(self, attr))
        return new_col
    
    def over(self, window):
        new_col = Column(self.name, self.expr)
        new_col._window = window
        new_col._is_window_func = True
        for attr in ['_agg_func', '_source_col', '_window_func', '_window_args']:
            if hasattr(self, attr): setattr(new_col, attr, getattr(self, attr))
        return new_col
    
    def isNull(self): return Column(self.name, f"({self.expr}).isna()")
    def isNotNull(self): return Column(self.name, f"({self.expr}).notna()")
    def desc(self):
        new_col = Column(self.name, self.expr)
        new_col._sort_desc = True
        return new_col

# ============ WINDOW CLASSES ============
class WindowSpec:
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"
    
    def __init__(self):
        self._partition_cols = []
        self._order_cols = []
        self._row_start = None
        self._row_end = None
    
    def partitionBy(self, *cols):
        new_spec = WindowSpec()
        new_spec._partition_cols = [c if isinstance(c, str) else c.name for c in cols]
        new_spec._order_cols = self._order_cols
        return new_spec
    
    def orderBy(self, *cols):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = list(cols)
        return new_spec
    
    def rowsBetween(self, start, end):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = self._order_cols
        new_spec._row_start = start
        new_spec._row_end = end
        return new_spec

class Window:
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"
    
    @staticmethod
    def partitionBy(*cols): return WindowSpec().partitionBy(*cols)
    @staticmethod
    def orderBy(*cols): return WindowSpec().orderBy(*cols)

# ============ GROUPED DATA ============
class GroupedData:
    def __init__(self, df, group_cols):
        self._df = df
        self._group_cols = group_cols
    
    def agg(self, *exprs):
        pdf = self._df._pdf.copy()
        grouped = pdf.groupby(self._group_cols, as_index=False)
        
        agg_results = {}
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg': agg_results[col_name] = grouped[source_col].mean()[source_col]
                elif func == 'sum': agg_results[col_name] = grouped[source_col].sum()[source_col]
                elif func == 'count': agg_results[col_name] = grouped[source_col].count()[source_col]
                elif func == 'min': agg_results[col_name] = grouped[source_col].min()[source_col]
                elif func == 'max': agg_results[col_name] = grouped[source_col].max()[source_col]
                
                if hasattr(expr, '_round_decimals'):
                    agg_results[col_name] = agg_results[col_name].round(expr._round_decimals)
        
        result_pdf = grouped[self._group_cols].first()
        for col_name, values in agg_results.items():
            result_pdf[col_name] = values.values
        
        return DataFrame(result_pdf)

# ============ DATAFRAME CLASS ============
class DataFrame:
    def __init__(self, pdf):
        self._pdf = pdf
    
    @property
    def columns(self): return list(self._pdf.columns)
    
    @property
    def write(self): return DataFrameWriter(self)
    
    def count(self): return len(self._pdf)
    def show(self, n=20): print(self._pdf.head(n).to_string())
    
    def collect(self):
        rows = []
        for _, row in self._pdf.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            rows.append(Row(**row_dict))
        return rows
    
    def first(self):
        if len(self._pdf) == 0: return None
        row = self._pdf.iloc[0]
        return Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
    
    def select(self, *cols):
        result_cols = {}
        for c in cols:
            if isinstance(c, str): result_cols[c] = self._pdf[c]
            elif isinstance(c, Column):
                col_name = c._alias or c.name
                if hasattr(c, '_transform_func'): result_cols[col_name] = c._transform_func(self._pdf)
                else:
                    try: result_cols[col_name] = self._pdf.eval(c.expr)
                    except: result_cols[col_name] = self._pdf[c.name]
        return DataFrame(pd.DataFrame(result_cols))
    
    def filter(self, condition):
        if isinstance(condition, Column):
            mask = self._pdf.eval(condition.expr)
            return DataFrame(self._pdf[mask].reset_index(drop=True))
        return self
    
    def where(self, condition): return self.filter(condition)
    
    def withColumn(self, name, col):
        pdf = self._pdf.copy()
        
        if hasattr(col, '_is_window_func') and col._is_window_func:
            window = col._window
            if window._order_cols:
                sort_cols = [oc if isinstance(oc, str) else oc.name for oc in window._order_cols]
                pdf = pdf.sort_values(sort_cols).reset_index(drop=True)
            
            if hasattr(col, '_agg_func') and col._agg_func == 'sum':
                source_col = col._source_col
                if window._partition_cols:
                    pdf[name] = pdf.groupby(window._partition_cols)[source_col].cumsum()
                else:
                    pdf[name] = pdf[source_col].cumsum()
            return DataFrame(pdf)
        
        if hasattr(col, '_transform_func'):
            pdf[name] = col._transform_func(pdf)
            return DataFrame(pdf)
        
        try: pdf[name] = pdf.eval(col.expr)
        except:
            if col.name in pdf.columns: pdf[name] = pdf[col.name]
        return DataFrame(pdf)
    
    def groupBy(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return GroupedData(self, col_names)
    
    def join(self, other, on, how='inner'):
        result = self._pdf.merge(other._pdf, on=on, how=how)
        return DataFrame(result)
    
    def orderBy(self, *cols):
        sort_cols = []
        asc_list = []
        for c in cols:
            if isinstance(c, str):
                sort_cols.append(c)
                asc_list.append(True)
            else:
                sort_cols.append(c.name)
                asc_list.append(not getattr(c, '_sort_desc', False))
        return DataFrame(self._pdf.sort_values(sort_cols, ascending=asc_list).reset_index(drop=True))
    
    def drop(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.drop(columns=col_names))
    
    def distinct(self): return DataFrame(self._pdf.drop_duplicates().reset_index(drop=True))
    def limit(self, n): return DataFrame(self._pdf.head(n).copy())
    def toPandas(self): return self._pdf.copy()

# ============ SPARK SESSION ============
class SparkSession:
    @property
    def read(self): return DataFrameReader(self)
    
    def createDataFrame(self, data, schema):
        pdf = pd.DataFrame(data, columns=schema)
        return DataFrame(pdf)

# ============ DATAFRAME READER/WRITER ============
class DataFrameReader:
    def __init__(self, spark):
        self._spark = spark
        self._format = None
        self._options = {}
    
    def format(self, fmt):
        self._format = fmt
        return self
    
    def option(self, key, value):
        self._options[key] = value
        return self
    
    def load(self, path):
        if self._format == "delta":
            if path not in _delta_tables:
                raise ValueError(f"Delta table not found: {path}")
            table_data = _delta_tables[path]
            
            if "versionAsOf" in self._options:
                version = int(self._options["versionAsOf"])
                return DataFrame(table_data.get_df(version))
            return DataFrame(table_data.get_df())
        raise ValueError(f"Unsupported format: {self._format}")

class DataFrameWriter:
    def __init__(self, df):
        self._df = df
        self._format = None
        self._mode = "error"
        self._options = {}
    
    def format(self, fmt):
        self._format = fmt
        return self
    
    def mode(self, mode):
        self._mode = mode
        return self
    
    def option(self, key, value):
        self._options[key] = value
        return self
    
    def save(self, path):
        if self._format != "delta":
            raise ValueError(f"Unsupported format: {self._format}")
        
        pdf = self._df._pdf
        
        if path in _delta_tables:
            if self._mode == "error":
                raise ValueError(f"Table exists: {path}")
            elif self._mode == "overwrite":
                _delta_tables[path]._add_version(pdf, "WRITE", {"mode": "overwrite"})
            elif self._mode == "append":
                current = _delta_tables[path].get_df()
                new_df = pd.concat([current, pdf], ignore_index=True)
                _delta_tables[path]._add_version(new_df, "WRITE", {"mode": "append"})
        else:
            table_data = DeltaTableData(path, pdf)
            _delta_tables[path] = table_data

# ============ MERGE BUILDER ============
class MergeBuilder:
    def __init__(self, delta_table, source_df, condition):
        self._delta_table = delta_table
        self._source_df = source_df._pdf if hasattr(source_df, '_pdf') else source_df
        self._condition = condition
        self._when_matched_update = None
        self._when_matched_delete = False
        self._when_not_matched_insert = None
    
    def whenMatchedUpdate(self, set=None):
        new_builder = MergeBuilder(self._delta_table, self._source_df, self._condition)
        new_builder._when_matched_update = set or "ALL"
        new_builder._when_not_matched_insert = self._when_not_matched_insert
        return new_builder
    
    def whenMatchedUpdateAll(self):
        return self.whenMatchedUpdate("ALL")
    
    def whenMatchedDelete(self):
        new_builder = MergeBuilder(self._delta_table, self._source_df, self._condition)
        new_builder._when_matched_delete = True
        new_builder._when_not_matched_insert = self._when_not_matched_insert
        return new_builder
    
    def whenNotMatchedInsert(self, values=None):
        new_builder = MergeBuilder(self._delta_table, self._source_df, self._condition)
        new_builder._when_matched_update = self._when_matched_update
        new_builder._when_matched_delete = self._when_matched_delete
        new_builder._when_not_matched_insert = values or "ALL"
        return new_builder
    
    def whenNotMatchedInsertAll(self):
        return self.whenNotMatchedInsert("ALL")
    
    def execute(self):
        target_df = self._delta_table._table_data.get_df()
        source_df = self._source_df
        
        # Extract key column from condition
        key_cols = []
        for part in self._condition.replace("target.", "").replace("source.", "").split(" AND "):
            if "=" in part:
                key_cols.append(part.split("=")[0].strip())
        if not key_cols: key_cols = [target_df.columns[0]]
        
        merged = target_df.merge(source_df, on=key_cols, how='outer', indicator=True, suffixes=('', '_source'))
        
        rows_updated = rows_inserted = rows_deleted = 0
        result_rows = []
        
        for idx, row in merged.iterrows():
            merge_status = row['_merge']
            
            if merge_status == 'both':
                if self._when_matched_delete:
                    rows_deleted += 1
                    continue
                elif self._when_matched_update:
                    rows_updated += 1
                    new_row = {}
                    for col in target_df.columns:
                        source_col = f"{col}_source"
                        if source_col in row and pd.notna(row[source_col]):
                            new_row[col] = row[source_col]
                        else:
                            new_row[col] = row[col]
                    result_rows.append(new_row)
                else:
                    result_rows.append({col: row[col] for col in target_df.columns})
                    
            elif merge_status == 'left_only':
                result_rows.append({col: row[col] for col in target_df.columns})
                    
            elif merge_status == 'right_only':
                if self._when_not_matched_insert:
                    rows_inserted += 1
                    new_row = {}
                    for col in target_df.columns:
                        source_col = f"{col}_source"
                        if source_col in row and pd.notna(row[source_col]):
                            new_row[col] = row[source_col]
                        elif col in source_df.columns:
                            new_row[col] = row.get(col)
                        else:
                            new_row[col] = None
                    result_rows.append(new_row)
        
        result_df = pd.DataFrame(result_rows, columns=target_df.columns)
        self._delta_table._table_data._add_version(result_df, "MERGE", {
            "numTargetRowsUpdated": rows_updated,
            "numTargetRowsInserted": rows_inserted,
            "numTargetRowsDeleted": rows_deleted
        })
        return {"rows_updated": rows_updated, "rows_inserted": rows_inserted, "rows_deleted": rows_deleted}

# ============ OPTIMIZE BUILDER ============
class OptimizeBuilder:
    def __init__(self, delta_table):
        self._delta_table = delta_table
    
    def zorderBy(self, *cols):
        self._delta_table._table_data.history_log[-1]["operationMetrics"]["zOrderBy"] = list(cols)
        return self

# ============ DELTA TABLE BUILDER ============
class DeltaTableBuilder:
    def __init__(self, spark, if_not_exists=False):
        self._spark = spark
        self._if_not_exists = if_not_exists
        self._table_name = None
        self._columns = []
    
    def tableName(self, name):
        self._table_name = name
        return self
    
    def addColumn(self, name, dtype, nullable=True):
        self._columns.append({"name": name, "type": dtype})
        return self
    
    def execute(self):
        path = self._table_name
        if self._if_not_exists and path in _delta_tables:
            return DeltaTable(_delta_tables[path])
        
        columns = {col["name"]: pd.Series(dtype='object') for col in self._columns}
        initial_df = pd.DataFrame(columns)
        
        table_data = DeltaTableData(path, initial_df)
        table_data.schema = self._columns
        _delta_tables[path] = table_data
        
        return DeltaTable(table_data)

# ============ DELTA TABLE CLASS ============
class DeltaTable:
    def __init__(self, table_data):
        self._table_data = table_data
    
    @classmethod
    def forPath(cls, spark, path):
        if path not in _delta_tables:
            raise ValueError(f"Delta table not found: {path}")
        return cls(_delta_tables[path])
    
    @classmethod
    def forName(cls, spark, name):
        return cls.forPath(spark, name)
    
    @classmethod
    def create(cls, spark):
        return DeltaTableBuilder(spark, if_not_exists=False)
    
    @classmethod
    def createIfNotExists(cls, spark):
        return DeltaTableBuilder(spark, if_not_exists=True)
    
    @classmethod
    def isDeltaTable(cls, spark, path):
        return path in _delta_tables
    
    def toDF(self):
        return DataFrame(self._table_data.get_df())
    
    def merge(self, source, condition):
        return MergeBuilder(self, source, condition)
    
    def update(self, condition=None, set_values=None):
        df = self._table_data.get_df()
        if condition:
            mask = df.eval(condition)
        else:
            mask = pd.Series([True] * len(df))
        
        rows_updated = mask.sum()
        for col, value in (set_values or {}).items():
            df.loc[mask, col] = value
        
        self._table_data._add_version(df, "UPDATE", {"numUpdatedRows": int(rows_updated)})
    
    def delete(self, condition=None):
        df = self._table_data.get_df()
        if condition:
            mask = ~df.eval(condition)
            rows_deleted = (~mask).sum()
            df = df[mask].reset_index(drop=True)
        else:
            rows_deleted = len(df)
            df = pd.DataFrame(columns=df.columns)
        
        self._table_data._add_version(df, "DELETE", {"numDeletedRows": int(rows_deleted)})
    
    def history(self, limit=None):
        history = self._table_data.history_log.copy()
        history.reverse()
        if limit: history = history[:limit]
        return DataFrame(pd.DataFrame(history))
    
    def optimize(self):
        df = self._table_data.get_df()
        self._table_data._add_version(df, "OPTIMIZE", {"numFilesAdded": 1, "numFilesRemoved": 5})
        return OptimizeBuilder(self)
    
    def vacuum(self, retention_hours=168):
        self._table_data.history_log.append({
            "version": self._table_data.current_version,
            "timestamp": datetime.now().isoformat(),
            "operation": "VACUUM",
            "operationMetrics": {"numFilesDeleted": 3}
        })
        return {"files_deleted": 3}

# ============ SQL FUNCTIONS ============
def col(name): return Column(name)

def lit(value):
    c = Column("_lit", repr(value))
    c._lit_value = value
    def transform(pdf): return pd.Series([value] * len(pdf))
    c._transform_func = transform
    return c

def avg(col_name):
    c = Column(col_name)
    c._agg_func = 'avg'
    c._source_col = col_name
    return c

def sum(col_name):
    c = Column(col_name)
    c._agg_func = 'sum'
    c._source_col = col_name
    return c

def count(col_name):
    c = Column(col_name)
    c._agg_func = 'count'
    c._source_col = col_name
    return c

def min(col_name):
    c = Column(col_name)
    c._agg_func = 'min'
    c._source_col = col_name
    return c

def max(col_name):
    c = Column(col_name)
    c._agg_func = 'max'
    c._source_col = col_name
    return c

def round(col_expr, decimals=0):
    new_col = Column(col_expr.name, col_expr.expr)
    if hasattr(col_expr, '_agg_func'):
        new_col._agg_func = col_expr._agg_func
        new_col._source_col = col_expr._source_col
    new_col._round_decimals = decimals
    if hasattr(col_expr, '_alias'): new_col._alias = col_expr._alias
    return new_col

# Create global spark session
spark = SparkSession()
spark_sum = sum
`;

// Categories for navigation
const CATEGORIES = [...new Set(KOANS.map(k => k.category))];

export default function PySparkKoans() {
  const [currentKoan, setCurrentKoan] = useState(0);
  const [code, setCode] = useState('');
  const [output, setOutput] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [pyodide, setPyodide] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [showHints, setShowHints] = useState(false);
  const [currentHint, setCurrentHint] = useState(0);
  const [completedKoans, setCompletedKoans] = useState(new Set());
  const [showSolution, setShowSolution] = useState(false);
  const [selectedCategory, setSelectedCategory] = useState(null);
  const textareaRef = useRef(null);

  const koan = KOANS[currentKoan];
  const filteredKoans = selectedCategory 
    ? KOANS.filter(k => k.category === selectedCategory)
    : KOANS;

  useEffect(() => {
    async function initPyodide() {
      setIsLoading(true);
      try {
        const pyodideInstance = await window.loadPyodide({
          indexURL: "https://cdn.jsdelivr.net/pyodide/v0.24.1/full/"
        });
        await pyodideInstance.loadPackage(['pandas']);
        await pyodideInstance.runPythonAsync(FULL_SHIM);
        
        setPyodide(pyodideInstance);
        setOutput('✓ PySpark + Delta Lake environment ready!\\n\\nClick "Run Code" to test your solution.');
      } catch (error) {
        setOutput(\`Error loading Python environment: \${error.message}\`);
      }
      setIsLoading(false);
    }
    
    if (!window.loadPyodide) {
      const script = document.createElement('script');
      script.src = 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js';
      script.onload = initPyodide;
      document.head.appendChild(script);
    } else {
      initPyodide();
    }
  }, []);

  useEffect(() => {
    setCode(koan.template);
    setOutput('');
    setShowHints(false);
    setCurrentHint(0);
    setShowSolution(false);
  }, [currentKoan]);

  const runCode = async () => {
    if (!pyodide) return;
    
    setIsRunning(true);
    setOutput('Running...\\n');
    
    try {
      await pyodide.runPythonAsync(FULL_SHIM);
      await pyodide.runPythonAsync(koan.setup);
      
      await pyodide.runPythonAsync(\`
import sys
from io import StringIO
_stdout_capture = StringIO()
sys.stdout = _stdout_capture
\`);
      
      await pyodide.runPythonAsync(code);
      
      const capturedOutput = await pyodide.runPythonAsync(\`
sys.stdout = sys.__stdout__
_stdout_capture.getvalue()
\`);
      
      setOutput(capturedOutput);
      
      if (capturedOutput.includes('🎉 Koan complete!')) {
        setCompletedKoans(prev => new Set([...prev, currentKoan]));
      }
    } catch (error) {
      setOutput(\`Error:\\n\${error.message}\\n\\nTip: Check your syntax and make sure you replaced all ___ placeholders.\`);
    }
    
    setIsRunning(false);
  };

  const nextHint = () => {
    setShowHints(true);
    if (currentHint < koan.hints.length - 1) {
      setCurrentHint(prev => prev + 1);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newCode = code.substring(0, start) + '    ' + code.substring(end);
      setCode(newCode);
      setTimeout(() => {
        e.target.selectionStart = e.target.selectionEnd = start + 4;
      }, 0);
    }
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      runCode();
    }
  };

  const goToNextKoan = () => {
    const currentIndex = filteredKoans.findIndex(k => k.id === koan.id);
    if (currentIndex < filteredKoans.length - 1) {
      const nextKoan = filteredKoans[currentIndex + 1];
      setCurrentKoan(KOANS.findIndex(k => k.id === nextKoan.id));
    }
  };

  const goToPrevKoan = () => {
    const currentIndex = filteredKoans.findIndex(k => k.id === koan.id);
    if (currentIndex > 0) {
      const prevKoan = filteredKoans[currentIndex - 1];
      setCurrentKoan(KOANS.findIndex(k => k.id === prevKoan.id));
    }
  };

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="flex">
        {/* Sidebar */}
        <div className="w-72 min-h-screen bg-gray-900 border-r border-gray-800 p-4 overflow-y-auto">
          <h1 className="text-2xl font-bold text-orange-500 mb-1">PySpark Koans</h1>
          <p className="text-gray-500 text-sm mb-4">Learn by fixing tests</p>
          
          {/* Progress */}
          <div className="mb-4">
            <div className="flex justify-between text-sm text-gray-500 mb-1">
              <span>Progress</span>
              <span>{completedKoans.size}/{KOANS.length}</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div 
                className="bg-orange-600 h-2 rounded-full transition-all"
                style={{ width: \`\${(completedKoans.size / KOANS.length) * 100}%\` }}
              />
            </div>
          </div>
          
          {/* Categories */}
          <div className="mb-4">
            <button
              onClick={() => setSelectedCategory(null)}
              className={\`w-full text-left px-3 py-2 rounded-lg text-sm mb-1 transition-colors \${
                selectedCategory === null ? 'bg-orange-600 text-white' : 'text-gray-400 hover:bg-gray-800'
              }\`}
            >
              All Koans ({KOANS.length})
            </button>
            {CATEGORIES.map(cat => {
              const catKoans = KOANS.filter(k => k.category === cat);
              const catCompleted = catKoans.filter(k => completedKoans.has(KOANS.indexOf(k))).length;
              return (
                <button
                  key={cat}
                  onClick={() => setSelectedCategory(cat)}
                  className={\`w-full text-left px-3 py-2 rounded-lg text-sm mb-1 transition-colors flex justify-between \${
                    selectedCategory === cat ? 'bg-orange-600 text-white' : 'text-gray-400 hover:bg-gray-800'
                  }\`}
                >
                  <span>{cat}</span>
                  <span className="text-xs opacity-75">{catCompleted}/{catKoans.length}</span>
                </button>
              );
            })}
          </div>
          
          {/* Koan list */}
          <div className="space-y-1">
            {filteredKoans.map((k) => {
              const globalIndex = KOANS.indexOf(k);
              return (
                <button
                  key={k.id}
                  onClick={() => setCurrentKoan(globalIndex)}
                  className={\`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors flex items-center gap-2 \${
                    globalIndex === currentKoan ? 'bg-gray-800 text-white' : 'text-gray-400 hover:bg-gray-800/50'
                  }\`}
                >
                  <span className={\`w-5 h-5 rounded flex items-center justify-center text-xs \${
                    completedKoans.has(globalIndex) ? 'bg-green-600 text-white' : 'bg-gray-700 text-gray-400'
                  }\`}>
                    {completedKoans.has(globalIndex) ? '✓' : k.id}
                  </span>
                  <span className="truncate">{k.title}</span>
                </button>
              );
            })}
          </div>
        </div>
        
        {/* Main content */}
        <div className="flex-1 p-6 overflow-y-auto">
          <div className="max-w-5xl mx-auto">
            <div className="mb-6">
              <div className="flex items-center gap-2 mb-2">
                <span className={\`text-xs px-2 py-1 rounded \${
                  koan.category === 'Delta Lake' ? 'bg-blue-900 text-blue-300' : 'bg-gray-800 text-gray-400'
                }\`}>
                  {koan.category}
                </span>
                <span className="text-xs text-gray-600">Koan {koan.id}</span>
              </div>
              <h2 className="text-2xl font-semibold text-white mb-2">{koan.title}</h2>
              <p className="text-gray-400">{koan.description}</p>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Setup (read-only)</span>
                  </div>
                  <pre className="p-4 text-sm text-gray-400 font-mono whitespace-pre-wrap overflow-x-auto">
                    {koan.setup.trim()}
                  </pre>
                </div>

                {showHints && (
                  <div className="bg-yellow-900/20 border border-yellow-800/50 rounded-lg p-4">
                    <h3 className="text-sm font-medium text-yellow-500 mb-2">
                      Hint {currentHint + 1}/{koan.hints.length}
                    </h3>
                    <p className="text-yellow-200/80">{koan.hints[currentHint]}</p>
                  </div>
                )}

                {showSolution && (
                  <div className="bg-green-900/20 border border-green-800/50 rounded-lg p-4">
                    <h3 className="text-sm font-medium text-green-500 mb-2">Solution</h3>
                    <pre className="text-sm text-green-200/80 font-mono whitespace-pre-wrap">
                      {koan.solution}
                    </pre>
                  </div>
                )}
              </div>

              <div className="space-y-4">
                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Your Code</span>
                    <span className="text-xs text-gray-600">Ctrl/Cmd+Enter to run</span>
                  </div>
                  <textarea
                    ref={textareaRef}
                    value={code}
                    onChange={(e) => setCode(e.target.value)}
                    onKeyDown={handleKeyDown}
                    className="w-full h-64 p-4 bg-gray-950 text-gray-100 font-mono text-sm resize-none focus:outline-none"
                    spellCheck={false}
                  />
                </div>

                <div className="flex gap-2 flex-wrap">
                  <button
                    onClick={runCode}
                    disabled={isLoading || isRunning}
                    className="px-4 py-2 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-700 disabled:text-gray-500 rounded-lg font-medium transition-colors"
                  >
                    {isLoading ? 'Loading...' : isRunning ? 'Running...' : 'Run Code'}
                  </button>
                  <button
                    onClick={nextHint}
                    disabled={currentHint >= koan.hints.length - 1 && showHints}
                    className="px-4 py-2 bg-gray-700 hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 rounded-lg transition-colors"
                  >
                    {showHints ? 'Next Hint' : 'Hint'}
                  </button>
                  <button
                    onClick={() => setShowSolution(!showSolution)}
                    className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
                  >
                    {showSolution ? 'Hide' : 'Solution'}
                  </button>
                  <button
                    onClick={() => setCode(koan.template)}
                    className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
                  >
                    Reset
                  </button>
                </div>

                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Output</span>
                  </div>
                  <pre className={\`p-4 h-48 overflow-auto font-mono text-sm whitespace-pre-wrap \${
                    output.includes('🎉') ? 'text-green-400' : 
                    output.includes('Error') ? 'text-red-400' : 'text-gray-300'
                  }\`}>
                    {output || 'Output will appear here...'}
                  </pre>
                </div>

                <div className="flex justify-between">
                  <button
                    onClick={goToPrevKoan}
                    disabled={filteredKoans.findIndex(k => k.id === koan.id) === 0}
                    className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
                  >
                    ← Previous
                  </button>
                  <button
                    onClick={goToNextKoan}
                    disabled={filteredKoans.findIndex(k => k.id === koan.id) === filteredKoans.length - 1}
                    className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
                  >
                    Next →
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
