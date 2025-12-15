# Delta Lake Koans

Browser-based exercises for learning Delta Lake through test-driven practice.

---

## Koan 101: Creating a Delta Table

**Objective:** Write a DataFrame as a Delta table and read it back.

**Setup:**
```python
_reset_delta_tables()  # Clean slate

data = [("Alice", 34, "Engineering"), ("Bob", 45, "Sales"), ("Charlie", 29, "Engineering")]
df = spark.createDataFrame(data, ["name", "age", "department"])
```

**Exercise:**
```python
# Write the DataFrame as a Delta table
df.write.___("delta").mode("overwrite").save("/data/employees")

# Read it back
result = spark.read.format("___").load("/data/employees")

assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Delta table created and read successfully")

# Verify it's a Delta table
assert DeltaTable.___(spark, "/data/employees"), "Should be a Delta table"
print("✓ Confirmed as Delta table")
```

**Solution:**
```python
df.write.format("delta").mode("overwrite").save("/data/employees")
result = spark.read.format("delta").load("/data/employees")
assert DeltaTable.isDeltaTable(spark, "/data/employees")
```

**Key Concepts:**
- Use `.format("delta")` for both reading and writing
- `DeltaTable.isDeltaTable()` verifies a path contains a Delta table
- Mode options: `overwrite`, `append`, `error`, `ignore`

---

## Koan 102: Time Travel - Version

**Objective:** Query a previous version of a Delta table.

**Setup:**
```python
_reset_delta_tables()

# Create initial data (version 0)
data_v0 = [("Alice", 100), ("Bob", 200)]
df_v0 = spark.createDataFrame(data_v0, ["name", "balance"])
df_v0.write.format("delta").save("/data/accounts")

# Update data (version 1)
data_v1 = [("Alice", 150), ("Bob", 250), ("Charlie", 300)]
df_v1 = spark.createDataFrame(data_v1, ["name", "balance"])
df_v1.write.format("delta").mode("overwrite").save("/data/accounts")
```

**Exercise:**
```python
# Read the current version
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
```

**Solution:**
```python
historical = spark.read.format("delta").option("versionAsOf", 0).load("/data/accounts")
```

**Key Concepts:**
- `versionAsOf` - query by version number (0, 1, 2, ...)
- `timestampAsOf` - query by timestamp (e.g., "2024-01-01")
- Each write creates a new version
- Time travel enables auditing, debugging, and rollback

---

## Koan 103: MERGE - Upsert Pattern

**Objective:** Use MERGE to update existing rows and insert new ones (upsert).

**Setup:**
```python
_reset_delta_tables()

# Create target table
target_data = [("Alice", 100), ("Bob", 200)]
target_df = spark.createDataFrame(target_data, ["name", "balance"])
target_df.write.format("delta").save("/data/accounts")

# Source data with updates and new records
source_data = [("Alice", 150), ("Charlie", 300)]  # Alice updated, Charlie is new
source_df = spark.createDataFrame(source_data, ["name", "balance"])
```

**Exercise:**
```python
# Get the Delta table
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
```

**Solution:**
```python
dt.merge(
    source_df,
    "target.name = source.name"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Key Concepts:**
- `DeltaTable.forPath()` gets a reference to an existing Delta table
- MERGE condition uses `target.` and `source.` prefixes
- `whenMatchedUpdateAll()` - update all columns when match found
- `whenNotMatchedInsertAll()` - insert all columns when no match
- Always call `.execute()` to run the merge

---

## Koan 104: MERGE - Selective Update

**Objective:** Use MERGE with specific column mappings instead of updating all columns.

**Setup:**
```python
_reset_delta_tables()

# Create target table with more columns
target_data = [("Alice", 100, "2024-01-01"), ("Bob", 200, "2024-01-01")]
target_df = spark.createDataFrame(target_data, ["name", "balance", "last_updated"])
target_df.write.format("delta").save("/data/accounts")

# Source only has name and new balance
source_data = [("Alice", 500)]
source_df = spark.createDataFrame(source_data, ["name", "new_balance"])
```

**Exercise:**
```python
# Get the Delta table
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
```

**Solution:**
```python
dt.merge(
    source_df,
    "target.name = source.name"
).whenMatchedUpdate(set={
    "balance": "source.new_balance",
    "last_updated": "'2024-06-01'"
}).execute()
```

**Key Concepts:**
- `whenMatchedUpdate(set={...})` allows specific column mappings
- Map target columns to source expressions
- Use string literals with inner quotes: `"'value'"`
- Only matched rows are affected; unmatched rows stay unchanged

---

## Koan 105: Table History

**Objective:** View the history of operations on a Delta table.

**Setup:**
```python
_reset_delta_tables()

# Create and modify table multiple times
data = [("Alice", 100)]
df = spark.createDataFrame(data, ["name", "balance"])
df.write.format("delta").save("/data/accounts")

# Make some updates
data2 = [("Alice", 100), ("Bob", 200)]
df2 = spark.createDataFrame(data2, ["name", "balance"])
df2.write.format("delta").mode("overwrite").save("/data/accounts")
```

**Exercise:**
```python
# Get the Delta table
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
```

**Solution:**
```python
history_df = dt.history()
```

**Key Concepts:**
- `.history()` returns a DataFrame with version metadata
- Each row includes: version, timestamp, operation, operationMetrics
- Pass a limit: `.history(10)` for last 10 operations
- SQL equivalent: `DESCRIBE HISTORY table_name`

---

## Koan 106: OPTIMIZE and Z-ORDER

**Objective:** Optimize a Delta table for better query performance.

**Setup:**
```python
_reset_delta_tables()

# Create table with data
data = [(i, f"user_{i}", i % 10) for i in range(100)]
df = spark.createDataFrame(data, ["id", "name", "category"])
df.write.format("delta").save("/data/users")
```

**Exercise:**
```python
# Get the Delta table
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
```

**Solution:**
```python
dt.optimize()
dt.optimize().zorderBy("category")
```

**Key Concepts:**
- `OPTIMIZE` compacts small files into larger ones (improves read performance)
- `Z-ORDER` co-locates related data for faster filtering
- Z-ORDER on columns you frequently filter by
- Run OPTIMIZE periodically on tables with many small writes
- SQL: `OPTIMIZE table_name ZORDER BY (col1, col2)`

---

## Koan 107: Delete with Condition

**Objective:** Delete rows from a Delta table based on a condition.

**Setup:**
```python
_reset_delta_tables()

data = [("Alice", 100, True), ("Bob", 200, False), ("Charlie", 150, True)]
df = spark.createDataFrame(data, ["name", "balance", "is_active"])
df.write.format("delta").save("/data/accounts")
```

**Exercise:**
```python
# Get the Delta table
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
```

**Solution:**
```python
dt.delete(condition="is_active == False")
```

**Key Concepts:**
- `.delete()` removes rows matching the condition
- Without condition, deletes ALL rows (use carefully!)
- Delta Lake delete is transactional and creates a new version
- SQL: `DELETE FROM table_name WHERE condition`

---

## Koan 108: Update with Condition

**Objective:** Update rows in a Delta table based on a condition.

**Setup:**
```python
_reset_delta_tables()

data = [("Alice", 100, "basic"), ("Bob", 200, "premium"), ("Charlie", 50, "basic")]
df = spark.createDataFrame(data, ["name", "balance", "tier"])
df.write.format("delta").save("/data/accounts")
```

**Exercise:**
```python
# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Give all premium users a bonus: set their balance to 300
dt.___(
    condition="tier == 'premium'",
    set_values={"___": 300}
)

# Verify Bob got the update
result = dt.toDF()
bob = result.filter(col("name") == "Bob").collect()[0]

assert bob["balance"] == 300, f"Bob should have 300, got {bob['balance']}"
print("✓ Premium user (Bob) balance updated")

# Verify others unchanged
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["balance"] == 100, f"Alice should still have 100"
print("✓ Basic users unchanged")
```

**Solution:**
```python
dt.update(
    condition="tier == 'premium'",
    set_values={"balance": 300}
)
```

**Key Concepts:**
- `.update()` modifies rows matching the condition
- `set_values` is a dict mapping column names to new values
- Only matched rows are updated
- SQL: `UPDATE table_name SET col = value WHERE condition`

---

## Koan 109: Create Table with Builder

**Objective:** Create a Delta table with an explicit schema using the builder API.

**Setup:**
```python
_reset_delta_tables()
```

**Exercise:**
```python
# Create a Delta table with explicit schema
DeltaTable._____(spark) \
    .tableName("products") \
    .addColumn("id", "INT") \
    .addColumn("___", "STRING") \
    .addColumn("price", "DOUBLE") \
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
```

**Solution:**
```python
DeltaTable.create(spark) \
    .tableName("products") \
    .addColumn("id", "INT") \
    .addColumn("name", "STRING") \
    .addColumn("price", "DOUBLE") \
    .execute()
```

**Key Concepts:**
- `DeltaTable.create(spark)` starts the builder
- `DeltaTable.createIfNotExists(spark)` - won't error if exists
- Chain `.addColumn(name, type)` for each column
- Optional: `.partitionedBy()`, `.location()`, `.property()`
- Always end with `.execute()`

---

## Koan 110: VACUUM Old Files

**Objective:** Clean up old files from a Delta table to save storage.

**Setup:**
```python
_reset_delta_tables()

# Create table and make several updates
for i in range(3):
    data = [(j, f"version_{i}") for j in range(10)]
    df = spark.createDataFrame(data, ["id", "data"])
    df.write.format("delta").mode("overwrite").save("/data/versions")
```

**Exercise:**
```python
# Get the Delta table  
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
print("\n⚠️  Warning: VACUUM removes old version files!")
print("   Time travel to vacuumed versions will fail.")
```

**Solution:**
```python
result = dt.vacuum(retention_hours=168)
```

**Key Concepts:**
- `VACUUM` removes files no longer referenced by the table
- Default retention: 168 hours (7 days)
- **Warning:** After vacuum, time travel to old versions fails!
- Set `spark.databricks.delta.retentionDurationCheck.enabled = false` to use shorter retention (dangerous!)
- SQL: `VACUUM table_name RETAIN 168 HOURS`
- Trade-off: Storage savings vs. time travel capability

---

## Summary: Delta Lake Operations

| Operation | Python API | SQL |
|-----------|------------|-----|
| Write | `df.write.format("delta").save(path)` | `INSERT INTO` |
| Read | `spark.read.format("delta").load(path)` | `SELECT * FROM` |
| Time Travel | `.option("versionAsOf", n)` | `SELECT * FROM table VERSION AS OF n` |
| MERGE | `dt.merge(...).whenMatched...execute()` | `MERGE INTO ... USING ...` |
| Update | `dt.update(condition, set_values)` | `UPDATE ... SET ... WHERE` |
| Delete | `dt.delete(condition)` | `DELETE FROM ... WHERE` |
| History | `dt.history()` | `DESCRIBE HISTORY table` |
| Optimize | `dt.optimize().zorderBy(col)` | `OPTIMIZE table ZORDER BY (col)` |
| Vacuum | `dt.vacuum(hours)` | `VACUUM table RETAIN n HOURS` |

---

## Exam Relevance

| Koan | DEA | DEP | DAA |
|------|-----|-----|-----|
| 101 - Creating a Delta Table | ✓ | ✓ | ✓ |
| 102 - Time Travel | ✓ | ✓ | ✓ |
| 103 - MERGE Upsert | ✓ | ✓ | |
| 104 - MERGE Selective | | ✓ | |
| 105 - Table History | ✓ | ✓ | ✓ |
| 106 - OPTIMIZE & Z-ORDER | ✓ | ✓ | |
| 107 - Delete | ✓ | ✓ | |
| 108 - Update | ✓ | ✓ | |
| 109 - Builder API | | ✓ | |
| 110 - VACUUM | ✓ | ✓ | |

**DEA** = Data Engineer Associate  
**DEP** = Data Engineer Professional  
**DAA** = Data Analyst Associate
