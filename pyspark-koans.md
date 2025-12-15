# PySpark Koans

Browser-based exercises for learning PySpark through test-driven practice.

---

## Koan 1: Creating a DataFrame

**Objective:** Create a DataFrame from Python data structures.

**Setup:**
```python
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
```

**Exercise:**
```python
# Create a DataFrame from the data and columns
df = spark.___(___, ___)

# The DataFrame should have 3 rows
assert df.count() == 3, f"Expected 3 rows, got {df.count()}"
print("✓ DataFrame created with correct row count")

# The DataFrame should have 2 columns
assert len(df.columns) == 2, f"Expected 2 columns, got {len(df.columns)}"
print("✓ DataFrame has correct number of columns")
```

**Solution:**
```python
df = spark.createDataFrame(data, columns)
```

**Key Concepts:**
- `spark.createDataFrame()` creates a DataFrame from local data
- First argument: data (list of tuples/rows)
- Second argument: schema (list of column names or StructType)

---

## Koan 2: Selecting Columns

**Objective:** Select specific columns from a DataFrame.

**Setup:**
```python
data = [("Alice", 34, "NYC"), ("Bob", 45, "LA"), ("Charlie", 29, "Chicago")]
df = spark.createDataFrame(data, ["name", "age", "city"])
```

**Exercise:**
```python
# Select only the 'name' and 'city' columns
result = df.___("name", "___")

# Result should have exactly 2 columns
assert len(result.columns) == 2, f"Expected 2 columns, got {len(result.columns)}"
print("✓ Correct number of columns selected")

# Result should contain 'name' and 'city'
assert "name" in result.columns, "Missing 'name' column"
assert "city" in result.columns, "Missing 'city' column"
print("✓ Correct columns selected")
```

**Solution:**
```python
result = df.select("name", "city")
```

**Key Concepts:**
- `.select()` chooses which columns to keep
- Pass column names as strings or Column objects
- Returns a new DataFrame (original unchanged)

---

## Koan 3: Filtering Rows

**Objective:** Filter rows based on a condition.

**Setup:**
```python
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 52)]
df = spark.createDataFrame(data, ["name", "age"])
```

**Exercise:**
```python
# Filter to only include people over 35
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
```

**Solution:**
```python
result = df.filter(col("age") > 35)
```

**Key Concepts:**
- `.filter()` keeps rows matching a condition
- `.where()` is an alias for `.filter()`
- `col("name")` creates a Column reference
- Use comparison operators: `>`, `<`, `>=`, `<=`, `==`, `!=`

---

## Koan 4: Adding Columns

**Objective:** Add a new calculated column to a DataFrame.

**Setup:**
```python
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])
```

**Exercise:**
```python
# Add a new column 'age_in_months' that multiplies age by 12
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
```

**Solution:**
```python
result = df.withColumn("age_in_months", col("age") * 12)
```

**Key Concepts:**
- `.withColumn(name, expression)` adds or replaces a column
- First argument: new column name
- Second argument: Column expression
- Can use arithmetic: `+`, `-`, `*`, `/`

---

## Koan 5: Grouping and Aggregating

**Objective:** Group data and calculate aggregates.

**Setup:**
```python
data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
```

**Exercise:**
```python
# Group by department and calculate average salary
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
```

**Solution:**
```python
result = df.groupBy("department").agg(
    round(avg("salary"), 2).alias("avg_salary")
)
```

**Key Concepts:**
- `.groupBy()` groups rows by column values
- `.agg()` applies aggregate functions to groups
- Common aggregates: `avg()`, `sum()`, `count()`, `min()`, `max()`
- `.alias()` renames the resulting column

---

## Koan 6: Joining DataFrames

**Objective:** Join two DataFrames to combine related data.

**Setup:**
```python
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
```

**Exercise:**
```python
# Join employees with departments on dept_id
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
```

**Solution:**
```python
result = employees.join(departments, "dept_id", "inner")
```

**Key Concepts:**
- `.join(other, on, how)` combines DataFrames
- Join types: `inner`, `left`, `right`, `outer`, `semi`, `anti`
- Can join on single column (string) or multiple (list)
- For different column names: `.join(other, left.col == right.col)`

---

## Koan 7: Window Functions

**Objective:** Use window functions to calculate running totals.

**Setup:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 200),
    ("2024-01-04", 175)
]
df = spark.createDataFrame(data, ["date", "sales"])
```

**Exercise:**
```python
# Create a window that orders by date and includes all previous rows
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
```

**Solution:**
```python
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
result = df.withColumn("running_total", spark_sum("sales").over(window_spec))
```

**Key Concepts:**
- `Window.orderBy()` defines row ordering
- `Window.partitionBy()` creates groups (like groupBy but keeps all rows)
- `.rowsBetween()` defines the window frame
- `unboundedPreceding` = start of partition
- `currentRow` = current row
- `.over(window)` applies aggregate to window

---

## Summary: PySpark DataFrame Operations

| Operation | Method | Example |
|-----------|--------|---------|
| Create | `spark.createDataFrame()` | `spark.createDataFrame(data, schema)` |
| Select | `.select()` | `df.select("col1", "col2")` |
| Filter | `.filter()` / `.where()` | `df.filter(col("age") > 30)` |
| Add Column | `.withColumn()` | `df.withColumn("new", col("x") * 2)` |
| Drop Column | `.drop()` | `df.drop("col1")` |
| Rename | `.withColumnRenamed()` | `df.withColumnRenamed("old", "new")` |
| Sort | `.orderBy()` | `df.orderBy(col("date").desc())` |
| Distinct | `.distinct()` | `df.distinct()` |
| Limit | `.limit()` | `df.limit(10)` |
| Group | `.groupBy().agg()` | `df.groupBy("dept").agg(sum("sal"))` |
| Join | `.join()` | `df1.join(df2, "key", "inner")` |
| Union | `.union()` | `df1.union(df2)` |

---

## Exam Relevance

| Koan | DEA | DAA | MLA |
|------|-----|-----|-----|
| 1 - Creating a DataFrame | ✓ | ✓ | ✓ |
| 2 - Selecting Columns | ✓ | ✓ | ✓ |
| 3 - Filtering Rows | ✓ | ✓ | ✓ |
| 4 - Adding Columns | ✓ | ✓ | ✓ |
| 5 - Grouping & Aggregating | ✓ | ✓ | ✓ |
| 6 - Joining DataFrames | ✓ | ✓ | |
| 7 - Window Functions | ✓ | ✓ | |

**DEA** = Data Engineer Associate  
**DAA** = Data Analyst Associate  
**MLA** = Machine Learning Associate
