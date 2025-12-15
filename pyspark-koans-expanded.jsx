import React, { useState, useEffect, useRef } from 'react';

// Expanded Koan definitions organized by category
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
    title: "Sorting Data",
    category: "Basics",
    description: "Sort a DataFrame by one or more columns. Replace ___ with the correct code.",
    setup: `
data = [("Charlie", 29), ("Alice", 34), ("Bob", 45), ("Diana", 29)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Sort by age in ascending order
result = df.___(col("age"))

rows = result.collect()
ages = [row["age"] for row in rows]
assert ages == [29, 29, 34, 45], f"Expected [29, 29, 34, 45], got {ages}"
print("✓ Sorted by age ascending")

# Now sort by age descending
result_desc = df.orderBy(col("age").___())

rows_desc = result_desc.collect()
ages_desc = [row["age"] for row in rows_desc]
assert ages_desc == [45, 34, 29, 29], f"Expected [45, 34, 29, 29], got {ages_desc}"
print("✓ Sorted by age descending")

print("\\n🎉 Koan complete! You've learned to sort DataFrames.")`,
    solution: `result = df.orderBy(col("age"))\nresult_desc = df.orderBy(col("age").desc())`,
    hints: [
      "Use 'orderBy' to sort a DataFrame",
      "For descending order, call a method on the column",
      "The descending method is abbreviated"
    ]
  },
  {
    id: 5,
    title: "Limiting Results",
    category: "Basics",
    description: "Limit the number of rows returned. Replace ___ with the correct code.",
    setup: `
data = [(i, i * 10) for i in range(100)]
df = spark.createDataFrame(data, ["id", "value"])
`,
    template: `# Get only the first 5 rows
result = df.___(5)

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
print("✓ Limited to 5 rows")

# Get the first row as a single Row object
first_row = df.___()
assert first_row["id"] == 0, "First row should have id=0"
print("✓ Got first row")

print("\\n🎉 Koan complete! You've learned to limit results.")`,
    solution: `result = df.limit(5)\nfirst_row = df.first()`,
    hints: [
      "The method to restrict row count is 'limit'",
      "To get just one row, think about what comes 'first'"
    ]
  },
  {
    id: 6,
    title: "Dropping Columns",
    category: "Basics",
    description: "Remove columns from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34, "NYC", "F"), ("Bob", 45, "LA", "M")]
df = spark.createDataFrame(data, ["name", "age", "city", "gender"])
`,
    template: `# Drop the 'gender' column
result = df.___("gender")

assert "gender" not in result.columns, "gender column should be dropped"
assert len(result.columns) == 3, f"Expected 3 columns, got {len(result.columns)}"
print("✓ Dropped gender column")

# Drop multiple columns
result2 = df.___("city", "gender")
assert len(result2.columns) == 2, f"Expected 2 columns, got {len(result2.columns)}"
print("✓ Dropped multiple columns")

print("\\n🎉 Koan complete! You've learned to drop columns.")`,
    solution: `result = df.drop("gender")\nresult2 = df.drop("city", "gender")`,
    hints: [
      "The opposite of 'select' for removing columns is 'drop'",
      "You can drop multiple columns by passing multiple arguments"
    ]
  },
  {
    id: 7,
    title: "Distinct Values",
    category: "Basics",
    description: "Remove duplicate rows from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "NYC"), ("Bob", "LA"), ("Alice", "NYC"), ("Charlie", "NYC")]
df = spark.createDataFrame(data, ["name", "city"])
`,
    template: `# Get distinct rows
result = df.___()

assert result.count() == 3, f"Expected 3 distinct rows, got {result.count()}"
print("✓ Got distinct rows")

# Get distinct cities only
cities = df.select("city").___()
assert cities.count() == 2, f"Expected 2 distinct cities, got {cities.count()}"
print("✓ Got distinct cities (NYC, LA)")

print("\\n🎉 Koan complete! You've learned to get distinct values.")`,
    solution: `result = df.distinct()\ncities = df.select("city").distinct()`,
    hints: [
      "The method name is exactly what you want: 'distinct'",
      "You can chain select() with distinct() to get unique values of specific columns"
    ]
  },

  // ==================== COLUMN OPERATIONS ====================
  {
    id: 8,
    title: "Adding Columns",
    category: "Column Operations",
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
    id: 9,
    title: "Renaming Columns",
    category: "Column Operations",
    description: "Rename columns in a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Rename 'name' to 'employee_name'
result = df.___(___="employee_name")

assert "employee_name" in result.columns, "Should have employee_name column"
assert "name" not in result.columns, "Should not have name column anymore"
print("✓ Renamed name to employee_name")

# Rename using alias in select
result2 = df.select(col("name").___("full_name"), col("age"))
assert "full_name" in result2.columns, "Should have full_name column"
print("✓ Used alias in select")

print("\\n🎉 Koan complete! You've learned to rename columns.")`,
    solution: `result = df.withColumnRenamed("name", "employee_name")\nresult2 = df.select(col("name").alias("full_name"), col("age"))`,
    hints: [
      "withColumnRenamed takes the old name and new name",
      "The syntax is withColumnRenamed(old, new) - but we're using keyword style here",
      "alias() is used within select() to rename on the fly"
    ]
  },
  {
    id: 10,
    title: "Literal Values",
    category: "Column Operations",
    description: "Add a column with a constant value. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Add a column 'country' with value 'USA' for all rows
from pyspark.sql.functions import lit

result = df.withColumn("country", ___("USA"))

rows = result.collect()
assert all(row["country"] == "USA" for row in rows), "All rows should have country=USA"
print("✓ Added literal column")

# Add a numeric literal
result2 = df.withColumn("bonus", ___(1000))
assert result2.collect()[0]["bonus"] == 1000, "Bonus should be 1000"
print("✓ Added numeric literal")

print("\\n🎉 Koan complete! You've learned to use literal values.")`,
    solution: `result = df.withColumn("country", lit("USA"))\nresult2 = df.withColumn("bonus", lit(1000))`,
    hints: [
      "lit() creates a literal (constant) column",
      "It can be used with strings, numbers, or other values"
    ]
  },
  {
    id: 11,
    title: "Conditional Logic with when/otherwise",
    category: "Column Operations",
    description: "Create columns with conditional logic. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 17), ("Diana", 65)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Create an 'age_group' column based on age
from pyspark.sql.functions import when, col

result = df.withColumn(
    "age_group",
    _____(col("age") < 18, "minor")
    .when(col("age") < 65, "adult")
    ._____("senior")
)

rows = result.collect()
groups = {row["name"]: row["age_group"] for row in rows}

assert groups["Charlie"] == "minor", f"Charlie should be minor, got {groups['Charlie']}"
print("✓ Charlie (17) is minor")

assert groups["Alice"] == "adult", f"Alice should be adult, got {groups['Alice']}"
print("✓ Alice (34) is adult")

assert groups["Diana"] == "senior", f"Diana should be senior, got {groups['Diana']}"
print("✓ Diana (65) is senior")

print("\\n🎉 Koan complete! You've learned conditional column logic.")`,
    solution: `result = df.withColumn("age_group", when(col("age") < 18, "minor").when(col("age") < 65, "adult").otherwise("senior"))`,
    hints: [
      "Start with 'when' for the first condition",
      "Chain additional .when() for more conditions",
      "End with .otherwise() for the default case"
    ]
  },
  {
    id: 12,
    title: "Type Casting",
    category: "Column Operations",
    description: "Cast columns to different data types. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "34"), ("Bob", "45")]
df = spark.createDataFrame(data, ["name", "age_str"])
`,
    template: `# Cast age_str from string to integer
result = df.withColumn("age", col("age_str").cast("___"))

# Verify we can do math on the new column
result = result.withColumn("age_plus_10", col("age") + 10)

rows = result.collect()
assert rows[0]["age_plus_10"] == 44, f"Expected 44, got {rows[0]['age_plus_10']}"
print("✓ Cast to integer and performed math")

# Cast to double
result2 = df.withColumn("age_float", col("age_str").___("double"))
print("✓ Cast to double")

print("\\n🎉 Koan complete! You've learned to cast types.")`,
    solution: `result = df.withColumn("age", col("age_str").cast("integer"))\nresult2 = df.withColumn("age_float", col("age_str").cast("double"))`,
    hints: [
      "Use .cast() on a column to change its type",
      "Common types: 'integer', 'double', 'string', 'boolean', 'date'"
    ]
  },

  // ==================== STRING FUNCTIONS ====================
  {
    id: 13,
    title: "String Functions - Case",
    category: "String Functions",
    description: "Transform string case. Replace ___ with the correct code.",
    setup: `
data = [("alice smith",), ("BOB JONES",), ("Charlie Brown",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Convert to uppercase
from pyspark.sql.functions import upper, lower, initcap

result = df.withColumn("upper_name", ___(col("name")))
assert result.collect()[0]["upper_name"] == "ALICE SMITH"
print("✓ Converted to uppercase")

# Convert to lowercase
result = df.withColumn("lower_name", ___(col("name")))
assert result.collect()[1]["lower_name"] == "bob jones"
print("✓ Converted to lowercase")

# Convert to title case (capitalize first letter of each word)
result = df.withColumn("title_name", ___(col("name")))
assert result.collect()[0]["title_name"] == "Alice Smith"
print("✓ Converted to title case")

print("\\n🎉 Koan complete! You've learned string case functions.")`,
    solution: `result = df.withColumn("upper_name", upper(col("name")))\nresult = df.withColumn("lower_name", lower(col("name")))\nresult = df.withColumn("title_name", initcap(col("name")))`,
    hints: [
      "upper() converts to uppercase",
      "lower() converts to lowercase",
      "initcap() capitalizes the first letter of each word"
    ]
  },
  {
    id: 14,
    title: "String Functions - Concatenation",
    category: "String Functions",
    description: "Concatenate strings together. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "Smith"), ("Bob", "Jones")]
df = spark.createDataFrame(data, ["first", "last"])
`,
    template: `# Concatenate first and last name with a space
from pyspark.sql.functions import concat, concat_ws, lit

result = df.withColumn("full_name", ___(col("first"), lit(" "), col("last")))
assert result.collect()[0]["full_name"] == "Alice Smith"
print("✓ Concatenated with concat()")

# Use concat_ws (with separator) - cleaner for multiple values
result2 = df.withColumn("full_name", ___(" ", col("first"), col("last")))
assert result2.collect()[0]["full_name"] == "Alice Smith"
print("✓ Concatenated with concat_ws()")

print("\\n🎉 Koan complete! You've learned string concatenation.")`,
    solution: `result = df.withColumn("full_name", concat(col("first"), lit(" "), col("last")))\nresult2 = df.withColumn("full_name", concat_ws(" ", col("first"), col("last")))`,
    hints: [
      "concat() joins columns directly",
      "concat_ws() takes a separator as the first argument",
      "Use lit() for literal strings in concat()"
    ]
  },
  {
    id: 15,
    title: "String Functions - Substring and Length",
    category: "String Functions",
    description: "Extract parts of strings and measure length. Replace ___ with the correct code.",
    setup: `
data = [("Alice",), ("Bob",), ("Charlotte",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Get the length of each name
from pyspark.sql.functions import length, substring

result = df.withColumn("name_length", ___(col("name")))
lengths = [row["name_length"] for row in result.collect()]
assert lengths == [5, 3, 9], f"Expected [5, 3, 9], got {lengths}"
print("✓ Calculated string lengths")

# Get first 3 characters (substring is 1-indexed!)
result2 = df.withColumn("first_three", ___(col("name"), 1, 3))
firsts = [row["first_three"] for row in result2.collect()]
assert firsts == ["Ali", "Bob", "Cha"], f"Expected ['Ali', 'Bob', 'Cha'], got {firsts}"
print("✓ Extracted first 3 characters")

print("\\n🎉 Koan complete! You've learned substring and length.")`,
    solution: `result = df.withColumn("name_length", length(col("name")))\nresult2 = df.withColumn("first_three", substring(col("name"), 1, 3))`,
    hints: [
      "length() returns the number of characters",
      "substring(col, start, length) extracts a portion",
      "Note: substring is 1-indexed, not 0-indexed!"
    ]
  },
  {
    id: 16,
    title: "String Functions - Trim and Pad",
    category: "String Functions",
    description: "Remove or add whitespace. Replace ___ with the correct code.",
    setup: `
data = [("  Alice  ",), ("Bob",), (" Charlie ",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Trim whitespace from both sides
from pyspark.sql.functions import trim, ltrim, rtrim, lpad, rpad

result = df.withColumn("trimmed", ___(col("name")))
trimmed = [row["trimmed"] for row in result.collect()]
assert trimmed == ["Alice", "Bob", "Charlie"], f"Expected trimmed names, got {trimmed}"
print("✓ Trimmed whitespace")

# Pad names to 10 characters with asterisks
result2 = df.withColumn("trimmed", trim(col("name")))
result2 = result2.withColumn("padded", ___(col("trimmed"), 10, "*"))
assert result2.collect()[1]["padded"] == "*******Bob"
print("✓ Left-padded with asterisks")

print("\\n🎉 Koan complete! You've learned trim and pad functions.")`,
    solution: `result = df.withColumn("trimmed", trim(col("name")))\nresult2 = result2.withColumn("padded", lpad(col("trimmed"), 10, "*"))`,
    hints: [
      "trim() removes whitespace from both sides",
      "lpad(col, length, pad_string) pads on the left",
      "rpad() pads on the right"
    ]
  },

  // ==================== AGGREGATIONS ====================
  {
    id: 17,
    title: "Grouping and Aggregating",
    category: "Aggregations",
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
    id: 18,
    title: "Multiple Aggregations",
    category: "Aggregations",
    description: "Calculate multiple aggregations at once. Replace ___ with the correct code.",
    setup: `
data = [
    ("Sales", 5000), ("Sales", 4500), ("Sales", 6000),
    ("Engineering", 6000), ("Engineering", 6500)
]
df = spark.createDataFrame(data, ["department", "salary"])
`,
    template: `# Calculate min, max, avg, and count per department
from pyspark.sql.functions import min, max, avg, count

result = df.groupBy("department").agg(
    ___("salary").alias("min_salary"),
    ___("salary").alias("max_salary"),
    avg("salary").alias("avg_salary"),
    ___("salary").alias("emp_count")
)

sales = result.filter(col("department") == "Sales").collect()[0]

assert sales["min_salary"] == 4500, f"Min should be 4500, got {sales['min_salary']}"
print("✓ Min salary correct")

assert sales["max_salary"] == 6000, f"Max should be 6000, got {sales['max_salary']}"
print("✓ Max salary correct")

assert sales["emp_count"] == 3, f"Count should be 3, got {sales['emp_count']}"
print("✓ Employee count correct")

print("\\n🎉 Koan complete! You've learned multiple aggregations.")`,
    solution: `result = df.groupBy("department").agg(min("salary").alias("min_salary"), max("salary").alias("max_salary"), avg("salary").alias("avg_salary"), count("salary").alias("emp_count"))`,
    hints: [
      "min() finds the minimum value",
      "max() finds the maximum value",
      "count() counts the number of values"
    ]
  },
  {
    id: 19,
    title: "Aggregate Without Grouping",
    category: "Aggregations",
    description: "Calculate aggregates across the entire DataFrame. Replace ___ with the correct code.",
    setup: `
data = [(100,), (200,), (300,), (400,), (500,)]
df = spark.createDataFrame(data, ["value"])
`,
    template: `# Calculate sum of all values without grouping
from pyspark.sql.functions import sum as spark_sum, avg, count

result = df.___(spark_sum("value").alias("total"))

total = result.collect()[0]["total"]
assert total == 1500, f"Expected 1500, got {total}"
print("✓ Sum calculated: 1500")

# Calculate multiple aggregates
result2 = df.agg(
    spark_sum("value").alias("total"),
    ___("value").alias("average"),
    count("value").alias("num_rows")
)

row = result2.collect()[0]
assert row["average"] == 300.0, f"Expected 300.0, got {row['average']}"
assert row["num_rows"] == 5, f"Expected 5, got {row['num_rows']}"
print("✓ Multiple aggregates calculated")

print("\\n🎉 Koan complete! You've learned global aggregations.")`,
    solution: `result = df.agg(spark_sum("value").alias("total"))\nresult2 = df.agg(spark_sum("value").alias("total"), avg("value").alias("average"), count("value").alias("num_rows"))`,
    hints: [
      "Use .agg() directly on the DataFrame without groupBy",
      "Note: we import sum as spark_sum to avoid conflict with Python's built-in",
      "avg() calculates the average"
    ]
  },

  // ==================== JOINS ====================
  {
    id: 20,
    title: "Inner Join",
    category: "Joins",
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

print("\\n🎉 Koan complete! You've learned inner joins.")`,
    solution: `result = employees.join(departments, "dept_id", "inner")`,
    hints: [
      "The method to combine DataFrames is called 'join'",
      "Specify the column to join on as a string",
      "The join type is already provided: 'inner'"
    ]
  },
  {
    id: 21,
    title: "Left Outer Join",
    category: "Joins",
    description: "Keep all rows from the left DataFrame, even without matches. Replace ___ with the correct code.",
    setup: `
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 999)  # No matching department!
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Sales")
], ["dept_id", "dept_name"])
`,
    template: `# Left join to keep all employees, even without matching dept
result = employees.join(departments, "dept_id", "___")

# Should have 3 rows (all employees kept)
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ All employees kept")

# Charlie should have null department name
charlie = result.filter(col("name") == "Charlie").collect()[0]
assert charlie["dept_name"] is None, f"Expected None, got {charlie['dept_name']}"
print("✓ Charlie has no matching department (null)")

print("\\n🎉 Koan complete! You've learned left outer joins.")`,
    solution: `result = employees.join(departments, "dept_id", "left")`,
    hints: [
      "Left outer join keeps all rows from the left DataFrame",
      "The join type can be 'left' or 'left_outer'"
    ]
  },
  {
    id: 22,
    title: "Join on Multiple Columns",
    category: "Joins",
    description: "Join on multiple columns. Replace ___ with the correct code.",
    setup: `
orders = spark.createDataFrame([
    ("2024", "Q1", "Alice", 100),
    ("2024", "Q2", "Alice", 150),
    ("2024", "Q1", "Bob", 200)
], ["year", "quarter", "rep", "amount"])

targets = spark.createDataFrame([
    ("2024", "Q1", 120),
    ("2024", "Q2", 140)
], ["year", "quarter", "target"])
`,
    template: `# Join on both year and quarter
result = orders.join(targets, [___, ___], "inner")

# Should have 3 rows
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Joined on multiple columns")

# Check that Alice Q1 has target 120
alice_q1 = result.filter((col("rep") == "Alice") & (col("quarter") == "Q1")).collect()[0]
assert alice_q1["target"] == 120, f"Expected target 120, got {alice_q1['target']}"
print("✓ Targets matched correctly")

print("\\n🎉 Koan complete! You've learned multi-column joins.")`,
    solution: `result = orders.join(targets, ["year", "quarter"], "inner")`,
    hints: [
      "Pass a list of column names to join on multiple columns",
      "The columns must match in both DataFrames"
    ]
  },

  // ==================== WINDOW FUNCTIONS ====================
  {
    id: 23,
    title: "Window Functions - Running Total",
    category: "Window Functions",
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

print("\\n🎉 Koan complete! You've learned window running totals.")`,
    solution: `window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)\nresult = df.withColumn("running_total", spark_sum("sales").over(window_spec))`,
    hints: [
      "For a running total, you want from the start up to the 'currentRow'",
      "Use spark_sum (aliased from sum) to add up values",
      "The .over() method applies the function to the window"
    ]
  },
  {
    id: 24,
    title: "Window Functions - Row Number",
    category: "Window Functions",
    description: "Assign sequential row numbers within groups. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 5500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["dept", "name", "salary"])
`,
    template: `# Rank employees within each department by salary (highest first)
window_spec = Window.partitionBy("___").orderBy(col("salary").desc())

result = df.withColumn("rank", ___().___(window_spec))

# Check rankings
eng = result.filter(col("dept") == "Engineering").orderBy("rank").collect()
assert eng[0]["name"] == "Diana", f"Diana should be #1 in Engineering, got {eng[0]['name']}"
assert eng[0]["rank"] == 1
print("✓ Diana is #1 in Engineering ($6500)")

assert eng[1]["name"] == "Charlie", f"Charlie should be #2, got {eng[1]['name']}"
print("✓ Charlie is #2 in Engineering ($6000)")

print("\\n🎉 Koan complete! You've learned row_number().")`,
    solution: `window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())\nresult = df.withColumn("rank", row_number().over(window_spec))`,
    hints: [
      "partitionBy creates groups like groupBy but for windows",
      "row_number() assigns 1, 2, 3... within each partition",
      "Use .over() to apply to the window specification"
    ]
  },
  {
    id: 25,
    title: "Window Functions - Lag and Lead",
    category: "Window Functions",
    description: "Access previous or next row values. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 120),
    ("2024-01-04", 200)
]
df = spark.createDataFrame(data, ["date", "price"])
`,
    template: `# Get yesterday's price and calculate daily change
window_spec = Window.orderBy("date")

result = df.withColumn("prev_price", ___("price", 1).over(window_spec))
result = result.withColumn("change", col("price") - col("prev_price"))

rows = result.orderBy("date").collect()

# First row has no previous
assert rows[0]["prev_price"] is None, "First row should have no prev_price"
print("✓ First row has no previous")

# Second row: prev=100, change=50
assert rows[1]["prev_price"] == 100, f"Expected prev=100, got {rows[1]['prev_price']}"
assert rows[1]["change"] == 50, f"Expected change=50, got {rows[1]['change']}"
print("✓ Day 2: prev=100, change=+50")

# Get tomorrow's price
result2 = df.withColumn("next_price", ___("price", 1).over(window_spec))
rows2 = result2.orderBy("date").collect()
assert rows2[0]["next_price"] == 150, f"Expected next=150, got {rows2[0]['next_price']}"
print("✓ Lead shows next day's price")

print("\\n🎉 Koan complete! You've learned lag and lead.")`,
    solution: `result = df.withColumn("prev_price", lag("price", 1).over(window_spec))\nresult2 = df.withColumn("next_price", lead("price", 1).over(window_spec))`,
    hints: [
      "lag() looks at previous rows",
      "lead() looks at following rows",
      "The second argument is how many rows to look back/forward"
    ]
  },

  // ==================== NULL HANDLING ====================
  {
    id: 26,
    title: "Handling Nulls - Detection",
    category: "Null Handling",
    description: "Detect and filter null values. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", None), ("Charlie", 29), (None, 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Filter to rows where age is not null
from pyspark.sql.functions import col, isnan, isnull

result = df.filter(col("age").___())

assert result.count() == 3, f"Expected 3 rows with age, got {result.count()}"
print("✓ Filtered to non-null ages")

# Filter to rows where age IS null
nulls = df.filter(col("age").___())
assert nulls.count() == 1, f"Expected 1 null age, got {nulls.count()}"
print("✓ Found rows with null age")

# Check for null name
null_names = df.filter(col("name").isNull())
assert null_names.count() == 1
print("✓ Found row with null name")

print("\\n🎉 Koan complete! You've learned null detection.")`,
    solution: `result = df.filter(col("age").isNotNull())\nnulls = df.filter(col("age").isNull())`,
    hints: [
      "isNotNull() returns true for non-null values",
      "isNull() returns true for null values",
      "These are methods on Column objects"
    ]
  },
  {
    id: 27,
    title: "Handling Nulls - Fill and Drop",
    category: "Null Handling",
    description: "Replace or remove null values. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", None), (None, 29), ("Diana", None)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Fill null ages with 0
result = df.___(0, subset=["age"])

ages = [row["age"] for row in result.collect()]
assert None not in ages, "Should have no null ages"
assert ages.count(0) == 2, "Should have 2 zeros"
print("✓ Filled null ages with 0")

# Fill null names with "Unknown"
result2 = df.fillna("Unknown", subset=["name"])
names = [row["name"] for row in result2.collect()]
assert "Unknown" in names, "Should have Unknown name"
print("✓ Filled null names")

# Drop rows with ANY null values
result3 = df.___()
assert result3.count() == 1, f"Expected 1 complete row, got {result3.count()}"
print("✓ Dropped rows with nulls")

print("\\n🎉 Koan complete! You've learned to handle nulls.")`,
    solution: `result = df.fillna(0, subset=["age"])\nresult3 = df.dropna()`,
    hints: [
      "fillna() replaces null values",
      "You can specify which columns with subset=",
      "dropna() removes rows with null values"
    ]
  },

  // ==================== ADVANCED ====================
  {
    id: 28,
    title: "Union DataFrames",
    category: "Advanced",
    description: "Combine DataFrames vertically. Replace ___ with the correct code.",
    setup: `
df1 = spark.createDataFrame([("Alice", 34), ("Bob", 45)], ["name", "age"])
df2 = spark.createDataFrame([("Charlie", 29), ("Diana", 52)], ["name", "age"])
`,
    template: `# Combine two DataFrames with the same schema
result = df1.___(df2)

assert result.count() == 4, f"Expected 4 rows, got {result.count()}"
print("✓ Combined DataFrames")

names = [row["name"] for row in result.collect()]
assert "Alice" in names and "Charlie" in names, "Should have names from both DFs"
print("✓ Contains data from both DataFrames")

print("\\n🎉 Koan complete! You've learned to union DataFrames.")`,
    solution: `result = df1.union(df2)`,
    hints: [
      "union() stacks DataFrames on top of each other",
      "Both DataFrames must have the same schema"
    ]
  },
  {
    id: 29,
    title: "Explode Arrays",
    category: "Advanced",
    description: "Expand array columns into multiple rows. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.functions import explode, split

data = [("Alice", "python,sql,spark"), ("Bob", "java,scala")]
df = spark.createDataFrame(data, ["name", "skills_str"])

# First split the string into an array
df = df.withColumn("skills", split(col("skills_str"), ","))
`,
    template: `# Explode the skills array into separate rows
from pyspark.sql.functions import explode

result = df.select("name", ___(col("skills")).alias("skill"))

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
print("✓ Exploded to 5 rows")

alice_skills = [row["skill"] for row in result.filter(col("name") == "Alice").collect()]
assert len(alice_skills) == 3, f"Alice should have 3 skills, got {len(alice_skills)}"
assert "spark" in alice_skills
print("✓ Alice has 3 skills including spark")

print("\\n🎉 Koan complete! You've learned to explode arrays.")`,
    solution: `result = df.select("name", explode(col("skills")).alias("skill"))`,
    hints: [
      "explode() turns each array element into a separate row",
      "The original row is duplicated for each array element"
    ]
  },
  {
    id: 30,
    title: "Pivot Tables",
    category: "Advanced",
    description: "Pivot data from rows to columns. Replace ___ with the correct code.",
    setup: `
data = [
    ("Alice", "Q1", 100), ("Alice", "Q2", 150),
    ("Bob", "Q1", 200), ("Bob", "Q2", 180)
]
df = spark.createDataFrame(data, ["name", "quarter", "sales"])
`,
    template: `# Pivot to get quarters as columns
from pyspark.sql.functions import sum as spark_sum

result = df.groupBy("name").___(___).agg(spark_sum("sales"))

# Should have columns: name, Q1, Q2
assert "Q1" in result.columns, "Should have Q1 column"
assert "Q2" in result.columns, "Should have Q2 column"
print("✓ Pivoted quarters to columns")

alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["Q1"] == 100, f"Expected Q1=100, got {alice['Q1']}"
assert alice["Q2"] == 150, f"Expected Q2=150, got {alice['Q2']}"
print("✓ Values correctly placed in columns")

print("\\n🎉 Koan complete! You've learned pivot tables.")`,
    solution: `result = df.groupBy("name").pivot("quarter").agg(spark_sum("sales"))`,
    hints: [
      "pivot() goes after groupBy()",
      "Specify which column's values become new column names"
    ]
  }
];

// Enhanced PySpark shim with more functions
const PYSPARK_SHIM = `
import pandas as pd
from typing import List, Any, Optional, Union
from dataclasses import dataclass
import json

class Row(dict):
    """PySpark-like Row class"""
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

class Column:
    """PySpark-like Column class for expressions"""
    def __init__(self, name: str, expr: Optional[str] = None):
        self.name = name
        self.expr = expr or name
        self._alias = None
    
    def __gt__(self, other):
        return Column(self.name, f"({self.expr}) > {repr(other)}")
    
    def __lt__(self, other):
        return Column(self.name, f"({self.expr}) < {repr(other)}")
    
    def __ge__(self, other):
        return Column(self.name, f"({self.expr}) >= {repr(other)}")
    
    def __le__(self, other):
        return Column(self.name, f"({self.expr}) <= {repr(other)}")
    
    def __eq__(self, other):
        if other is None:
            return Column(self.name, f"({self.expr}).isna()")
        return Column(self.name, f"({self.expr}) == {repr(other)}")
    
    def __ne__(self, other):
        if other is None:
            return Column(self.name, f"({self.expr}).notna()")
        return Column(self.name, f"({self.expr}) != {repr(other)}")
    
    def __mul__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) * ({other.expr})")
        return Column(self.name, f"({self.expr}) * {repr(other)}")
    
    def __add__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) + ({other.expr})")
        return Column(self.name, f"({self.expr}) + {repr(other)}")
    
    def __sub__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) - ({other.expr})")
        return Column(self.name, f"({self.expr}) - {repr(other)}")
    
    def __truediv__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) / ({other.expr})")
        return Column(self.name, f"({self.expr}) / {repr(other)}")
    
    def __and__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) & ({other.expr})")
        return Column(self.name, f"({self.expr}) & {repr(other)}")
    
    def __or__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) | ({other.expr})")
        return Column(self.name, f"({self.expr}) | {repr(other)}")
    
    def alias(self, name: str):
        new_col = Column(self.name, self.expr)
        new_col._alias = name
        if hasattr(self, '_agg_func'):
            new_col._agg_func = self._agg_func
            new_col._source_col = self._source_col
        if hasattr(self, '_round_decimals'):
            new_col._round_decimals = self._round_decimals
        if hasattr(self, '_transform_func'):
            new_col._transform_func = self._transform_func
        if hasattr(self, '_is_window_func'):
            new_col._is_window_func = self._is_window_func
            new_col._window = self._window
        return new_col
    
    def over(self, window):
        new_col = Column(self.name, self.expr)
        new_col._window = window
        new_col._is_window_func = True
        if hasattr(self, '_agg_func'):
            new_col._agg_func = self._agg_func
            new_col._source_col = self._source_col
        if hasattr(self, '_window_func'):
            new_col._window_func = self._window_func
            new_col._window_args = getattr(self, '_window_args', [])
        return new_col
    
    def isNull(self):
        return Column(self.name, f"({self.expr}).isna()")
    
    def isNotNull(self):
        return Column(self.name, f"({self.expr}).notna()")
    
    def cast(self, dtype: str):
        new_col = Column(self.name, self.expr)
        new_col._cast_type = dtype
        return new_col
    
    def desc(self):
        new_col = Column(self.name, self.expr)
        new_col._sort_desc = True
        return new_col
    
    def asc(self):
        new_col = Column(self.name, self.expr)
        new_col._sort_desc = False
        return new_col

class WhenColumn(Column):
    """Column with when/otherwise logic"""
    def __init__(self):
        super().__init__("_when")
        self._conditions = []
    
    def when(self, condition, value):
        new_col = WhenColumn()
        new_col._conditions = self._conditions + [(condition, value)]
        return new_col
    
    def otherwise(self, value):
        new_col = WhenColumn()
        new_col._conditions = self._conditions
        new_col._otherwise = value
        return new_col

class WindowSpec:
    """PySpark-like Window specification"""
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
        new_spec._row_start = self._row_start
        new_spec._row_end = self._row_end
        return new_spec
    
    def orderBy(self, *cols):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = list(cols)
        new_spec._row_start = self._row_start
        new_spec._row_end = self._row_end
        return new_spec
    
    def rowsBetween(self, start, end):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = self._order_cols
        new_spec._row_start = start
        new_spec._row_end = end
        return new_spec

class Window:
    """PySpark-like Window class"""
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"
    
    @staticmethod
    def partitionBy(*cols):
        spec = WindowSpec()
        return spec.partitionBy(*cols)
    
    @staticmethod
    def orderBy(*cols):
        spec = WindowSpec()
        return spec.orderBy(*cols)

class GroupedData:
    """PySpark-like GroupedData class"""
    def __init__(self, df: 'DataFrame', group_cols: List[str]):
        self._df = df
        self._group_cols = group_cols
        self._pivot_col = None
    
    def pivot(self, col_name: str):
        new_gd = GroupedData(self._df, self._group_cols)
        new_gd._pivot_col = col_name
        return new_gd
    
    def agg(self, *exprs):
        pdf = self._df._pdf.copy()
        
        if self._pivot_col:
            # Handle pivot aggregation
            pivot_vals = pdf[self._pivot_col].unique()
            result_data = {}
            
            for group_col in self._group_cols:
                result_data[group_col] = []
            for pv in pivot_vals:
                result_data[pv] = []
            
            for group_vals, group_df in pdf.groupby(self._group_cols):
                if not isinstance(group_vals, tuple):
                    group_vals = (group_vals,)
                for i, gc in enumerate(self._group_cols):
                    result_data[gc].append(group_vals[i])
                for pv in pivot_vals:
                    pv_df = group_df[group_df[self._pivot_col] == pv]
                    for expr in exprs:
                        if hasattr(expr, '_agg_func'):
                            source_col = expr._source_col
                            func = expr._agg_func
                            if func == 'sum':
                                val = pv_df[source_col].sum() if len(pv_df) > 0 else None
                            elif func == 'avg':
                                val = pv_df[source_col].mean() if len(pv_df) > 0 else None
                            else:
                                val = None
                            result_data[pv].append(val)
            
            return DataFrame(pd.DataFrame(result_data))
        
        # Regular aggregation
        agg_dict = {}
        rename_dict = {}
        
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg':
                    agg_dict[source_col] = 'mean'
                elif func == 'sum':
                    agg_dict[source_col] = 'sum'
                elif func == 'count':
                    agg_dict[source_col] = 'count'
                elif func == 'min':
                    agg_dict[source_col] = 'min'
                elif func == 'max':
                    agg_dict[source_col] = 'max'
                
                rename_dict[source_col] = col_name
        
        grouped = pdf.groupby(self._group_cols, as_index=False)
        
        # Build aggregation result
        agg_results = {}
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg':
                    agg_results[col_name] = grouped[source_col].mean()[source_col]
                elif func == 'sum':
                    agg_results[col_name] = grouped[source_col].sum()[source_col]
                elif func == 'count':
                    agg_results[col_name] = grouped[source_col].count()[source_col]
                elif func == 'min':
                    agg_results[col_name] = grouped[source_col].min()[source_col]
                elif func == 'max':
                    agg_results[col_name] = grouped[source_col].max()[source_col]
                
                if hasattr(expr, '_round_decimals'):
                    agg_results[col_name] = agg_results[col_name].round(expr._round_decimals)
        
        result_pdf = grouped[self._group_cols].first()
        for col_name, values in agg_results.items():
            result_pdf[col_name] = values.values
        
        return DataFrame(result_pdf)

class DataFrame:
    """PySpark-like DataFrame backed by pandas"""
    def __init__(self, pdf: pd.DataFrame):
        self._pdf = pdf
    
    @property
    def columns(self) -> List[str]:
        return list(self._pdf.columns)
    
    def count(self) -> int:
        return len(self._pdf)
    
    def show(self, n: int = 20, truncate: bool = True):
        print(self._pdf.head(n).to_string())
    
    def collect(self) -> List[Row]:
        rows = []
        for _, row in self._pdf.iterrows():
            row_dict = {}
            for k, v in row.items():
                if pd.isna(v):
                    row_dict[k] = None
                else:
                    row_dict[k] = v
            rows.append(Row(**row_dict))
        return rows
    
    def first(self):
        if len(self._pdf) == 0:
            return None
        row = self._pdf.iloc[0]
        row_dict = {}
        for k, v in row.items():
            if pd.isna(v):
                row_dict[k] = None
            else:
                row_dict[k] = v
        return Row(**row_dict)
    
    def select(self, *cols) -> 'DataFrame':
        result_cols = {}
        for c in cols:
            if isinstance(c, str):
                result_cols[c] = self._pdf[c]
            elif isinstance(c, Column):
                col_name = c._alias or c.name
                
                if hasattr(c, '_transform_func'):
                    result_cols[col_name] = c._transform_func(self._pdf)
                elif hasattr(c, '_is_window_func') and c._is_window_func:
                    result_cols[col_name] = self._apply_window_func(c)
                else:
                    try:
                        result_cols[col_name] = self._pdf.eval(c.expr)
                    except:
                        result_cols[col_name] = self._pdf[c.name]
        return DataFrame(pd.DataFrame(result_cols))
    
    def _apply_window_func(self, col):
        window = col._window
        pdf = self._pdf.copy()
        
        # Sort if needed
        if window._order_cols:
            sort_cols = []
            ascending = []
            for oc in window._order_cols:
                if isinstance(oc, str):
                    sort_cols.append(oc)
                    ascending.append(True)
                else:
                    sort_cols.append(oc.name)
                    ascending.append(not getattr(oc, '_sort_desc', False))
            pdf = pdf.sort_values(sort_cols, ascending=ascending)
        
        # Handle different window functions
        if hasattr(col, '_window_func'):
            func = col._window_func
            args = getattr(col, '_window_args', [])
            
            if func == 'row_number':
                if window._partition_cols:
                    return pdf.groupby(window._partition_cols).cumcount() + 1
                else:
                    return pd.Series(range(1, len(pdf) + 1))
            
            elif func == 'lag':
                source_col = args[0]
                offset = args[1] if len(args) > 1 else 1
                if window._partition_cols:
                    return pdf.groupby(window._partition_cols)[source_col].shift(offset)
                else:
                    return pdf[source_col].shift(offset)
            
            elif func == 'lead':
                source_col = args[0]
                offset = args[1] if len(args) > 1 else 1
                if window._partition_cols:
                    return pdf.groupby(window._partition_cols)[source_col].shift(-offset)
                else:
                    return pdf[source_col].shift(-offset)
        
        elif hasattr(col, '_agg_func'):
            func = col._agg_func
            source_col = col._source_col
            
            if func == 'sum':
                if window._partition_cols:
                    return pdf.groupby(window._partition_cols)[source_col].cumsum()
                else:
                    return pdf[source_col].cumsum()
        
        return pdf[col.name]
    
    def filter(self, condition: Column) -> 'DataFrame':
        if isinstance(condition, Column):
            mask = self._pdf.eval(condition.expr)
            return DataFrame(self._pdf[mask].reset_index(drop=True))
        return self
    
    def where(self, condition: Column) -> 'DataFrame':
        return self.filter(condition)
    
    def withColumn(self, name: str, col: Column) -> 'DataFrame':
        pdf = self._pdf.copy()
        
        if isinstance(col, WhenColumn):
            # Handle when/otherwise
            result = pd.Series([None] * len(pdf))
            for cond, val in col._conditions:
                mask = pdf.eval(cond.expr)
                result = result.where(~mask, val)
            if hasattr(col, '_otherwise'):
                result = result.fillna(col._otherwise)
            pdf[name] = result
            return DataFrame(pdf)
        
        if hasattr(col, '_is_window_func') and col._is_window_func:
            window = col._window
            
            # Sort by order columns if specified
            if window._order_cols:
                sort_cols = []
                ascending = []
                for oc in window._order_cols:
                    if isinstance(oc, str):
                        sort_cols.append(oc)
                        ascending.append(True)
                    else:
                        sort_cols.append(oc.name)
                        ascending.append(not getattr(oc, '_sort_desc', False))
                pdf = pdf.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
            
            if hasattr(col, '_window_func'):
                func = col._window_func
                args = getattr(col, '_window_args', [])
                
                if func == 'row_number':
                    if window._partition_cols:
                        pdf[name] = pdf.groupby(window._partition_cols).cumcount() + 1
                    else:
                        pdf[name] = range(1, len(pdf) + 1)
                
                elif func == 'lag':
                    source_col = args[0]
                    offset = args[1] if len(args) > 1 else 1
                    if window._partition_cols:
                        pdf[name] = pdf.groupby(window._partition_cols)[source_col].shift(offset)
                    else:
                        pdf[name] = pdf[source_col].shift(offset)
                
                elif func == 'lead':
                    source_col = args[0]
                    offset = args[1] if len(args) > 1 else 1
                    if window._partition_cols:
                        pdf[name] = pdf.groupby(window._partition_cols)[source_col].shift(-offset)
                    else:
                        pdf[name] = pdf[source_col].shift(-offset)
            
            elif hasattr(col, '_agg_func') and col._agg_func == 'sum':
                source_col = col._source_col
                if window._partition_cols:
                    pdf[name] = pdf.groupby(window._partition_cols)[source_col].cumsum()
                else:
                    pdf[name] = pdf[source_col].cumsum()
            
            return DataFrame(pdf)
        
        if hasattr(col, '_transform_func'):
            pdf[name] = col._transform_func(pdf)
            return DataFrame(pdf)
        
        if hasattr(col, '_cast_type'):
            dtype_map = {
                'integer': 'int64',
                'int': 'int64',
                'double': 'float64',
                'float': 'float64',
                'string': 'str',
                'boolean': 'bool'
            }
            source_val = pdf.eval(col.expr)
            target_dtype = dtype_map.get(col._cast_type, col._cast_type)
            pdf[name] = source_val.astype(target_dtype)
            return DataFrame(pdf)
        
        try:
            pdf[name] = pdf.eval(col.expr)
        except:
            if col.name in pdf.columns:
                pdf[name] = pdf[col.name]
        return DataFrame(pdf)
    
    def withColumnRenamed(self, existing: str = None, new: str = None, **kwargs) -> 'DataFrame':
        pdf = self._pdf.copy()
        if existing and new:
            pdf = pdf.rename(columns={existing: new})
        for old_name, new_name in kwargs.items():
            pdf = pdf.rename(columns={old_name: new_name})
        return DataFrame(pdf)
    
    def groupBy(self, *cols) -> GroupedData:
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return GroupedData(self, col_names)
    
    def agg(self, *exprs) -> 'DataFrame':
        """Aggregate without grouping"""
        result = {}
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg':
                    result[col_name] = [self._pdf[source_col].mean()]
                elif func == 'sum':
                    result[col_name] = [self._pdf[source_col].sum()]
                elif func == 'count':
                    result[col_name] = [self._pdf[source_col].count()]
                elif func == 'min':
                    result[col_name] = [self._pdf[source_col].min()]
                elif func == 'max':
                    result[col_name] = [self._pdf[source_col].max()]
        
        return DataFrame(pd.DataFrame(result))
    
    def join(self, other: 'DataFrame', on: Union[str, List[str]], how: str = 'inner') -> 'DataFrame':
        result = self._pdf.merge(other._pdf, on=on, how=how)
        return DataFrame(result)
    
    def orderBy(self, *cols, ascending=True) -> 'DataFrame':
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
    
    def sort(self, *cols, ascending=True) -> 'DataFrame':
        return self.orderBy(*cols, ascending=ascending)
    
    def drop(self, *cols) -> 'DataFrame':
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.drop(columns=col_names))
    
    def distinct(self) -> 'DataFrame':
        return DataFrame(self._pdf.drop_duplicates().reset_index(drop=True))
    
    def dropDuplicates(self, subset=None) -> 'DataFrame':
        return DataFrame(self._pdf.drop_duplicates(subset=subset).reset_index(drop=True))
    
    def limit(self, n: int) -> 'DataFrame':
        return DataFrame(self._pdf.head(n).copy())
    
    def union(self, other: 'DataFrame') -> 'DataFrame':
        return DataFrame(pd.concat([self._pdf, other._pdf], ignore_index=True))
    
    def unionByName(self, other: 'DataFrame') -> 'DataFrame':
        return DataFrame(pd.concat([self._pdf, other._pdf], ignore_index=True))
    
    def fillna(self, value, subset=None) -> 'DataFrame':
        pdf = self._pdf.copy()
        if subset:
            for col in subset:
                pdf[col] = pdf[col].fillna(value)
        else:
            pdf = pdf.fillna(value)
        return DataFrame(pdf)
    
    def dropna(self, how='any', subset=None) -> 'DataFrame':
        return DataFrame(self._pdf.dropna(how=how, subset=subset).reset_index(drop=True))
    
    def toPandas(self) -> pd.DataFrame:
        return self._pdf.copy()

class SparkSession:
    """PySpark-like SparkSession"""
    def __init__(self):
        pass
    
    def createDataFrame(self, data: List[tuple], schema: List[str]) -> DataFrame:
        pdf = pd.DataFrame(data, columns=schema)
        return DataFrame(pdf)

# Create global spark session
spark = SparkSession()

# ============ FUNCTIONS ============

def col(name: str) -> Column:
    return Column(name)

def lit(value: Any) -> Column:
    c = Column("_lit", repr(value))
    c._lit_value = value
    def transform(pdf):
        return pd.Series([value] * len(pdf))
    c._transform_func = transform
    return c

def when(condition: Column, value: Any) -> WhenColumn:
    w = WhenColumn()
    return w.when(condition, value)

# Aggregation functions
def avg(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'avg'
    c._source_col = col_name
    return c

def sum(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'sum'
    c._source_col = col_name
    return c

def count(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'count'
    c._source_col = col_name
    return c

def min(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'min'
    c._source_col = col_name
    return c

def max(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'max'
    c._source_col = col_name
    return c

def round(col_expr: Column, decimals: int = 0) -> Column:
    new_col = Column(col_expr.name, col_expr.expr)
    if hasattr(col_expr, '_agg_func'):
        new_col._agg_func = col_expr._agg_func
        new_col._source_col = col_expr._source_col
    new_col._round_decimals = decimals
    if hasattr(col_expr, '_alias'):
        new_col._alias = col_expr._alias
    return new_col

# String functions
def upper(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.upper()
    c._transform_func = transform
    return c

def lower(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.lower()
    c._transform_func = transform
    return c

def initcap(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.title()
    c._transform_func = transform
    return c

def concat(*cols) -> Column:
    c = Column("_concat")
    def transform(pdf):
        result = None
        for col_item in cols:
            if hasattr(col_item, '_lit_value'):
                val = pd.Series([col_item._lit_value] * len(pdf))
            elif isinstance(col_item, Column):
                val = pdf[col_item.name]
            else:
                val = pdf[col_item]
            if result is None:
                result = val.astype(str)
            else:
                result = result + val.astype(str)
        return result
    c._transform_func = transform
    return c

def concat_ws(sep: str, *cols) -> Column:
    c = Column("_concat_ws")
    def transform(pdf):
        vals = []
        for col_item in cols:
            if hasattr(col_item, '_lit_value'):
                vals.append(pd.Series([col_item._lit_value] * len(pdf)).astype(str))
            elif isinstance(col_item, Column):
                vals.append(pdf[col_item.name].astype(str))
            else:
                vals.append(pdf[col_item].astype(str))
        return vals[0].str.cat(vals[1:], sep=sep)
    c._transform_func = transform
    return c

def length(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.len()
    c._transform_func = transform
    return c

def substring(col_expr: Column, start: int, length: int) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str[start-1:start-1+length]
    c._transform_func = transform
    return c

def trim(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.strip()
    c._transform_func = transform
    return c

def ltrim(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.lstrip()
    c._transform_func = transform
    return c

def rtrim(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.rstrip()
    c._transform_func = transform
    return c

def lpad(col_expr: Column, length: int, pad: str) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.rjust(length, pad)
    c._transform_func = transform
    return c

def rpad(col_expr: Column, length: int, pad: str) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.ljust(length, pad)
    c._transform_func = transform
    return c

def split(col_expr: Column, pattern: str) -> Column:
    c = Column(col_expr.name)
    def transform(pdf):
        return pdf[col_expr.name].str.split(pattern)
    c._transform_func = transform
    return c

def explode(col_expr: Column) -> Column:
    c = Column(col_expr.name)
    c._explode = True
    c._source_col = col_expr.name
    def transform(pdf):
        return pdf[col_expr.name].explode()
    c._transform_func = transform
    return c

# Window functions
def row_number() -> Column:
    c = Column("_row_number")
    c._window_func = 'row_number'
    return c

def lag(col_name: str, offset: int = 1) -> Column:
    c = Column(col_name)
    c._window_func = 'lag'
    c._window_args = [col_name, offset]
    return c

def lead(col_name: str, offset: int = 1) -> Column:
    c = Column(col_name)
    c._window_func = 'lead'
    c._window_args = [col_name, offset]
    return c

# Null functions
def isnull(col_expr: Column) -> Column:
    return col_expr.isNull()

def isnan(col_expr: Column) -> Column:
    c = Column(col_expr.name, f"{col_expr.name}.isna()")
    return c

# Make spark_sum available
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
  
  // Filter koans by category
  const filteredKoans = selectedCategory 
    ? KOANS.filter(k => k.category === selectedCategory)
    : KOANS;

  // Initialize Pyodide
  useEffect(() => {
    async function initPyodide() {
      setIsLoading(true);
      try {
        const pyodideInstance = await window.loadPyodide({
          indexURL: "https://cdn.jsdelivr.net/pyodide/v0.24.1/full/"
        });
        await pyodideInstance.loadPackage(['pandas']);
        await pyodideInstance.runPythonAsync(PYSPARK_SHIM);
        
        setPyodide(pyodideInstance);
        setOutput('✓ PySpark environment ready!\n\nClick "Run Code" to test your solution.');
      } catch (error) {
        setOutput(`Error loading Python environment: ${error.message}`);
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

  // Update code when koan changes
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
    setOutput('Running...\n');
    
    try {
      await pyodide.runPythonAsync(PYSPARK_SHIM);
      await pyodide.runPythonAsync(koan.setup);
      
      await pyodide.runPythonAsync(`
import sys
from io import StringIO
_stdout_capture = StringIO()
sys.stdout = _stdout_capture
`);
      
      await pyodide.runPythonAsync(code);
      
      const capturedOutput = await pyodide.runPythonAsync(`
sys.stdout = sys.__stdout__
_stdout_capture.getvalue()
`);
      
      setOutput(capturedOutput);
      
      if (capturedOutput.includes('🎉 Koan complete!')) {
        setCompletedKoans(prev => new Set([...prev, currentKoan]));
      }
    } catch (error) {
      setOutput(`Error:\n${error.message}\n\nTip: Check your syntax and make sure you replaced all ___ placeholders.`);
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
          
          {/* Progress bar */}
          <div className="mb-4">
            <div className="flex justify-between text-sm text-gray-500 mb-1">
              <span>Progress</span>
              <span>{completedKoans.size}/{KOANS.length}</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div 
                className="bg-orange-600 h-2 rounded-full transition-all"
                style={{ width: `${(completedKoans.size / KOANS.length) * 100}%` }}
              />
            </div>
          </div>
          
          {/* Category filter */}
          <div className="mb-4">
            <button
              onClick={() => setSelectedCategory(null)}
              className={`w-full text-left px-3 py-2 rounded-lg text-sm mb-1 transition-colors ${
                selectedCategory === null 
                  ? 'bg-orange-600 text-white' 
                  : 'text-gray-400 hover:bg-gray-800'
              }`}
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
                  className={`w-full text-left px-3 py-2 rounded-lg text-sm mb-1 transition-colors flex justify-between ${
                    selectedCategory === cat 
                      ? 'bg-orange-600 text-white' 
                      : 'text-gray-400 hover:bg-gray-800'
                  }`}
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
                  className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors flex items-center gap-2 ${
                    globalIndex === currentKoan
                      ? 'bg-gray-800 text-white'
                      : 'text-gray-400 hover:bg-gray-800/50'
                  }`}
                >
                  <span className={`w-5 h-5 rounded flex items-center justify-center text-xs ${
                    completedKoans.has(globalIndex)
                      ? 'bg-green-600 text-white'
                      : 'bg-gray-700 text-gray-400'
                  }`}>
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
            {/* Koan header */}
            <div className="mb-6">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xs px-2 py-1 bg-gray-800 rounded text-gray-400">
                  {koan.category}
                </span>
                <span className="text-xs text-gray-600">Koan {koan.id} of {KOANS.length}</span>
              </div>
              <h2 className="text-2xl font-semibold text-white mb-2">{koan.title}</h2>
              <p className="text-gray-400">{koan.description}</p>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {/* Left column */}
              <div className="space-y-4">
                {/* Setup code */}
                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Setup (read-only)</span>
                  </div>
                  <pre className="p-4 text-sm text-gray-400 font-mono whitespace-pre-wrap overflow-x-auto">
                    {koan.setup.trim()}
                  </pre>
                </div>

                {/* Hints */}
                {showHints && (
                  <div className="bg-yellow-900/20 border border-yellow-800/50 rounded-lg p-4">
                    <h3 className="text-sm font-medium text-yellow-500 mb-2">
                      Hint {currentHint + 1}/{koan.hints.length}
                    </h3>
                    <p className="text-yellow-200/80">{koan.hints[currentHint]}</p>
                  </div>
                )}

                {/* Solution */}
                {showSolution && (
                  <div className="bg-green-900/20 border border-green-800/50 rounded-lg p-4">
                    <h3 className="text-sm font-medium text-green-500 mb-2">Solution</h3>
                    <pre className="text-sm text-green-200/80 font-mono whitespace-pre-wrap">
                      {koan.solution}
                    </pre>
                  </div>
                )}
              </div>

              {/* Right column */}
              <div className="space-y-4">
                {/* Editor */}
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

                {/* Controls */}
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

                {/* Output */}
                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Output</span>
                  </div>
                  <pre className={`p-4 h-48 overflow-auto font-mono text-sm whitespace-pre-wrap ${
                    output.includes('🎉') ? 'text-green-400' : 
                    output.includes('Error') ? 'text-red-400' : 'text-gray-300'
                  }`}>
                    {output || 'Output will appear here...'}
                  </pre>
                </div>

                {/* Navigation */}
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
