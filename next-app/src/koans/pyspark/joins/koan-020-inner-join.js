/**
 * Koan 20: Inner Join
 * Category: Joins
 */

const koan = {
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
  };

export default koan;
