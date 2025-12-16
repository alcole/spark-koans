/**
 * Koan 21: Left Outer Join
 * Category: Joins
 */

export default {
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
  };
