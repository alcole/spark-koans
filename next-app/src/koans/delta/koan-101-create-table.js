/**
 * Koan 101: Creating a Delta Table
 * Category: Delta Lake
 * Difficulty: Beginner
 */

export default {
  id: 101,
  title: "Creating a Delta Table",
  category: "Delta Lake",
  difficulty: "beginner",
  description: "Learn how to create and read Delta tables. Replace ___ with the correct code.",

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

  solution: `df.write.format("delta").mode("overwrite").save("/data/employees")
result = spark.read.format("delta").load("/data/employees")
assert DeltaTable.isDeltaTable(spark, "/data/employees")`,

  hints: [
    "Use .format() to specify 'delta' as the format",
    "Reading uses the same format specification",
    "isDeltaTable() checks if a path contains a Delta table"
  ],

  examCoverage: ["DEA", "DEP", "DAA"],
  prerequisiteKoans: [1, 2, 3],
  nextKoans: [102],
};
