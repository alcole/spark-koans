/**
 * Koan 107: Delete with Condition
 * Category: Delta Lake
 */

const koan = {
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
    template: `from delta.tables import DeltaTable

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

print("\\n🎉 Koan complete! You've learned Delta delete operations.")`,
    solution: `dt.delete(condition="is_active == False")`,
    hints: [
      "Use .delete() with a condition string",
      "The condition filters which rows to delete"
    ]
  };

export default koan;
