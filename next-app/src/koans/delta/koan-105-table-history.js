/**
 * Koan 105: Table History
 * Category: Delta Lake
 */

export default {
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
  };
