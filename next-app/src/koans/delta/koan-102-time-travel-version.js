/**
 * Koan 102: Time Travel - Version
 * Category: Delta Lake
 */

export default {
    id: 102,
    title: "Time Travel - Version",
    category: "Delta Lake",
    difficulty: "intermediate",
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
  };
