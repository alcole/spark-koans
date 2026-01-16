/**
 * Koan 106: OPTIMIZE and Z-ORDER
 * Category: Delta Lake
 */

const koan = {
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
  };

export default koan;
