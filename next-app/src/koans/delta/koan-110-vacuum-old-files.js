/**
 * Koan 110: VACUUM Old Files
 * Category: Delta Lake
 */

export default {
    id: 110,
    title: "VACUUM Old Files",
    category: "Delta Lake",
    description: "Clean up old files from a Delta table. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create table and make several updates
for i in range(3):
    data = [(j, f"version_{i}") for j in range(10)]
    df = spark.createDataFrame(data, ["id", "data"])
    df.write.format("delta").mode("overwrite").save("/data/versions")
`,
    template: `# Get the Delta table  
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
print("\\n⚠️  Warning: VACUUM removes old version files!")
print("   Time travel to vacuumed versions will fail.")

print("\\n🎉 Koan complete! You understand VACUUM and retention.")`,
    solution: `result = dt.vacuum(retention_hours=168)`,
    hints: [
      "Use .vacuum() to clean up old files",
      "retention_hours specifies the minimum age of files to keep",
      "Be careful: this breaks time travel to old versions!"
    ]
  };
