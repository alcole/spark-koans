/**
 * Koan 103: MERGE - Upsert Pattern
 * Category: Delta Lake
 * Difficulty: Intermediate
 */

const koan = {
  id: 103,
  title: "MERGE - Upsert Pattern",
  category: "Delta Lake",
  difficulty: "intermediate",
  description: "Use MERGE to update existing rows and insert new ones. Replace ___ with the correct code.",

  setup: `
_reset_delta_tables()

# Create target table
target_data = [("Alice", 100), ("Bob", 200)]
target_df = spark.createDataFrame(target_data, ["name", "balance"])
target_df.write.format("delta").save("/data/accounts")

# Source data with updates and new records
source_data = [("Alice", 150), ("Charlie", 300)]  # Alice updated, Charlie is new
source_df = spark.createDataFrame(source_data, ["name", "balance"])
`,

  template: `from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Merge source into target
# When matched: update the balance
# When not matched: insert the new record
dt.___(
    source_df,
    "target.name = source.name"
).___().___().execute()

# Verify results
result = dt.toDF()
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Correct row count after merge")

# Check Alice was updated
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["balance"] == 150, f"Alice balance should be 150, got {alice['balance']}"
print("✓ Alice's balance updated to 150")

# Check Charlie was inserted
charlie = result.filter(col("name") == "Charlie").collect()[0]
assert charlie["balance"] == 300, f"Charlie balance should be 300"
print("✓ Charlie inserted with balance 300")

print("\\n🎉 Koan complete! You've mastered the MERGE upsert pattern.")`,

  solution: `dt.merge(source_df, "target.name = source.name").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,

  hints: [
    "Start with .merge(source, condition)",
    "Use whenMatchedUpdateAll() to update all columns",
    "Use whenNotMatchedInsertAll() to insert all columns",
    "Don't forget .execute() at the end"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [101],
  nextKoans: [104],
};

export default koan;
