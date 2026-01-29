/**
 * Koan 104: MERGE - Selective Update
 * Category: Delta Lake
 */

const koan = {
    id: 104,
    title: "MERGE - Selective Update",
    category: "Delta Lake",
    description: "Use MERGE with specific column updates. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

# Create target table with more columns
target_data = [("Alice", 100, "2024-01-01"), ("Bob", 200, "2024-01-01")]
target_df = spark.createDataFrame(target_data, ["name", "balance", "last_updated"])
target_df.write.format("delta").save("/data/accounts")

# Source only has name and new balance
source_data = [("Alice", 500)]
source_df = spark.createDataFrame(source_data, ["name", "new_balance"])
`,
    template: `from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Merge with specific column mapping
dt.merge(
    source_df,
    "target.name = source.name"
).whenMatchedUpdate(set={
    "___": "source.new_balance",
    "last_updated": "'2024-06-01'"
}).execute()

# Verify Alice's balance was updated
result = dt.toDF()
alice = result.filter(col("name") == "Alice").collect()[0]

assert alice["balance"] == 500, f"Expected balance 500, got {alice['balance']}"
print("✓ Alice's balance updated to 500")

# Verify Bob was not changed
bob = result.filter(col("name") == "Bob").collect()[0]
assert bob["balance"] == 200, f"Bob should still have 200, got {bob['balance']}"
print("✓ Bob's balance unchanged")

print("\\n🎉 Koan complete! You've learned selective MERGE updates.")`,
    solution: `dt.merge(source_df, "target.name = source.name").whenMatchedUpdate(set={"balance": "source.new_balance", "last_updated": "'2024-06-01'"}).execute()`,
    hints: [
      "whenMatchedUpdate takes a 'set' parameter with column mappings",
      "Map target column names to source expressions",
      "The target column to update is 'balance'"
    ]
  };

export default koan;
