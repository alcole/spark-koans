/**
 * Koan 108: Update with Condition
 * Category: Delta Lake
 */

export default {
    id: 108,
    title: "Update with Condition",
    category: "Delta Lake",
    description: "Update rows in a Delta table based on a condition. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()

data = [("Alice", 100, "basic"), ("Bob", 200, "premium"), ("Charlie", 50, "basic")]
df = spark.createDataFrame(data, ["name", "balance", "tier"])
df.write.format("delta").save("/data/accounts")
`,
    template: `# Get the Delta table
dt = DeltaTable.forPath(spark, "/data/accounts")

# Give all premium users a bonus: set their balance to balance + 100
dt.___(
    condition="tier == 'premium'",
    set_values={"___": 300}  # Bob's new balance
)

# Verify Bob got the bonus
result = dt.toDF()
bob = result.filter(col("name") == "Bob").collect()[0]

assert bob["balance"] == 300, f"Bob should have 300, got {bob['balance']}"
print("✓ Premium user (Bob) balance updated")

# Verify others unchanged
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["balance"] == 100, f"Alice should still have 100"
print("✓ Basic users unchanged")

print("\\n🎉 Koan complete! You've learned Delta update operations.")`,
    solution: `dt.update(condition="tier == 'premium'", set_values={"balance": 300})`,
    hints: [
      "Use .update() with condition and set_values",
      "set_values is a dict mapping column names to new values",
      "The column to update is 'balance'"
    ]
  };
