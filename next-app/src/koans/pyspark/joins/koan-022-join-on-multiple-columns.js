/**
 * Koan 22: Join on Multiple Columns
 * Category: Joins
 */

export default {
    id: 22,
    title: "Join on Multiple Columns",
    category: "Joins",
    description: "Join on multiple columns. Replace ___ with the correct code.",
    setup: `
orders = spark.createDataFrame([
    ("2024", "Q1", "Alice", 100),
    ("2024", "Q2", "Alice", 150),
    ("2024", "Q1", "Bob", 200)
], ["year", "quarter", "rep", "amount"])

targets = spark.createDataFrame([
    ("2024", "Q1", 120),
    ("2024", "Q2", 140)
], ["year", "quarter", "target"])
`,
    template: `# Join on both year and quarter
result = orders.join(targets, [___, ___], "inner")

# Should have 3 rows
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Joined on multiple columns")

# Check that Alice Q1 has target 120
alice_q1 = result.filter((col("rep") == "Alice") & (col("quarter") == "Q1")).collect()[0]
assert alice_q1["target"] == 120, f"Expected target 120, got {alice_q1['target']}"
print("✓ Targets matched correctly")

print("\\n🎉 Koan complete! You've learned multi-column joins.")`,
    solution: `result = orders.join(targets, ["year", "quarter"], "inner")`,
    hints: [
      "Pass a list of column names to join on multiple columns",
      "The columns must match in both DataFrames"
    ]
  };
