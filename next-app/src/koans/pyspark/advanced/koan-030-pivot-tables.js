/**
 * Koan 30: Pivot Tables
 * Category: Advanced
 */

export default {
    id: 30,
    title: "Pivot Tables",
    category: "Advanced",
    description: "Pivot data from rows to columns. Replace ___ with the correct code.",
    setup: `
data = [
    ("Alice", "Q1", 100), ("Alice", "Q2", 150),
    ("Bob", "Q1", 200), ("Bob", "Q2", 180)
]
df = spark.createDataFrame(data, ["name", "quarter", "sales"])
`,
    template: `# Pivot to get quarters as columns
from pyspark.sql.functions import sum as spark_sum

result = df.groupBy("name").___(___).agg(spark_sum("sales"))

# Should have columns: name, Q1, Q2
assert "Q1" in result.columns, "Should have Q1 column"
assert "Q2" in result.columns, "Should have Q2 column"
print("✓ Pivoted quarters to columns")

alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["Q1"] == 100, f"Expected Q1=100, got {alice['Q1']}"
assert alice["Q2"] == 150, f"Expected Q2=150, got {alice['Q2']}"
print("✓ Values correctly placed in columns")

print("\\n🎉 Koan complete! You've learned pivot tables.")`,
    solution: `result = df.groupBy("name").pivot("quarter").agg(spark_sum("sales"))`,
    hints: [
      "pivot() goes after groupBy()",
      "Specify which column's values become new column names"
    ]
  };
