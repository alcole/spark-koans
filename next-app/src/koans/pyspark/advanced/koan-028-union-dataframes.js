/**
 * Koan 28: Union DataFrames
 * Category: Advanced
 */

export default {
    id: 28,
    title: "Union DataFrames",
    category: "Advanced",
    difficulty: "intermediate",
    description: "Combine DataFrames vertically. Replace ___ with the correct code.",
    setup: `
df1 = spark.createDataFrame([("Alice", 34), ("Bob", 45)], ["name", "age"])
df2 = spark.createDataFrame([("Charlie", 29), ("Diana", 52)], ["name", "age"])
`,
    template: `# Combine two DataFrames with the same schema
result = df1.___(df2)

assert result.count() == 4, f"Expected 4 rows, got {result.count()}"
print("✓ Combined DataFrames")

names = [row["name"] for row in result.collect()]
assert "Alice" in names and "Charlie" in names, "Should have names from both DFs"
print("✓ Contains data from both DataFrames")

print("\\n🎉 Koan complete! You've learned to union DataFrames.")`,
    solution: `result = df1.union(df2)`,
    hints: [
      "union() stacks DataFrames on top of each other",
      "Both DataFrames must have the same schema"
    ]
  };
