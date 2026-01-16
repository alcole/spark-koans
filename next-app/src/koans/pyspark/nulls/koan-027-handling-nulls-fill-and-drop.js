/**
 * Koan 27: Handling Nulls - Fill and Drop
 * Category: Null Handling
 */

const koan = {
    id: 27,
    title: "Handling Nulls - Fill and Drop",
    category: "Null Handling",
    description: "Replace or remove null values. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", None), (None, 29), ("Diana", None)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Fill null ages with 0
result = df.___(0, subset=["age"])

ages = [row["age"] for row in result.collect()]
assert None not in ages, "Should have no null ages"
assert ages.count(0) == 2, "Should have 2 zeros"
print("✓ Filled null ages with 0")

# Fill null names with "Unknown"
result2 = df.fillna("Unknown", subset=["name"])
names = [row["name"] for row in result2.collect()]
assert "Unknown" in names, "Should have Unknown name"
print("✓ Filled null names")

# Drop rows with ANY null values
result3 = df.___()
assert result3.count() == 1, f"Expected 1 complete row, got {result3.count()}"
print("✓ Dropped rows with nulls")

print("\\n🎉 Koan complete! You've learned to handle nulls.")`,
    solution: `result = df.fillna(0, subset=["age"])\nresult3 = df.dropna()`,
    hints: [
      "fillna() replaces null values",
      "You can specify which columns with subset=",
      "dropna() removes rows with null values"
    ]
  };

export default koan;
