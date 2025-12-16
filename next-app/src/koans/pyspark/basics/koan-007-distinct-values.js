/**
 * Koan 7: Distinct Values
 * Category: Basics
 */

export default {
    id: 7,
    title: "Distinct Values",
    category: "Basics",
    description: "Remove duplicate rows from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "NYC"), ("Bob", "LA"), ("Alice", "NYC"), ("Charlie", "NYC")]
df = spark.createDataFrame(data, ["name", "city"])
`,
    template: `# Get distinct rows
result = df.___()

assert result.count() == 3, f"Expected 3 distinct rows, got {result.count()}"
print("✓ Got distinct rows")

# Get distinct cities only
cities = df.select("city").___()
assert cities.count() == 2, f"Expected 2 distinct cities, got {cities.count()}"
print("✓ Got distinct cities (NYC, LA)")

print("\\n🎉 Koan complete! You've learned to get distinct values.")`,
    solution: `result = df.distinct()\ncities = df.select("city").distinct()`,
    hints: [
      "The method name is exactly what you want: 'distinct'",
      "You can chain select() with distinct() to get unique values of specific columns"
    ]
  };
