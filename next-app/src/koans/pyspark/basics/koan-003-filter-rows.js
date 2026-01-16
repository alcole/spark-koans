/**
 * Koan 3: Filtering Rows
 * Category: Basics
 * Difficulty: Beginner
 */

const koan = {
  id: 3,
  title: "Filtering Rows",
  category: "Basics",
  difficulty: "beginner",
  description: "Learn how to filter rows based on conditions. Replace ___ with the correct code.",

  setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 52)]
df = spark.createDataFrame(data, ["name", "age"])
`,

  template: `# Filter to only include people over 35
from pyspark.sql.functions import col

result = df.___(col("age") ___ 35)

# Should have 2 people over 35
assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
print("✓ Correct number of rows filtered")

# Collect and verify
rows = result.collect()
ages = [row["age"] for row in rows]
assert all(age > 35 for age in ages), "Some ages are not > 35"
print("✓ All remaining rows have age > 35")

print("\\n🎉 Koan complete! You've learned to filter rows.")`,

  solution: `result = df.filter(col("age") > 35)`,

  hints: [
    "You want to 'filter' the DataFrame",
    "Use a comparison operator to check if age is greater than 35",
    "The col() function references a column by name"
  ],

  examCoverage: ["DEA", "DAA", "MLA"],
  prerequisiteKoans: [1, 2],
  nextKoans: [4],
};

export default koan;
