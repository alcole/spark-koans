/**
 * Koan 302: Indexing with .loc and .iloc
 * Category: Pandas API on Spark
 * Difficulty: Beginner
 */

export default {
  id: 302,
  title: "Indexing with .loc and .iloc",
  category: "Pandas API on Spark",
  difficulty: "beginner",
  description: "Learn how to use .loc and .iloc for indexing in Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a Pandas-on-Spark DataFrame
psdf = ps.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "age": [25, 30, 35, 28],
    "city": ["NYC", "LA", "Chicago", "Boston"]
})
print("DataFrame created:")
print(psdf)
`,

  template: `# Use .loc to select by label/condition
# Select rows where age > 28
result1 = psdf.___[psdf["age"] > 28]
print("\\nRows with age > 28:")
print(result1)
assert len(result1) == 2, f"Expected 2 rows, got {len(result1)}"
print("✓ .loc with condition works")

# Use .loc to select specific columns
result2 = psdf.loc[:, ["name", "___"]]
print("\\nSelected name and city columns:")
print(result2)
assert "age" not in result2.columns.tolist(), "age should not be included"
print("✓ .loc column selection works")

# Use .iloc for integer-based indexing
# Get the first 2 rows
result3 = psdf.___[:2]
print("\\nFirst 2 rows (using iloc):")
print(result3)
assert len(result3) == 2, f"Expected 2 rows, got {len(result3)}"
print("✓ .iloc position indexing works")

# Get specific row and column by position
value = psdf.iloc[1, 0]  # Second row, first column
print(f"\\nValue at [1, 0]: {value}")
assert value == "Bob", f"Expected 'Bob', got {value}"
print("✓ .iloc single value access works")

print("\\n🎉 Koan complete! You can now use pandas-style indexing.")`,

  solution: `result1 = psdf.loc[psdf["age"] > 28]

result2 = psdf.loc[:, ["name", "city"]]

result3 = psdf.iloc[:2]`,

  hints: [
    ".loc uses label-based indexing (conditions, column names)",
    ".iloc uses integer position-based indexing",
    "Use .loc[rows, columns] syntax",
    ".iloc[row_positions, column_positions] for positional access"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [303],
};
