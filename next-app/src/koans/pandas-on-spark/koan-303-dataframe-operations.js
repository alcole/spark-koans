/**
 * Koan 303: DataFrame Operations
 * Category: Pandas API on Spark
 * Difficulty: Beginner
 */

export default {
  id: 303,
  title: "DataFrame Operations",
  category: "Pandas API on Spark",
  difficulty: "beginner",
  description: "Learn common DataFrame operations in Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a Pandas-on-Spark DataFrame
psdf = ps.DataFrame({
    "product": ["A", "B", "C", "D", "E"],
    "price": [100, 250, 175, 300, 125],
    "quantity": [10, 5, 8, 3, 12]
})
`,

  template: `# Show first 3 rows
print("First 3 rows:")
print(psdf.___(3))
print("✓ head() works")

# Show last 2 rows
print("\\nLast 2 rows:")
print(psdf.___(2))
print("✓ tail() works")

# Get summary statistics
print("\\nSummary statistics:")
stats = psdf.___()
print(stats)
print("✓ describe() provides statistics")

# Get shape (rows, columns)
rows, cols = psdf.___
print(f"\\nShape: {rows} rows, {cols} columns")
assert rows == 5, f"Expected 5 rows"
assert cols == 3, f"Expected 3 columns"
print("✓ shape attribute works")

# Get column data types
print("\\nData types:")
print(psdf.___)
print("✓ dtypes shows column types")

# Get column names
columns = psdf.___.tolist()
print(f"\\nColumns: {columns}")
assert "price" in columns, "Should have 'price' column"
print("✓ columns attribute works")

print("\\n🎉 Koan complete! You know pandas DataFrame operations.")`,

  solution: `print(psdf.head(3))

print(psdf.tail(2))

stats = psdf.describe()

rows, cols = psdf.shape

print(psdf.dtypes)

columns = psdf.columns.tolist()`,

  hints: [
    "head(n) shows first n rows",
    "tail(n) shows last n rows",
    "describe() generates summary statistics",
    "shape is a tuple: (rows, columns)",
    "dtypes shows data types of each column",
    "columns returns column names"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [304],
};
