/**
 * Koan 305: Index Operations
 * Category: Pandas API on Spark
 * Difficulty: Intermediate
 */

export default {
  id: 305,
  title: "Index Operations",
  category: "Pandas API on Spark",
  difficulty: "intermediate",
  description: "Learn how to work with DataFrame indexes in Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a DataFrame with default integer index
psdf = ps.DataFrame({
    "employee_id": [101, 102, 103, 104],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "salary": [75000, 82000, 68000, 91000]
})
`,

  template: `# Set a column as the index
psdf_indexed = psdf.set_index("___")
print("DataFrame with employee_id as index:")
print(psdf_indexed)
print("✓ set_index() applied")

# Access the index
index_values = psdf_indexed.___.tolist()
print(f"\\nIndex values: {index_values}")
assert 101 in index_values, "101 should be in index"
print("✓ index attribute accessed")

# Reset the index back to default
psdf_reset = psdf_indexed.___()
print("\\nDataFrame with reset index:")
print(psdf_reset)
print("✓ reset_index() applied")

# Sort by index
psdf_indexed_desc = psdf_indexed.sort_index(ascending=___)
print("\\nSorted by index (descending):")
print(psdf_indexed_desc)
print("✓ sort_index() works")

# Set index name
psdf_indexed.index.___ = "emp_id"
print(f"\\nIndex name: {psdf_indexed.index.name}")
assert psdf_indexed.index.name == "emp_id", "Index name should be 'emp_id'"
print("✓ Index name set")

print("\\n🎉 Koan complete! You can now manage DataFrame indexes.")`,

  solution: `psdf_indexed = psdf.set_index("employee_id")

index_values = psdf_indexed.index.tolist()

psdf_reset = psdf_indexed.reset_index()

psdf_indexed_desc = psdf_indexed.sort_index(ascending=False)

psdf_indexed.index.name = "emp_id"`,

  hints: [
    "set_index(column_name) sets a column as the index",
    ".index attribute accesses the index",
    "reset_index() converts index back to a regular column",
    "sort_index(ascending=False) sorts by index in descending order",
    "index.name sets the name of the index"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301, 302],
  nextKoans: [306],
};
