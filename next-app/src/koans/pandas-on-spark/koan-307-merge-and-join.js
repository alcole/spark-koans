/**
 * Koan 307: Merge and Join with Pandas Syntax
 * Category: Pandas API on Spark
 * Difficulty: Intermediate
 */

export default {
  id: 307,
  title: "Merge and Join with Pandas Syntax",
  category: "Pandas API on Spark",
  difficulty: "intermediate",
  description: "Learn how to merge and join DataFrames using pandas syntax. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create two DataFrames
employees = ps.DataFrame({
    "emp_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "dept_id": [10, 20, 10, 30]
})

departments = ps.DataFrame({
    "dept_id": [10, 20, 30],
    "dept_name": ["Engineering", "Sales", "HR"]
})
`,

  template: `# Merge on a common column
result = employees.merge(departments, on="___")
print("Merged DataFrame:")
print(result)
assert len(result) == 4, f"Expected 4 rows after merge"
print("✓ merge() on common column works")

# Left join (keep all employees even if no dept match)
# First add an employee with no department
employees_extended = ps.DataFrame({
    "emp_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "dept_id": [10, 20, 10, 30, 99]  # 99 doesn't exist in departments
})

left_result = employees_extended.merge(departments, on="dept_id", how="___")
print("\\nLeft join result:")
print(left_result)
assert len(left_result) == 5, f"Expected 5 rows in left join"
print("✓ Left join preserves all left rows")

# Merge with different column names
products = ps.DataFrame({
    "product_id": [1, 2, 3],
    "product_name": ["Widget", "Gadget", "Doohickey"]
})

sales = ps.DataFrame({
    "sale_id": [101, 102, 103],
    "prod_id": [1, 2, 1],  # Note: different column name
    "amount": [100, 200, 150]
})

# Merge using left_on and right_on
merged_sales = sales.merge(
    products,
    left_on="___",
    right_on="product_id"
)
print("\\nMerged sales with products:")
print(merged_sales)
print("✓ merge() with different column names works")

print("\\n🎉 Koan complete! You can now merge DataFrames like pandas.")`,

  solution: `result = employees.merge(departments, on="dept_id")

left_result = employees_extended.merge(departments, on="dept_id", how="left")

merged_sales = sales.merge(
    products,
    left_on="prod_id",
    right_on="product_id"
)`,

  hints: [
    "merge(other, on='column') joins on a common column",
    "how='left' keeps all left DataFrame rows",
    "how='inner' (default) keeps only matching rows",
    "Use left_on and right_on when column names differ",
    "Other join types: 'right', 'outer'"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [308],
};
