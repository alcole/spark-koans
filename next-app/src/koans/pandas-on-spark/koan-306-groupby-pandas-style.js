/**
 * Koan 306: GroupBy with Pandas Syntax
 * Category: Pandas API on Spark
 * Difficulty: Intermediate
 */

export default {
  id: 306,
  title: "GroupBy with Pandas Syntax",
  category: "Pandas API on Spark",
  difficulty: "intermediate",
  description: "Learn how to use pandas-style groupby operations. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a sales DataFrame
psdf = ps.DataFrame({
    "region": ["East", "East", "West", "West", "East", "West"],
    "product": ["A", "B", "A", "B", "A", "B"],
    "sales": [100, 150, 200, 180, 120, 160]
})
`,

  template: `# Group by region and calculate mean sales
region_avg = psdf.groupby("___")["sales"].mean()
print("Average sales by region:")
print(region_avg)
print("✓ groupby() with aggregation works")

# Group by multiple columns
multi_group = psdf.groupby(["region", "___"])["sales"].sum()
print("\\nTotal sales by region and product:")
print(multi_group)
print("✓ Multi-column groupby works")

# Use agg() for multiple aggregations
result = psdf.groupby("region").___({
    "sales": ["sum", "mean", "___"]
})
print("\\nMultiple aggregations:")
print(result)
print("✓ agg() with multiple functions works")

# Group and get size (count of rows per group)
group_sizes = psdf.groupby("region").___()
print("\\nGroup sizes:")
print(group_sizes)
assert group_sizes["East"] == 3, f"East should have 3 rows"
print("✓ size() counts rows per group")

# Apply custom aggregation
max_sales_by_region = psdf.groupby("region")["sales"].___()
print("\\nMax sales by region:")
print(max_sales_by_region)
print("✓ max() aggregation works")

print("\\n🎉 Koan complete! You can use pandas-style groupby.")`,

  solution: `region_avg = psdf.groupby("region")["sales"].mean()

multi_group = psdf.groupby(["region", "product"])["sales"].sum()

result = psdf.groupby("region").agg({
    "sales": ["sum", "mean", "max"]
})

group_sizes = psdf.groupby("region").size()

max_sales_by_region = psdf.groupby("region")["sales"].max()`,

  hints: [
    "groupby(column) creates a groupby object",
    "Access columns with ['column'] after groupby",
    "Common aggregations: mean(), sum(), max(), min(), count()",
    "agg() accepts a dict mapping columns to functions",
    "size() returns the count of items in each group"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301, 303],
  nextKoans: [307],
};
