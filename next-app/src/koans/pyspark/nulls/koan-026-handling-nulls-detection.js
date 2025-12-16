/**
 * Koan 26: Handling Nulls - Detection
 * Category: Null Handling
 */

export default {
    id: 26,
    title: "Handling Nulls - Detection",
    category: "Null Handling",
    description: "Detect and filter null values. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", None), ("Charlie", 29), (None, 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Filter to rows where age is not null
from pyspark.sql.functions import col, isnan, isnull

result = df.filter(col("age").___())

assert result.count() == 3, f"Expected 3 rows with age, got {result.count()}"
print("✓ Filtered to non-null ages")

# Filter to rows where age IS null
nulls = df.filter(col("age").___())
assert nulls.count() == 1, f"Expected 1 null age, got {nulls.count()}"
print("✓ Found rows with null age")

# Check for null name
null_names = df.filter(col("name").isNull())
assert null_names.count() == 1
print("✓ Found row with null name")

print("\\n🎉 Koan complete! You've learned null detection.")`,
    solution: `result = df.filter(col("age").isNotNull())\nnulls = df.filter(col("age").isNull())`,
    hints: [
      "isNotNull() returns true for non-null values",
      "isNull() returns true for null values",
      "These are methods on Column objects"
    ]
  };
