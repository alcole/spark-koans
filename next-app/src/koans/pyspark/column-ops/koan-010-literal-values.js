/**
 * Koan 10: Literal Values
 * Category: Column Operations
 */

export default {
    id: 10,
    title: "Literal Values",
    category: "Column Operations",
    description: "Add a column with a constant value. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Add a column 'country' with value 'USA' for all rows
from pyspark.sql.functions import lit

result = df.withColumn("country", ___("USA"))

rows = result.collect()
assert all(row["country"] == "USA" for row in rows), "All rows should have country=USA"
print("✓ Added literal column")

# Add a numeric literal
result2 = df.withColumn("bonus", ___(1000))
assert result2.collect()[0]["bonus"] == 1000, "Bonus should be 1000"
print("✓ Added numeric literal")

print("\\n🎉 Koan complete! You've learned to use literal values.")`,
    solution: `result = df.withColumn("country", lit("USA"))\nresult2 = df.withColumn("bonus", lit(1000))`,
    hints: [
      "lit() creates a literal (constant) column",
      "It can be used with strings, numbers, or other values"
    ]
  };
