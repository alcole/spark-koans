/**
 * Koan 12: Type Casting
 * Category: Column Operations
 */

const koan = {
    id: 12,
    title: "Type Casting",
    category: "Column Operations",
    description: "Cast columns to different data types. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "34"), ("Bob", "45")]
df = spark.createDataFrame(data, ["name", "age_str"])
`,
    template: `from pyspark.sql.functions import col

# Cast age_str from string to integer
result = df.withColumn("age", col("age_str").cast("___"))

# Verify we can do math on the new column
result = result.withColumn("age_plus_10", col("age") + 10)

rows = result.collect()
assert rows[0]["age_plus_10"] == 44, f"Expected 44, got {rows[0]['age_plus_10']}"
print("✓ Cast to integer and performed math")

# Cast to double
result2 = df.withColumn("age_float", col("age_str").___("double"))
print("✓ Cast to double")

print("\\n🎉 Koan complete! You've learned to cast types.")`,
    solution: `result = df.withColumn("age", col("age_str").cast("integer"))\nresult2 = df.withColumn("age_float", col("age_str").cast("double"))`,
    hints: [
      "Use .cast() on a column to change its type",
      "Common types: 'integer', 'double', 'string', 'boolean', 'date'"
    ]
  };

export default koan;
