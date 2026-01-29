/**
 * Koan 13: String Functions - Case
 * Category: String Functions
 */

const koan = {
    id: 13,
    title: "String Functions - Case",
    category: "String Functions",
    difficulty: "beginner",
    description: "Transform string case. Replace ___ with the correct code.",
    setup: `
data = [("alice smith",), ("BOB JONES",), ("Charlie Brown",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Convert to uppercase
from pyspark.sql.functions import upper, lower, initcap, col

result = df.withColumn("upper_name", ___(col("name")))
assert result.collect()[0]["upper_name"] == "ALICE SMITH"
print("✓ Converted to uppercase")

# Convert to lowercase
result = df.withColumn("lower_name", ___(col("name")))
assert result.collect()[1]["lower_name"] == "bob jones"
print("✓ Converted to lowercase")

# Convert to title case (capitalize first letter of each word)
result = df.withColumn("title_name", ___(col("name")))
assert result.collect()[0]["title_name"] == "Alice Smith"
print("✓ Converted to title case")

print("\\n🎉 Koan complete! You've learned string case functions.")`,
    solution: `result = df.withColumn("upper_name", upper(col("name")))\nresult = df.withColumn("lower_name", lower(col("name")))\nresult = df.withColumn("title_name", initcap(col("name")))`,
    hints: [
      "upper() converts to uppercase",
      "lower() converts to lowercase",
      "initcap() capitalizes the first letter of each word"
    ]
  };

export default koan;
