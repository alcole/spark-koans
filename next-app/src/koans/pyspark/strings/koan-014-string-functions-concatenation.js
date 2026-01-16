/**
 * Koan 14: String Functions - Concatenation
 * Category: String Functions
 */

const koan = {
    id: 14,
    title: "String Functions - Concatenation",
    category: "String Functions",
    description: "Concatenate strings together. Replace ___ with the correct code.",
    setup: `
data = [("Alice", "Smith"), ("Bob", "Jones")]
df = spark.createDataFrame(data, ["first", "last"])
`,
    template: `# Concatenate first and last name with a space
from pyspark.sql.functions import concat, concat_ws, lit

result = df.withColumn("full_name", ___(col("first"), lit(" "), col("last")))
assert result.collect()[0]["full_name"] == "Alice Smith"
print("✓ Concatenated with concat()")

# Use concat_ws (with separator) - cleaner for multiple values
result2 = df.withColumn("full_name", ___(" ", col("first"), col("last")))
assert result2.collect()[0]["full_name"] == "Alice Smith"
print("✓ Concatenated with concat_ws()")

print("\\n🎉 Koan complete! You've learned string concatenation.")`,
    solution: `result = df.withColumn("full_name", concat(col("first"), lit(" "), col("last")))\nresult2 = df.withColumn("full_name", concat_ws(" ", col("first"), col("last")))`,
    hints: [
      "concat() joins columns directly",
      "concat_ws() takes a separator as the first argument",
      "Use lit() for literal strings in concat()"
    ]
  };

export default koan;
