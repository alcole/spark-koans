/**
 * Koan 16: String Functions - Trim and Pad
 * Category: String Functions
 */

const koan = {
    id: 16,
    title: "String Functions - Trim and Pad",
    category: "String Functions",
    description: "Remove or add whitespace. Replace ___ with the correct code.",
    setup: `
data = [("  Alice  ",), ("Bob",), (" Charlie ",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Trim whitespace from both sides
from pyspark.sql.functions import trim, ltrim, rtrim, lpad, rpad

result = df.withColumn("trimmed", ___(col("name")))
trimmed = [row["trimmed"] for row in result.collect()]
assert trimmed == ["Alice", "Bob", "Charlie"], f"Expected trimmed names, got {trimmed}"
print("✓ Trimmed whitespace")

# Pad names to 10 characters with asterisks
result2 = df.withColumn("trimmed", trim(col("name")))
result2 = result2.withColumn("padded", ___(col("trimmed"), 10, "*"))
assert result2.collect()[1]["padded"] == "*******Bob"
print("✓ Left-padded with asterisks")

print("\\n🎉 Koan complete! You've learned trim and pad functions.")`,
    solution: `result = df.withColumn("trimmed", trim(col("name")))\nresult2 = result2.withColumn("padded", lpad(col("trimmed"), 10, "*"))`,
    hints: [
      "trim() removes whitespace from both sides",
      "lpad(col, length, pad_string) pads on the left",
      "rpad() pads on the right"
    ]
  };

export default koan;
