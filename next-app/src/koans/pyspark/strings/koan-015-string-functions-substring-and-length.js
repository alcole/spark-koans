/**
 * Koan 15: String Functions - Substring and Length
 * Category: String Functions
 */

const koan = {
    id: 15,
    title: "String Functions - Substring and Length",
    category: "String Functions",
    description: "Extract parts of strings and measure length. Replace ___ with the correct code.",
    setup: `
data = [("Alice",), ("Bob",), ("Charlotte",)]
df = spark.createDataFrame(data, ["name"])
`,
    template: `# Get the length of each name
from pyspark.sql.functions import length, substring, col

result = df.withColumn("name_length", ___(col("name")))
lengths = [row["name_length"] for row in result.collect()]
assert lengths == [5, 3, 9], f"Expected [5, 3, 9], got {lengths}"
print("✓ Calculated string lengths")

# Get first 3 characters (substring is 1-indexed!)
result2 = df.withColumn("first_three", ___(col("name"), 1, 3))
firsts = [row["first_three"] for row in result2.collect()]
assert firsts == ["Ali", "Bob", "Cha"], f"Expected ['Ali', 'Bob', 'Cha'], got {firsts}"
print("✓ Extracted first 3 characters")

print("\\n🎉 Koan complete! You've learned substring and length.")`,
    solution: `result = df.withColumn("name_length", length(col("name")))\nresult2 = df.withColumn("first_three", substring(col("name"), 1, 3))`,
    hints: [
      "length() returns the number of characters",
      "substring(col, start, length) extracts a portion",
      "Note: substring is 1-indexed, not 0-indexed!"
    ]
  };

export default koan;
