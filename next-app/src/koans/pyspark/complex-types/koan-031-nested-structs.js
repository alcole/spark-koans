/**
 * Koan 31: Working with Nested Structs
 * Category: Complex Types
 */

const koan = {
  id: 31,
  title: "Nested Structs",
  category: "Complex Types",
  difficulty: "advanced",
  description: "Access fields within nested struct columns. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", {"street": "123 Main St", "city": "Springfield", "zip": "62701"}),
    ("Bob", {"street": "456 Oak Ave", "city": "Shelbyville", "zip": "62565"}),
    ("Carol", {"street": "789 Pine Rd", "city": "Capital City", "zip": "62702"})
]
df = spark.createDataFrame(data, ["name", "address"])
`,
  template: `# Access nested struct fields using dot notation or getField
from pyspark.sql.functions import col

# Extract the city from the nested address struct
result = df.select("name", col("address").___("city").alias("city"))

assert result.columns == ["name", "city"], f"Expected columns [name, city], got {result.columns}"
print("\\u2713 Selected nested field")

cities = [row["city"] for row in result.collect()]
assert cities == ["Springfield", "Shelbyville", "Capital City"], f"Expected correct cities, got {cities}"
print("\\u2713 Nested values extracted correctly")

# Extract multiple nested fields
full = df.select(
    "name",
    col("address").getField("street").alias("street"),
    col("address").___("zip").alias("zip")
)
assert "street" in full.columns and "zip" in full.columns
print("\\u2713 Multiple nested fields extracted")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to access nested struct fields.")`,
  solution: `result = df.select("name", col("address").getField("city").alias("city"))

full = df.select(
    "name",
    col("address").getField("street").alias("street"),
    col("address").getField("zip").alias("zip")
)`,
  hints: [
    "Struct fields can be accessed with getField()",
    "getField('field_name') extracts a single field from a struct column"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
