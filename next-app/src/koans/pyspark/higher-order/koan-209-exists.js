/**
 * Koan 209: Higher-Order Function - exists
 * Category: Higher-Order Functions
 */

const koan = {
  id: 209,
  title: "Higher-Order Function: exists",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use exists() to check if ANY element in an array matches a condition. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [10, 20, 30]),
    ("Bob", [4, 15, 25, 35]),
    ("Carol", [100, 200])
]
df = spark.createDataFrame(data, ["name", "values"])
`,
  template: `# exists() checks if ANY element in an array matches a condition
from pyspark.sql.functions import col, exists

# Check if any value is >= 100
has_large = df.select(
    "name",
    ___(col("values"), lambda x: x >= 100).alias("has_hundred")
)

carol_large = has_large.filter(col("name") == "Carol").collect()[0]["has_hundred"]
assert carol_large == True, "Carol should have a value >= 100"
print("\\u2713 exists() found matching element")

alice_large = has_large.filter(col("name") == "Alice").collect()[0]["has_hundred"]
assert alice_large == False, "Alice should not have a value >= 100"
print("\\u2713 exists() returns False when no match")

# Check if any value is even
has_even = df.select(
    "name",
    exists(col("values"), lambda x: x % 2 == ___).alias("has_even")
)

bob_even = has_even.filter(col("name") == "Bob").collect()[0]["has_even"]
assert bob_even == True, "Bob has an even value (4)"
print("\\u2713 exists() with modulo condition")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered exists().")`,
  solution: `has_large = df.select(
    "name",
    exists(col("values"), lambda x: x >= 100).alias("has_hundred")
)

has_even = df.select(
    "name",
    exists(col("values"), lambda x: x % 2 == 0).alias("has_even")
)`,
  hints: [
    "exists() takes (array_column, lambda) and returns True if any element matches",
    "The lambda receives each element and should return a boolean condition",
    "An even number has remainder 0 when divided by 2"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
