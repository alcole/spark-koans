/**
 * Koan 208: Higher-Order Function - aggregate
 * Category: Higher-Order Functions
 */

const koan = {
  id: 208,
  title: "Higher-Order Function: aggregate",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use aggregate() to reduce an array to a single value, and exists()/forall() for boolean checks. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [10, 20, 30]),
    ("Bob", [5, 15, 25, 35]),
    ("Carol", [100])
]
df = spark.createDataFrame(data, ["name", "values"])
`,
  template: `# aggregate() reduces an array to a single value (like fold/reduce)
from pyspark.sql.functions import col, aggregate, exists, forall, lit

# Sum all values in the array using aggregate
# aggregate(array, initial_value, merge_function)
result = df.select(
    "name",
    ___(col("values"), lit(0), lambda acc, x: acc + x).alias("total")
)

alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["total"] == 60, f"Expected total 60, got {alice['total']}"
print("\\u2713 Aggregated array with sum")

bob = result.filter(col("name") == "Bob").collect()[0]
assert bob["total"] == 80, f"Expected total 80, got {bob['total']}"
print("\\u2713 Aggregate works for different-length arrays")

# exists() checks if ANY element matches a condition
has_large = df.select(
    "name",
    ___(col("values"), lambda x: x >= 100).alias("has_hundred")
)

carol_large = has_large.filter(col("name") == "Carol").collect()[0]["has_hundred"]
assert carol_large == True, "Carol should have a value >= 100"
alice_large = has_large.filter(col("name") == "Alice").collect()[0]["has_hundred"]
assert alice_large == False, "Alice should not have a value >= 100"
print("\\u2713 exists() checks for matching elements")

# forall() checks if ALL elements match a condition
all_positive = df.select(
    "name",
    forall(col("values"), lambda x: x > ___).alias("all_positive")
)

assert all_positive.collect()[0]["all_positive"] == True
print("\\u2713 forall() checks all elements")

print("\\n\\ud83c\\udf89 Koan complete! You've learned aggregate, exists, and forall.")`,
  solution: `result = df.select(
    "name",
    aggregate(col("values"), lit(0), lambda acc, x: acc + x).alias("total")
)

has_large = df.select(
    "name",
    exists(col("values"), lambda x: x >= 100).alias("has_hundred")
)

all_positive = df.select(
    "name",
    forall(col("values"), lambda x: x > 0).alias("all_positive")
)`,
  hints: [
    "aggregate() takes (array, initial_value, merge_function)",
    "The merge function receives (accumulator, element) like reduce/fold",
    "exists() returns True if any element matches, forall() if all match"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
