/**
 * Koan 208: Higher-Order Function - aggregate
 * Category: Higher-Order Functions
 */

const koan = {
  id: 208,
  title: "Higher-Order Function: aggregate",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use aggregate() to reduce an array to a single value with an accumulator. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [10, 20, 30]),
    ("Bob", [5, 15, 25, 35]),
    ("Carol", [100])
]
df = spark.createDataFrame(data, ["name", "values"])
`,
  template: `# aggregate() reduces an array to a single value (like fold/reduce)
from pyspark.sql.functions import col, aggregate, lit

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

# Product using aggregate with initial value 1
product = df.select(
    "name",
    aggregate(col("values"), lit(___), lambda acc, x: acc * x).alias("product")
)

carol = product.filter(col("name") == "Carol").collect()[0]
assert carol["product"] == 100, f"Expected product 100, got {carol['product']}"
print("\\u2713 Aggregate with multiplication")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered aggregate().")`,
  solution: `result = df.select(
    "name",
    aggregate(col("values"), lit(0), lambda acc, x: acc + x).alias("total")
)

product = df.select(
    "name",
    aggregate(col("values"), lit(1), lambda acc, x: acc * x).alias("product")
)`,
  hints: [
    "aggregate() takes (array, initial_value, merge_function)",
    "The merge function receives (accumulator, element) like reduce/fold",
    "For multiplication, start with an initial value of 1"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
