/**
 * Koan 37: Higher-Order Function - filter
 * Category: Higher-Order Functions
 */

const koan = {
  id: 207,
  title: "Higher-Order Function: filter",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use filter() on arrays to keep only elements matching a condition. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [88, 42, 95, 67, 91]),
    ("Bob", [55, 73, 81, 39, 60]),
    ("Carol", [92, 87, 76, 94, 88])
]
df = spark.createDataFrame(data, ["student", "scores"])
`,
  template: `# filter() keeps array elements that match a predicate
from pyspark.sql.functions import col, filter, size

# Keep only passing scores (>= 70)
result = df.select(
    "student",
    ___(col("scores"), lambda x: x >= 70).alias("passing_scores")
)

alice = result.filter(col("student") == "Alice").collect()[0]
assert alice["passing_scores"] == [88, 95, 91], f"Expected [88, 95, 91], got {alice['passing_scores']}"
print("\\u2713 Filtered to passing scores")

bob = result.filter(col("student") == "Bob").collect()[0]
assert bob["passing_scores"] == [73, 81], f"Expected [73, 81], got {bob['passing_scores']}"
print("\\u2713 Correctly filtered Bob's scores")

# Count how many scores are above 90
high_scores = df.select(
    "student",
    size(filter(col("scores"), lambda x: x > ___)).alias("num_high")
)

carol_high = high_scores.filter(col("student") == "Carol").collect()[0]["num_high"]
assert carol_high == 2, f"Expected Carol to have 2 scores above 90, got {carol_high}"
print("\\u2713 Counted high scores using filter + size")

print("\\n\\ud83c\\udf89 Koan complete! You've learned the filter higher-order function.")`,
  solution: `result = df.select(
    "student",
    filter(col("scores"), lambda x: x >= 70).alias("passing_scores")
)

high_scores = df.select(
    "student",
    size(filter(col("scores"), lambda x: x > 90)).alias("num_high")
)`,
  hints: [
    "filter() takes an array column and a predicate lambda",
    "The lambda should return True to keep an element, False to discard it"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
