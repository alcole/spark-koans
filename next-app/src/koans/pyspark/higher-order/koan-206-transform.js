/**
 * Koan 206: Higher-Order Function - transform
 * Category: Higher-Order Functions
 */

const koan = {
  id: 206,
  title: "Higher-Order Function: transform",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use transform() to apply a function to every element in an array. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [80, 90, 85]),
    ("Bob", [70, 60, 75]),
    ("Carol", [95, 100, 92])
]
df = spark.createDataFrame(data, ["student", "scores"])
`,
  template: `# transform() applies a lambda to every element in an array
from pyspark.sql.functions import col, transform

# Add 5 bonus points to each score
result = df.select(
    "student",
    ___(col("scores"), lambda x: x + 5).alias("curved_scores")
)

alice = result.filter(col("student") == "Alice").collect()[0]
assert alice["curved_scores"] == [85, 95, 90], f"Expected [85, 95, 90], got {alice['curved_scores']}"
print("\\u2713 Applied transform to add bonus points")

# Double each score
doubled = df.select(
    "student",
    transform(col("scores"), lambda x: x * ___).alias("doubled_scores")
)

bob = doubled.filter(col("student") == "Bob").collect()[0]
assert bob["doubled_scores"] == [140, 120, 150], f"Expected [140, 120, 150], got {bob['doubled_scores']}"
print("\\u2713 Applied transform to double scores")

print("\\n\\ud83c\\udf89 Koan complete! You've learned the transform higher-order function.")`,
  solution: `result = df.select(
    "student",
    transform(col("scores"), lambda x: x + 5).alias("curved_scores")
)

doubled = df.select(
    "student",
    transform(col("scores"), lambda x: x * 2).alias("doubled_scores")
)`,
  hints: [
    "transform() takes an array column and a lambda function",
    "The lambda receives each element and returns the transformed value"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
