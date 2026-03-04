/**
 * Koan 210: Higher-Order Function - forall
 * Category: Higher-Order Functions
 */

const koan = {
  id: 210,
  title: "Higher-Order Function: forall",
  category: "Higher-Order Functions",
  difficulty: "advanced",
  description: "Use forall() to check if ALL elements in an array match a condition. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [10, 20, 30]),
    ("Bob", [5, 15, 25, 35]),
    ("Carol", [-1, 100, 200])
]
df = spark.createDataFrame(data, ["name", "values"])
`,
  template: `# forall() checks if ALL elements in an array match a condition
from pyspark.sql.functions import col, forall

# Check if all values are positive (> 0)
all_positive = df.select(
    "name",
    ___(col("values"), lambda x: x > 0).alias("all_positive")
)

alice_pos = all_positive.filter(col("name") == "Alice").collect()[0]["all_positive"]
assert alice_pos == True, "All of Alice's values should be positive"
print("\\u2713 forall() confirms all elements match")

carol_pos = all_positive.filter(col("name") == "Carol").collect()[0]["all_positive"]
assert carol_pos == False, "Carol has a negative value"
print("\\u2713 forall() returns False when any element fails")

# Check if all values are less than a threshold
all_small = df.select(
    "name",
    forall(col("values"), lambda x: x < ___).alias("all_under_50")
)

bob_small = all_small.filter(col("name") == "Bob").collect()[0]["all_under_50"]
assert bob_small == True, "All of Bob's values should be under 50"
print("\\u2713 forall() with threshold condition")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered forall().")`,
  solution: `all_positive = df.select(
    "name",
    forall(col("values"), lambda x: x > 0).alias("all_positive")
)

all_small = df.select(
    "name",
    forall(col("values"), lambda x: x < 50).alias("all_under_50")
)`,
  hints: [
    "forall() takes (array_column, lambda) and returns True only if ALL elements match",
    "Unlike exists(), forall() requires every element to satisfy the condition",
    "Bob's largest value is 35, so the threshold must be greater than 35"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
