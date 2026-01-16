/**
 * Koan 5: Grouping and Aggregating
 * Category: Basics
 * Difficulty: Beginner
 */

const koan = {
  id: 5,
  title: "Grouping and Aggregating",
  category: "Basics",
  difficulty: "beginner",
  description: "Learn how to group data and calculate aggregates. Replace ___ with the correct code.",

  setup: `
data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
`,

  template: `# Group by department and calculate average salary
from pyspark.sql.functions import avg, round, col

result = df.___("department").agg(
    round(___("salary"), 2).alias("avg_salary")
)

# Should have 2 departments
assert result.count() == 2, f"Expected 2 groups, got {result.count()}"
print("✓ Correct number of groups")

# Check Engineering average (6000 + 6500 + 5500) / 3 = 6000
eng_row = result.filter(col("department") == "Engineering").collect()[0]
assert eng_row["avg_salary"] == 6000.0, f"Expected 6000.0, got {eng_row['avg_salary']}"
print("✓ Engineering average salary is correct")

print("\\n🎉 Koan complete! You've learned to group and aggregate.")`,

  solution: `result = df.groupBy("department").agg(round(avg("salary"), 2).alias("avg_salary"))`,

  hints: [
    "First you need to group the data using 'groupBy'",
    "Then aggregate using 'avg' function for average",
    "The avg function takes a column name"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [1, 2, 3, 4],
  nextKoans: [101],
};

export default koan;
