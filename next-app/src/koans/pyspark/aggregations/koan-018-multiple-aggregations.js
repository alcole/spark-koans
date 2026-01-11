/**
 * Koan 18: Multiple Aggregations
 * Category: Aggregations
 */

export default {
    id: 18,
    title: "Multiple Aggregations",
    category: "Aggregations",
    difficulty: "intermediate",
    description: "Calculate multiple aggregations at once. Replace ___ with the correct code.",
    setup: `
data = [
    ("Sales", 5000), ("Sales", 4500), ("Sales", 6000),
    ("Engineering", 6000), ("Engineering", 6500)
]
df = spark.createDataFrame(data, ["department", "salary"])
`,
    template: `# Calculate min, max, avg, and count per department
from pyspark.sql.functions import min, max, avg, count, col

result = df.groupBy("department").agg(
    ___("salary").alias("min_salary"),
    ___("salary").alias("max_salary"),
    avg("salary").alias("avg_salary"),
    ___("salary").alias("emp_count")
)

sales = result.filter(col("department") == "Sales").collect()[0]

assert sales["min_salary"] == 4500, f"Min should be 4500, got {sales['min_salary']}"
print("✓ Min salary correct")

assert sales["max_salary"] == 6000, f"Max should be 6000, got {sales['max_salary']}"
print("✓ Max salary correct")

assert sales["emp_count"] == 3, f"Count should be 3, got {sales['emp_count']}"
print("✓ Employee count correct")

print("\\n🎉 Koan complete! You've learned multiple aggregations.")`,
    solution: `result = df.groupBy("department").agg(min("salary").alias("min_salary"), max("salary").alias("max_salary"), avg("salary").alias("avg_salary"), count("salary").alias("emp_count"))`,
    hints: [
      "min() finds the minimum value",
      "max() finds the maximum value",
      "count() counts the number of values"
    ]
  };
