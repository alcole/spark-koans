/**
 * Koan 19: Aggregate Without Grouping
 * Category: Aggregations
 */

export default {
    id: 19,
    title: "Aggregate Without Grouping",
    category: "Aggregations",
    description: "Calculate aggregates across the entire DataFrame. Replace ___ with the correct code.",
    setup: `
data = [(100,), (200,), (300,), (400,), (500,)]
df = spark.createDataFrame(data, ["value"])
`,
    template: `# Calculate sum of all values without grouping
from pyspark.sql.functions import sum as spark_sum, avg, count

result = df.___(spark_sum("value").alias("total"))

total = result.collect()[0]["total"]
assert total == 1500, f"Expected 1500, got {total}"
print("✓ Sum calculated: 1500")

# Calculate multiple aggregates
result2 = df.agg(
    spark_sum("value").alias("total"),
    ___("value").alias("average"),
    count("value").alias("num_rows")
)

row = result2.collect()[0]
assert row["average"] == 300.0, f"Expected 300.0, got {row['average']}"
assert row["num_rows"] == 5, f"Expected 5, got {row['num_rows']}"
print("✓ Multiple aggregates calculated")

print("\\n🎉 Koan complete! You've learned global aggregations.")`,
    solution: `result = df.agg(spark_sum("value").alias("total"))\nresult2 = df.agg(spark_sum("value").alias("total"), avg("value").alias("average"), count("value").alias("num_rows"))`,
    hints: [
      "Use .agg() directly on the DataFrame without groupBy",
      "Note: we import sum as spark_sum to avoid conflict with Python's built-in",
      "avg() calculates the average"
    ]
  };
