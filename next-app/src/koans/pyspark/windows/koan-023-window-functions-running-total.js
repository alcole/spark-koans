/**
 * Koan 23: Window Functions - Running Total
 * Category: Window Functions
 */

export default {
    id: 23,
    title: "Window Functions - Running Total",
    category: "Window Functions",
    difficulty: "advanced",
    description: "Use window functions to calculate running totals. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 200),
    ("2024-01-04", 175)
]
df = spark.createDataFrame(data, ["date", "sales"])
`,
    template: `# Create a window that orders by date and includes all previous rows
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.___)

# Add running total column
result = df.withColumn("running_total", ___("sales").over(window_spec))

# Check the running totals
rows = result.orderBy("date").collect()

assert rows[0]["running_total"] == 100, "Day 1 should be 100"
print("✓ Day 1: 100")

assert rows[1]["running_total"] == 250, "Day 2 should be 250 (100+150)"
print("✓ Day 2: 250")

assert rows[3]["running_total"] == 625, "Day 4 should be 625"
print("✓ Day 4: 625 (cumulative)")

print("\\n🎉 Koan complete! You've learned window running totals.")`,
    solution: `window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)\nresult = df.withColumn("running_total", spark_sum("sales").over(window_spec))`,
    hints: [
      "For a running total, you want from the start up to the 'currentRow'",
      "Use spark_sum (aliased from sum) to add up values",
      "The .over() method applies the function to the window"
    ]
  };
