/**
 * Koan 24: Window Functions - Row Number
 * Category: Window Functions
 */

export default {
    id: 24,
    title: "Window Functions - Row Number",
    category: "Window Functions",
    description: "Assign sequential row numbers within groups. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 5500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["dept", "name", "salary"])
`,
    template: `# Rank employees within each department by salary (highest first)
window_spec = Window.partitionBy("___").orderBy(col("salary").desc())

result = df.withColumn("rank", ___().___(window_spec))

# Check rankings
eng = result.filter(col("dept") == "Engineering").orderBy("rank").collect()
assert eng[0]["name"] == "Diana", f"Diana should be #1 in Engineering, got {eng[0]['name']}"
assert eng[0]["rank"] == 1
print("✓ Diana is #1 in Engineering ($6500)")

assert eng[1]["name"] == "Charlie", f"Charlie should be #2, got {eng[1]['name']}"
print("✓ Charlie is #2 in Engineering ($6000)")

print("\\n🎉 Koan complete! You've learned row_number().")`,
    solution: `window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())\nresult = df.withColumn("rank", row_number().over(window_spec))`,
    hints: [
      "partitionBy creates groups like groupBy but for windows",
      "row_number() assigns 1, 2, 3... within each partition",
      "Use .over() to apply to the window specification"
    ]
  };
