/**
 * Koan 25: Window Functions - Lag and Lead
 * Category: Window Functions
 */

const koan = {
    id: 25,
    title: "Window Functions - Lag and Lead",
    category: "Window Functions",
    description: "Access previous or next row values. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 120),
    ("2024-01-04", 200)
]
df = spark.createDataFrame(data, ["date", "price"])
`,
    template: `# Get yesterday's price and calculate daily change
window_spec = Window.orderBy("date")

result = df.withColumn("prev_price", ___("price", 1).over(window_spec))
result = result.withColumn("change", col("price") - col("prev_price"))

rows = result.orderBy("date").collect()

# First row has no previous
assert rows[0]["prev_price"] is None, "First row should have no prev_price"
print("✓ First row has no previous")

# Second row: prev=100, change=50
assert rows[1]["prev_price"] == 100, f"Expected prev=100, got {rows[1]['prev_price']}"
assert rows[1]["change"] == 50, f"Expected change=50, got {rows[1]['change']}"
print("✓ Day 2: prev=100, change=+50")

# Get tomorrow's price
result2 = df.withColumn("next_price", ___("price", 1).over(window_spec))
rows2 = result2.orderBy("date").collect()
assert rows2[0]["next_price"] == 150, f"Expected next=150, got {rows2[0]['next_price']}"
print("✓ Lead shows next day's price")

print("\\n🎉 Koan complete! You've learned lag and lead.")`,
    solution: `result = df.withColumn("prev_price", lag("price", 1).over(window_spec))\nresult2 = df.withColumn("next_price", lead("price", 1).over(window_spec))`,
    hints: [
      "lag() looks at previous rows",
      "lead() looks at following rows",
      "The second argument is how many rows to look back/forward"
    ]
  };

export default koan;
