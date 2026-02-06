/**
 * Koan 49: Streaming Transformations
 * Category: Structured Streaming
 */

const koan = {
  id: 49,
  title: "Streaming Transformations",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Apply DataFrame transformations to streaming data. Replace ___ with the correct code.",
  setup: `
`,
  template: `# The key insight: streaming DataFrames support the SAME transformations
# as batch DataFrames - select, filter, withColumn, groupBy, etc.
from pyspark.sql.functions import col, upper

# Start with a rate stream
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# Apply batch-style transformations to a stream
filtered = stream_df.___(col("value") > 5)
assert filtered.isStreaming == True, "Transformations preserve streaming nature"
print("\\u2713 filter() works on streaming DataFrames")

# Add a computed column
enriched = stream_df.withColumn("doubled", col("value") * ___)
assert "doubled" in enriched.columns
assert enriched.isStreaming == True
print("\\u2713 withColumn() works on streaming DataFrames")

# Select specific columns
selected = stream_df.___(col("value"))
assert selected.columns == ["value"]
assert selected.isStreaming == True
print("\\u2713 select() works on streaming DataFrames")

# Chain multiple transformations (just like batch!)
pipeline = stream_df \\
    .filter(col("value") > 0) \\
    .withColumn("category",
        when(col("value") > 50, "high").otherwise("low")) \\
    .select("value", "category")

assert pipeline.isStreaming == True
assert pipeline.columns == ["value", "category"]
print("\\u2713 Chained transformations on streaming DataFrame")

print("\\n\\ud83c\\udf89 Koan complete! Streaming uses the same API as batch.")`,
  solution: `filtered = stream_df.filter(col("value") > 5)

enriched = stream_df.withColumn("doubled", col("value") * 2)

selected = stream_df.select(col("value"))`,
  hints: [
    "Streaming DataFrames use the exact same transformation API as batch",
    "filter(), withColumn(), select() all work on streams",
    "Transformations preserve the isStreaming property"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
