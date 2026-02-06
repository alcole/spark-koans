/**
 * Koan 223: Sliding Windows
 * Category: Structured Streaming
 */

const koan = {
  id: 223,
  title: "Sliding Windows",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Use time-based sliding windows to aggregate streaming data. Replace ___ with the correct code.",
  setup: `
`,
  template: `# Sliding windows group events by time intervals
from pyspark.sql.functions import col, window, sum as spark_sum

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Create a tumbling window (window = slide): events grouped into fixed intervals
tumbling = stream_df \\
    .groupBy(window(col("timestamp"), ___)) \\
    .agg(spark_sum("value").alias("total"))

assert tumbling.isStreaming == True
assert "window" in tumbling.columns
print("\\u2713 Created tumbling window aggregation (10 seconds)")

# Create a sliding window: overlapping intervals
# window(timeColumn, windowDuration, slideDuration)
sliding = stream_df \\
    .groupBy(___(col("timestamp"), "10 seconds", "5 seconds")) \\
    .agg(spark_sum("value").alias("total"))

assert sliding.isStreaming == True
print("\\u2713 Created sliding window (10s window, 5s slide)")

# Process a batch and check the windowed results
query = tumbling.writeStream \\
    .format("memory") \\
    .queryName("windowed") \\
    .outputMode("complete") \\
    .start()

query.processBatch()
batch = query.getBatch()

# Window results have a struct column with start and end
if len(batch) > 0:
    first_window = batch[0]["window"]
    assert "start" in first_window and "end" in first_window
    print(f"\\u2713 Window has start and end: {first_window}")

query.stop()

print("\\n\\ud83c\\udf89 Koan complete! You've learned sliding windows in streaming.")`,
  solution: `tumbling = stream_df \\
    .groupBy(window(col("timestamp"), "10 seconds")) \\
    .agg(spark_sum("value").alias("total"))

sliding = stream_df \\
    .groupBy(window(col("timestamp"), "10 seconds", "5 seconds")) \\
    .agg(spark_sum("value").alias("total"))`,
  hints: [
    "window() with one duration creates a tumbling window",
    "window() with two durations creates a sliding window (window, slide)",
    "Tumbling = non-overlapping intervals, Sliding = overlapping intervals"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
