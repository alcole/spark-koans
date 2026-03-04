/**
 * Koan 225: Available Now Trigger
 * Category: Structured Streaming
 */

const koan = {
  id: 225,
  title: "Available Now Trigger",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Use availableNow triggers to process all available data and stop. Replace ___ with the correct code.",
  setup: `
`,
  template: `# availableNow processes all available data then stops
from pyspark.sql.functions import col

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# availableNow trigger: process all available data then stop
query1 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("batch_once") \\
    .outputMode("append") \\
    .trigger(___=True) \\
    .start()

assert query1.trigger == {"availableNow": True}
print("\\u2713 availableNow trigger: process all data and stop")
query1.stop()

# once trigger (deprecated): process exactly one batch
# availableNow is the recommended replacement
query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("once_stream") \\
    .outputMode("append") \\
    .trigger(once=___) \\
    .start()

assert query2.trigger == {"once": True}
print("\\u2713 once trigger: single micro-batch (deprecated, use availableNow)")
query2.stop()

# Key difference: availableNow may split data into multiple batches
# while once processes everything in a single batch
print("\\u2713 Prefer availableNow over deprecated once trigger")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered availableNow triggers.")`,
  solution: `query1 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("batch_once") \\
    .outputMode("append") \\
    .trigger(availableNow=True) \\
    .start()

query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("once_stream") \\
    .outputMode("append") \\
    .trigger(once=True) \\
    .start()`,
  hints: [
    "availableNow=True processes all available data then stops the query",
    "once=True is the deprecated predecessor of availableNow",
    "Both blanks take boolean True"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
