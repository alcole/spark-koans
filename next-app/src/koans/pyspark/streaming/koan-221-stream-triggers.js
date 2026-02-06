/**
 * Koan 221: Stream Triggers
 * Category: Structured Streaming
 */

const koan = {
  id: 221,
  title: "Stream Triggers",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Configure trigger modes for streaming queries. Replace ___ with the correct code.",
  setup: `
`,
  template: `# Triggers control WHEN a micro-batch is processed
from pyspark.sql.functions import col

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# processingTime trigger: process at fixed intervals
query1 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("timed_stream") \\
    .outputMode("append") \\
    .trigger(processingTime=___) \\
    .start()

assert query1.trigger == {"processingTime": "10 seconds"}
print("\\u2713 processingTime trigger: batch every 10 seconds")
query1.stop()

# availableNow trigger: process all available data then stop
query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("batch_once") \\
    .outputMode("append") \\
    .trigger(___=True) \\
    .start()

assert query2.trigger == {"availableNow": True}
print("\\u2713 availableNow trigger: process all data and stop")
query2.stop()

# once trigger (deprecated but good to know): process exactly one batch
query3 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("once_stream") \\
    .outputMode("append") \\
    .trigger(once=True) \\
    .start()

print("\\u2713 once trigger: single micro-batch (deprecated, use availableNow)")
query3.stop()

# Default: no trigger specified = process as fast as possible
print("\\u2713 Default trigger: continuous processing (no interval)")

print("\\n\\ud83c\\udf89 Koan complete! You've learned streaming trigger modes.")`,
  solution: `query1 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("timed_stream") \\
    .outputMode("append") \\
    .trigger(processingTime="10 seconds") \\
    .start()

query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("batch_once") \\
    .outputMode("append") \\
    .trigger(availableNow=True) \\
    .start()`,
  hints: [
    "processingTime takes a string like \"10 seconds\" or \"1 minute\"",
    "availableNow=True processes all available data then stops",
    "once=True is deprecated in favor of availableNow"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
