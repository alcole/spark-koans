/**
 * Koan 224: Processing Time Trigger
 * Category: Structured Streaming
 */

const koan = {
  id: 224,
  title: "Processing Time Trigger",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Use processingTime triggers to control micro-batch intervals. Replace ___ with the correct code.",
  setup: `
`,
  template: `# processingTime triggers process data at fixed intervals
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

# Different interval
query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("fast_stream") \\
    .outputMode("append") \\
    .trigger(processingTime=___) \\
    .start()

assert query2.trigger == {"processingTime": "1 minute"}
print("\\u2713 processingTime trigger: batch every minute")
query2.stop()

# Default: no trigger = process as fast as possible
print("\\u2713 Default trigger: continuous processing (no interval)")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered processingTime triggers.")`,
  solution: `query1 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("timed_stream") \\
    .outputMode("append") \\
    .trigger(processingTime="10 seconds") \\
    .start()

query2 = stream_df.writeStream \\
    .format("memory") \\
    .queryName("fast_stream") \\
    .outputMode("append") \\
    .trigger(processingTime="1 minute") \\
    .start()`,
  hints: [
    "processingTime takes a string like \"10 seconds\" or \"1 minute\"",
    "The interval string uses a number followed by a time unit",
    "Common units: seconds, minute, minutes"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
