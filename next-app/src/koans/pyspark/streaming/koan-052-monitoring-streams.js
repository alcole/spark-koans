/**
 * Koan 52: Monitoring Streams
 * Category: Structured Streaming
 */

const koan = {
  id: 52,
  title: "Monitoring Streams",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Monitor streaming query progress and status. Replace ___ with the correct code.",
  setup: `
`,
  template: `# Monitor streaming queries using status, recentProgress, and lastProgress
from pyspark.sql.functions import col

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

query = stream_df.writeStream \\
    .format("memory") \\
    .queryName("monitored_stream") \\
    .outputMode("append") \\
    .start()

# Process a batch so we have progress data
query.processBatch()

# Check the query status
status = query.___
assert "isDataAvailable" in status, "Status should include isDataAvailable"
assert "isTriggerActive" in status, "Status should include isTriggerActive"
print(f"\\u2713 Query status: {status}")

# Get the most recent progress report
progress = query.___
assert progress is not None, "Should have progress after processing a batch"
assert "numInputRows" in progress
print(f"\\u2713 Last progress: {progress['numInputRows']} input rows")

# recentProgress returns a list of recent progress reports
recent = query.recentProgress
assert isinstance(recent, list)
assert len(recent) >= ___, "Should have at least 1 progress report"
print(f"\\u2713 Recent progress: {len(recent)} report(s)")

query.stop()
print("\\u2713 Query stopped")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to monitor streaming queries.")`,
  solution: `status = query.status

progress = query.lastProgress

assert len(recent) >= 1`,
  hints: [
    ".status returns the current status of the streaming query",
    ".lastProgress returns the most recent progress report",
    ".recentProgress returns a list of recent progress reports"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
