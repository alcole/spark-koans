/**
 * Koan 220: Streaming Queries
 * Category: Structured Streaming
 */

const koan = {
  id: 220,
  title: "Streaming Queries",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Start and manage streaming queries with writeStream. Replace ___ with the correct code.",
  setup: `
`,
  template: `# A streaming query is started by calling writeStream on a streaming DataFrame
from pyspark.sql.functions import col

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Start a streaming query with writeStream
query = stream_df \\
    .writeStream \\
    .format("memory") \\
    .queryName("my_stream") \\
    .outputMode(___) \\
    .start()

# The query object lets you manage the stream
assert query.isActive == True, "Query should be active after start()"
print("\\u2713 Started streaming query")

# Check the query name
assert query.name == "my_stream"
print("\\u2713 Query has the correct name")

# Get the query ID (unique identifier)
assert query.id is not None
print(f"\\u2713 Query ID: {query.id}")

# Process a batch
query.processBatch()
print("\\u2713 Processed a micro-batch")

# Stop the query
query.___()
assert query.isActive == ___, "Query should be inactive after stop()"
print("\\u2713 Stopped streaming query")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to manage streaming queries.")`,
  solution: `query = stream_df \\
    .writeStream \\
    .format("memory") \\
    .queryName("my_stream") \\
    .outputMode("append") \\
    .start()

query.stop()
assert query.isActive == False`,
  hints: [
    "outputMode can be 'append', 'complete', or 'update'",
    "'append' is the most common - new rows are added to the output",
    "stop() terminates a running streaming query"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
