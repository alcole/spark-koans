/**
 * Koan 48: DataStreamReader
 * Category: Structured Streaming
 */

const koan = {
  id: 48,
  title: "DataStreamReader",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Configure DataStreamReader to read streaming data. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
`,
  template: `# DataStreamReader is accessed via spark.readStream
# It configures how to read streaming data

# Define a schema for our stream
schema = StructType([
    StructField("user", StringType(), True),
    StructField("action", StringType(), True),
    StructField("count", IntegerType(), True)
])

# Configure the stream reader with format, schema, and options
stream_df = spark.___ \\
    .format("memory") \\
    .schema(schema) \\
    .option("maxBatchSize", 100) \\
    .load()

assert stream_df.isStreaming == True
print("\\u2713 Created streaming DataFrame with DataStreamReader")

# Verify schema was applied
assert stream_df.columns == ["user", "action", "count"]
print("\\u2713 Schema applied to streaming DataFrame")

# The readStream builder pattern: format -> schema -> options -> load
# Format specifies the source (kafka, rate, memory, files, etc.)
reader = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", ___)

stream2 = reader.___()
assert stream2.isStreaming == True
print("\\u2713 Builder pattern: format -> options -> load")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to configure DataStreamReader.")`,
  solution: `stream_df = spark.readStream \\
    .format("memory") \\
    .schema(schema) \\
    .option("maxBatchSize", 100) \\
    .load()

reader = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", 5)

stream2 = reader.load()`,
  hints: [
    "spark.readStream starts the DataStreamReader builder",
    "load() finalizes the reader and returns a streaming DataFrame",
    "rowsPerSecond can be any positive number"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
