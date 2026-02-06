/**
 * Koan 217: Rate Source
 * Category: Structured Streaming
 */

const koan = {
  id: 217,
  title: "Rate Source for Testing",
  category: "Structured Streaming",
  difficulty: "intermediate",
  description: "Use the rate source to generate test streaming data. Replace ___ with the correct code.",
  setup: `
# Streaming shim is available
`,
  template: `# The rate source generates rows with (timestamp, value) at a fixed rate
# It's perfect for testing streaming pipelines without external data

# Read from the rate source
stream_df = spark.readStream \\
    .format(___) \\
    .option("rowsPerSecond", 10) \\
    .load()

# Verify it's a streaming DataFrame
assert stream_df.isStreaming == True, "Should be a streaming DataFrame"
print("\\u2713 Created streaming DataFrame from rate source")

# Check the schema - rate source always produces timestamp and value
assert "timestamp" in stream_df.columns, "Should have timestamp column"
assert "value" in stream_df.columns, "Should have value column"
print("\\u2713 Rate source schema: [timestamp, value]")

# Process a micro-batch to see the data
batch = stream_df.___()
assert len(batch) > 0, "Should have generated some rows"
assert "timestamp" in batch[0] and "value" in batch[0]
print(f"\\u2713 Got {len(batch)} rows from rate source")

# The value column is a monotonically increasing counter
values = [row["value"] for row in batch]
assert values == sorted(values), "Values should be monotonically increasing"
print("\\u2713 Values are monotonically increasing")

print("\\n\\ud83c\\udf89 Koan complete! You've learned the rate source for testing streams.")`,
  solution: `stream_df = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", 10) \\
    .load()

batch = stream_df.collectBatch()`,
  hints: [
    "The rate source format is simply \"rate\"",
    "collectBatch() processes one micro-batch for testing"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
