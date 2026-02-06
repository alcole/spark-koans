/**
 * Koan 224: transformWithStateInPandas
 * Category: Structured Streaming
 */

const koan = {
  id: 224,
  title: "transformWithStateInPandas",
  category: "Structured Streaming",
  difficulty: "advanced",
  description: "Use transformWithStateInPandas for custom stateful streaming. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
`,
  template: `# transformWithStateInPandas: custom stateful processing on streaming data
# Each group maintains its own state across micro-batches
from pyspark.sql.functions import col, when
import pandas as pd

# Define output schema
output_schema = StructType([
    StructField("key", StringType(), True),
    StructField("running_total", DoubleType(), True),
    StructField("batch_count", IntegerType(), True)
])

# The stateful function receives:
# - key: the grouping key tuple
# - pdf_iter: iterator of pandas DataFrames for this key
# - state: a state handle to read/write persistent state
def running_total_fn(key, pdf_iter, state):
    # Read previous state or initialize
    if state.exists:
        total = state.get["total"]
        count = state.get["count"]
    else:
        total = 0.0
        count = 0

    for pdf in pdf_iter:
        total += pdf["value"].sum()
        count += 1

    # Save updated state
    state.update({"total": total, "count": count})

    # Yield output
    yield pd.DataFrame({
        "key": [key[0]],
        "running_total": [total],
        "batch_count": [count]
    })

# Apply to a streaming DataFrame grouped by key
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
stream_df = stream_df.withColumn("group", when(col("value") % 2 == 0, "even").otherwise("odd"))

result = stream_df \\
    .groupBy(___) \\
    .transformWithStateInPandas(
        func=running_total_fn,
        outputStructType=___,
        outputMode="update"
    )

assert result.isStreaming == True
print("\\u2713 Created stateful streaming pipeline")

# Process two batches to see state accumulation
query = result.writeStream.format("memory").queryName("stateful").outputMode("update").start()
query.processBatch()
batch1 = query.getBatch()
print(f"\\u2713 Batch 1: {len(batch1)} groups processed")

query.processBatch()
batch2 = query.getBatch()
# State should accumulate across batches
if len(batch2) > 0:
    assert batch2[0]["batch_count"] >= ___, "Batch count should increase"
    print(f"\\u2713 State accumulated across batches (count={batch2[0]['batch_count']})")

query.stop()

print("\\n\\ud83c\\udf89 Koan complete! You've learned transformWithStateInPandas.")`,
  solution: `result = stream_df \\
    .groupBy("group") \\
    .transformWithStateInPandas(
        func=running_total_fn,
        outputStructType=output_schema,
        outputMode="update"
    )

assert batch2[0]["batch_count"] >= 2`,
  hints: [
    "groupBy the column you want to maintain separate state for",
    "outputStructType defines the schema of the output DataFrames",
    "After 2 batches, the batch_count should be at least 2"
  ],
  examCoverage: ["DEP"]
};

export default koan;
