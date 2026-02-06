/**
 * Koan 40: assertDataFrameEqual Advanced Options
 * Category: Testing
 */

const koan = {
  id: 210,
  title: "assertDataFrameEqual Advanced Options",
  category: "Testing",
  difficulty: "advanced",
  description: "Use advanced assertDataFrameEqual options for precise testing. Replace ___ with the correct code.",
  setup: `
data = [("Alice", 30.001), ("Bob", 25.002), ("Carol", 35.003)]
df = spark.createDataFrame(data, ["name", "score"])

ordered_data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
df_ordered = spark.createDataFrame(ordered_data, ["name", "rank"])
`,
  template: `# Advanced assertDataFrameEqual options
from pyspark.testing.utils import assertDataFrameEqual

# Use tolerances for floating-point comparison
approx_data = [("Alice", 30.0), ("Bob", 25.0), ("Carol", 35.0)]
approx = spark.createDataFrame(approx_data, ["name", "score"])

# atol = absolute tolerance, rtol = relative tolerance
assertDataFrameEqual(df, approx, atol=___)
print("\\u2713 Floating-point comparison with tolerance")

# Enforce strict row ordering with checkRowOrder=True
expected_order = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
expected_ordered = spark.createDataFrame(expected_order, ["name", "rank"])

assertDataFrameEqual(df_ordered, expected_ordered, checkRowOrder=___)
print("\\u2713 Strict row order check passed")

# Wrong order should fail with checkRowOrder=True
wrong_order = [("Bob", 2), ("Alice", 1), ("Carol", 3)]
wrong_ordered = spark.createDataFrame(wrong_order, ["name", "rank"])

try:
    assertDataFrameEqual(df_ordered, wrong_ordered, checkRowOrder=True)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Strict ordering correctly rejects wrong order")

# ignoreNullable ignores nullable differences in schema comparison
print("\\u2713 assertDataFrameEqual supports ignoreNullable, ignoreColumnOrder, ignoreColumnType")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered assertDataFrameEqual options.")`,
  solution: `assertDataFrameEqual(df, approx, atol=0.01)

assertDataFrameEqual(df_ordered, expected_ordered, checkRowOrder=True)`,
  hints: [
    "atol sets the absolute tolerance for floating-point comparisons",
    "checkRowOrder=True enforces that rows appear in the same order",
    "A tolerance of 0.01 would match 30.001 to 30.0"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
