/**
 * Koan 212: Floating-Point Tolerance
 * Category: Testing
 */

const koan = {
  id: 212,
  title: "Floating-Point Tolerance",
  category: "Testing",
  difficulty: "advanced",
  description: "Use atol and rtol for floating-point comparisons in assertDataFrameEqual. Replace ___ with the correct code.",
  setup: `
data = [("Alice", 30.001), ("Bob", 25.002), ("Carol", 35.003)]
df = spark.createDataFrame(data, ["name", "score"])
`,
  template: `# Floating-point values rarely match exactly
from pyspark.testing.utils import assertDataFrameEqual

# Rounded values we want to compare against
approx_data = [("Alice", 30.0), ("Bob", 25.0), ("Carol", 35.0)]
approx = spark.createDataFrame(approx_data, ["name", "score"])

# atol = absolute tolerance (max allowed difference)
# 30.001 vs 30.0 differs by 0.001 — need atol >= 0.001
assertDataFrameEqual(df, approx, atol=___)
print("\\u2713 Absolute tolerance comparison passed")

# rtol = relative tolerance (fraction of expected value)
# Use rtol when error scales with magnitude
approx2 = [("Alice", 30.1), ("Bob", 25.1), ("Carol", 35.1)]
approx_df2 = spark.createDataFrame(approx2, ["name", "score"])

assertDataFrameEqual(df, approx_df2, rtol=___)
print("\\u2713 Relative tolerance comparison passed")

# Without tolerance, small differences cause failure
try:
    assertDataFrameEqual(df, approx)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Without tolerance, tiny differences fail")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered floating-point tolerance.")`,
  solution: `assertDataFrameEqual(df, approx, atol=0.01)

assertDataFrameEqual(df, approx_df2, rtol=0.01)`,
  hints: [
    "atol sets the maximum absolute difference allowed between values",
    "The biggest difference is 0.003 (35.003 vs 35.0), so atol must be >= 0.003",
    "rtol=0.01 means a 1% relative difference is acceptable"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
