/**
 * Koan 213: checkRowOrder Enforcement
 * Category: Testing
 */

const koan = {
  id: 213,
  title: "checkRowOrder Enforcement",
  category: "Testing",
  difficulty: "advanced",
  description: "Use checkRowOrder to enforce row ordering in assertDataFrameEqual. Replace ___ with the correct code.",
  setup: `
ordered_data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
df_ordered = spark.createDataFrame(ordered_data, ["name", "rank"])
`,
  template: `# By default, assertDataFrameEqual ignores row order
from pyspark.testing.utils import assertDataFrameEqual

# Same data, different order
expected = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
expected_df = spark.createDataFrame(expected, ["name", "rank"])

# Without checkRowOrder, order doesn't matter
shuffled = [("Carol", 3), ("Alice", 1), ("Bob", 2)]
shuffled_df = spark.createDataFrame(shuffled, ["name", "rank"])
assertDataFrameEqual(df_ordered, shuffled_df)
print("\\u2713 Default: row order is ignored")

# Enable strict row order checking
assertDataFrameEqual(df_ordered, expected_df, checkRowOrder=___)
print("\\u2713 Strict row order check passed")

# Wrong order should fail with checkRowOrder=True
wrong_order = [("Bob", 2), ("Alice", 1), ("Carol", 3)]
wrong_df = spark.createDataFrame(wrong_order, ["name", "rank"])

try:
    assertDataFrameEqual(df_ordered, wrong_df, checkRowOrder=___)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Strict ordering correctly rejects wrong order")

print("\\n\\ud83c\\udf89 Koan complete! You've mastered checkRowOrder.")`,
  solution: `assertDataFrameEqual(df_ordered, expected_df, checkRowOrder=True)

try:
    assertDataFrameEqual(df_ordered, wrong_df, checkRowOrder=True)`,
  hints: [
    "checkRowOrder is a boolean parameter — True or False",
    "When checkRowOrder=True, rows must appear in the exact same order",
    "Both blanks need the same value to enforce strict ordering"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
