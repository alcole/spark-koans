/**
 * Koan 209: assertDataFrameEqual Basics
 * Category: Testing
 */

const koan = {
  id: 209,
  title: "assertDataFrameEqual Basics",
  category: "Testing",
  difficulty: "intermediate",
  description: "Use assertDataFrameEqual to compare DataFrames in tests. Replace ___ with the correct code.",
  setup: `
data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
df = spark.createDataFrame(data, ["name", "age"])
`,
  template: `# assertDataFrameEqual compares two DataFrames for equality
from pyspark.testing.utils import assertDataFrameEqual

# Create an expected DataFrame to compare against
expected_data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
expected = spark.createDataFrame(expected_data, ["name", "age"])

# This should pass - the DataFrames are equal
___(df, expected)
print("\\u2713 DataFrames are equal")

# Order doesn't matter by default (checkRowOrder=False)
reordered_data = [("Carol", 35), ("Alice", 30), ("Bob", 25)]
reordered = spark.createDataFrame(reordered_data, ["name", "age"])

assertDataFrameEqual(df, ___)
print("\\u2713 Row order is ignored by default")

# You can also compare against a list of Row objects
from pyspark.sql import Row
expected_rows = [Row(name="Alice", age=30), Row(name="Bob", age=25), Row(name="Carol", age=35)]
assertDataFrameEqual(df, ___)
print("\\u2713 Can compare against a list of Rows")

# Detect inequality - this should raise an AssertionError
wrong_data = [("Alice", 99), ("Bob", 25), ("Carol", 35)]
wrong_df = spark.createDataFrame(wrong_data, ["name", "age"])

try:
    assertDataFrameEqual(df, wrong_df)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Correctly detected unequal DataFrames")

print("\\n\\ud83c\\udf89 Koan complete! You've learned assertDataFrameEqual basics.")`,
  solution: `assertDataFrameEqual(df, expected)

assertDataFrameEqual(df, reordered)

assertDataFrameEqual(df, expected_rows)`,
  hints: [
    "assertDataFrameEqual(actual, expected) compares two DataFrames",
    "By default, row order doesn't matter",
    "You can pass a list of Row objects as the expected value"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
