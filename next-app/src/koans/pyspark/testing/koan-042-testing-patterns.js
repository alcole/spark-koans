/**
 * Koan 42: Testing Patterns
 * Category: Testing
 */

const koan = {
  id: 42,
  title: "Testing Patterns",
  category: "Testing",
  difficulty: "advanced",
  description: "Apply real-world DataFrame testing patterns. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

data = [
    ("Alice", "Engineering", 95000),
    ("Bob", "Marketing", 72000),
    ("Carol", "Engineering", 105000),
    ("Dave", "Marketing", 68000),
    ("Eve", "Engineering", 88000)
]
df = spark.createDataFrame(data, ["name", "dept", "salary"])
`,
  template: `# Real-world testing pattern: test a transformation pipeline
from pyspark.sql.functions import col, avg as spark_avg
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql import Row

# Step 1: Define the transformation under test
def avg_salary_by_dept(input_df):
    return input_df.groupBy("dept").agg(
        spark_avg("salary").alias("avg_salary")
    )

# Step 2: Run the transformation
result = avg_salary_by_dept(df)

# Step 3: Check schema FIRST (fail fast if structure is wrong)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
expected_schema = StructType([
    StructField("dept", StringType(), True),
    StructField("avg_salary", DoubleType(), True)
])
___(result.schema, expected_schema)
print("\\u2713 Schema validation passed")

# Step 4: Check data with expected results
expected_rows = [
    Row(dept="Engineering", avg_salary=96000.0),
    Row(dept="Marketing", avg_salary=___)
]
assertDataFrameEqual(result, expected_rows)
print("\\u2713 Aggregation results correct")

# Step 5: Test edge case - empty input
empty_df = spark.createDataFrame([], ["name", "dept", "salary"])
empty_result = avg_salary_by_dept(empty_df)
assert empty_result.count() == ___, "Empty input should produce empty output"
print("\\u2713 Edge case: empty input handled")

print("\\n\\ud83c\\udf89 Koan complete! You've learned real-world testing patterns.")`,
  solution: `assertSchemaEqual(result.schema, expected_schema)

expected_rows = [
    Row(dept="Engineering", avg_salary=96000.0),
    Row(dept="Marketing", avg_salary=70000.0)
]

assert empty_result.count() == 0`,
  hints: [
    "Always validate schema before data - it catches structural issues early",
    "Marketing average: (72000 + 68000) / 2 = 70000.0",
    "An aggregation on an empty DataFrame should return 0 rows"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
