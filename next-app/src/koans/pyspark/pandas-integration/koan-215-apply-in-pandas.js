/**
 * Koan 45: applyInPandas (Grouped Map)
 * Category: Pandas Integration
 */

const koan = {
  id: 215,
  title: "applyInPandas - Grouped Map",
  category: "Pandas Integration",
  difficulty: "advanced",
  description: "Use applyInPandas to apply custom pandas logic per group. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

data = [
    ("Engineering", "Alice", 95000.0),
    ("Engineering", "Carol", 105000.0),
    ("Engineering", "Eve", 88000.0),
    ("Marketing", "Bob", 72000.0),
    ("Marketing", "Dave", 68000.0)
]
df = spark.createDataFrame(data, ["dept", "name", "salary"])
`,
  template: `# applyInPandas: apply a function to each group as a pandas DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd

# Output schema must be defined
output_schema = StructType([
    StructField("dept", StringType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("dept_avg", DoubleType(), True),
    StructField("diff_from_avg", DoubleType(), True)
])

# This function receives a pandas DataFrame for EACH group
def add_dept_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    dept_avg = pdf["salary"].mean()
    pdf["dept_avg"] = dept_avg
    pdf["diff_from_avg"] = pdf["salary"] - dept_avg
    return pdf

# Apply the function per group
result = df.groupBy(___).applyInPandas(add_dept_stats, schema=___)

assert "dept_avg" in result.columns and "diff_from_avg" in result.columns
print("\\u2713 applyInPandas added computed columns")

# Check Engineering department
eng = result.filter(col("dept") == "Engineering")
eng_rows = eng.collect()
eng_avg = eng_rows[0]["dept_avg"]
expected_avg = (95000 + 105000 + 88000) / 3
assert abs(eng_avg - expected_avg) < 0.01, f"Expected ~{expected_avg}, got {eng_avg}"
print(f"\\u2713 Engineering dept_avg = {eng_avg:.2f}")

# Alice's diff from average
alice = result.filter(col("name") == "Alice").collect()[0]
assert abs(alice["diff_from_avg"] - (95000 - expected_avg)) < 0.01
print("\\u2713 Individual diff_from_avg calculated correctly")

print("\\n\\ud83c\\udf89 Koan complete! You've learned applyInPandas.")`,
  solution: `result = df.groupBy("dept").applyInPandas(add_dept_stats, schema=output_schema)`,
  hints: [
    "groupBy the column you want to split on, then call applyInPandas",
    "applyInPandas takes (function, schema=output_schema)",
    "The output schema must match what your function returns"
  ],
  examCoverage: ["DEP"]
};

export default koan;
