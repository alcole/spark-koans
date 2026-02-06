/**
 * Koan 44: Pandas UDFs (Scalar)
 * Category: Pandas Integration
 */

const koan = {
  id: 214,
  title: "Pandas UDFs - Scalar",
  category: "Pandas Integration",
  difficulty: "advanced",
  description: "Use pandas UDFs for vectorized operations. Replace ___ with the correct code.",
  setup: `
data = [("Alice", 95000), ("Bob", 72000), ("Carol", 105000), ("Dave", 68000)]
df = spark.createDataFrame(data, ["name", "salary"])
`,
  template: `# Pandas UDFs process data in vectorized batches using pandas Series
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Define a pandas UDF to calculate tax (30% bracket)
@pandas_udf(___)
def calculate_tax(salary: pd.Series) -> pd.Series:
    return salary * 0.30

# Apply the pandas UDF like any other function
result = df.select("name", "salary", ___(col("salary")).alias("tax"))

alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["tax"] == 28500.0, f"Expected tax 28500.0, got {alice['tax']}"
print("\\u2713 Pandas UDF calculated tax correctly")

# Define a pandas UDF for net salary
@pandas_udf(DoubleType())
def net_salary(salary: pd.Series) -> pd.Series:
    return salary - (salary * 0.30)

result2 = df.select("name", net_salary(col("salary")).alias("net"))

bob = result2.filter(col("name") == "Bob").collect()[0]
assert bob["net"] == 50400.0, f"Expected net 50400.0, got {bob['net']}"
print("\\u2713 Chained pandas UDF for net salary")

# Pandas UDFs are faster than regular Python UDFs because
# they use Apache Arrow for serialization and process batches
print("\\u2713 Pandas UDFs use vectorized (batch) processing")

print("\\n\\ud83c\\udf89 Koan complete! You've learned scalar pandas UDFs.")`,
  solution: `@pandas_udf(DoubleType())
def calculate_tax(salary: pd.Series) -> pd.Series:
    return salary * 0.30

result = df.select("name", "salary", calculate_tax(col("salary")).alias("tax"))`,
  hints: [
    "@pandas_udf decorator takes the return type as its argument",
    "DoubleType() for floating-point return values",
    "Apply a pandas UDF just like any built-in function: udf_name(col(...))"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
