/**
 * Koan 216: mapInPandas (Row-wise)
 * Category: Pandas Integration
 */

const koan = {
  id: 216,
  title: "mapInPandas - Row-wise Processing",
  category: "Pandas Integration",
  difficulty: "advanced",
  description: "Use mapInPandas to process DataFrame batches without grouping. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

data = [
    ("Alice", 95000.0),
    ("Bob", 72000.0),
    ("Carol", 105000.0),
    ("Dave", 68000.0)
]
df = spark.createDataFrame(data, ["name", "salary"])
`,
  template: `# mapInPandas processes the entire DataFrame in pandas batches
# Unlike applyInPandas, it doesn't require groupBy
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd

# Output schema for our transformation
output_schema = StructType([
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("tax_bracket", StringType(), True)
])

# mapInPandas function receives an iterator of pandas DataFrames
# and must yield pandas DataFrames
def classify_tax_bracket(iterator):
    for pdf in iterator:
        pdf["tax_bracket"] = pdf["salary"].apply(
            lambda s: "high" if s >= 90000 else "standard"
        )
        yield pdf

# Apply without grouping - processes all rows in batches
result = df.___(classify_tax_bracket, schema=___)

assert "tax_bracket" in result.columns
print("\\u2713 mapInPandas added tax_bracket column")

alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["tax_bracket"] == "high", f"Expected 'high', got '{alice['tax_bracket']}'"
print("\\u2713 Alice correctly classified as high bracket")

bob = result.filter(col("name") == "Bob").collect()[0]
assert bob["tax_bracket"] == "standard", f"Expected 'standard', got '{bob['tax_bracket']}'"
print("\\u2713 Bob correctly classified as standard bracket")

assert result.count() == 4, "Should preserve all rows"
print("\\u2713 All rows preserved")

print("\\n\\ud83c\\udf89 Koan complete! You've learned mapInPandas.")`,
  solution: `result = df.mapInPandas(classify_tax_bracket, schema=output_schema)`,
  hints: [
    "mapInPandas is called directly on a DataFrame, no groupBy needed",
    "The function receives an iterator of pandas DataFrames and must yield DataFrames",
    "Pass the output schema as the second argument"
  ],
  examCoverage: ["DEP"]
};

export default koan;
