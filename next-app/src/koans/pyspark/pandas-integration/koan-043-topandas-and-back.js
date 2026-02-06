/**
 * Koan 43: toPandas and createDataFrame from pandas
 * Category: Pandas Integration
 */

const koan = {
  id: 43,
  title: "toPandas and Back",
  category: "Pandas Integration",
  difficulty: "advanced",
  description: "Convert between Spark DataFrames and pandas DataFrames. Replace ___ with the correct code.",
  setup: `
data = [("Alice", 30, 95000), ("Bob", 25, 72000), ("Carol", 35, 105000)]
df = spark.createDataFrame(data, ["name", "age", "salary"])
`,
  template: `# Convert Spark DataFrame to pandas
import pandas as pd

pdf = df.___()

# It's now a regular pandas DataFrame
assert isinstance(pdf, pd.DataFrame), "Should be a pandas DataFrame"
assert len(pdf) == 3
print("\\u2713 Converted Spark DataFrame to pandas")

# Access pandas-specific functionality
mean_salary = pdf["salary"].mean()
assert mean_salary == 90666.66666666667 or abs(mean_salary - 90666.67) < 1
print(f"\\u2713 Used pandas .mean(): {mean_salary:.2f}")

# Create a new pandas DataFrame and convert back to Spark
new_pdf = pd.DataFrame({
    "name": ["Dave", "Eve"],
    "age": [28, 32],
    "salary": [68000, 88000]
})

new_df = spark.___(new_pdf, ["name", "age", "salary"])

assert new_df.count() == 2
print("\\u2713 Created Spark DataFrame from pandas")

# Verify the round-trip
dave = new_df.filter(col("name") == "Dave").collect()[0]
assert dave["salary"] == 68000
print("\\u2713 Data preserved through round-trip")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to convert between Spark and pandas.")`,
  solution: `pdf = df.toPandas()

new_df = spark.createDataFrame(new_pdf, ["name", "age", "salary"])`,
  hints: [
    "toPandas() converts a Spark DataFrame to a pandas DataFrame",
    "spark.createDataFrame() can accept a pandas DataFrame as input"
  ],
  examCoverage: ["DEA", "DAA"]
};

export default koan;
