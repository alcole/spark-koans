/**
 * Koan 310: Converting to Spark and Pandas
 * Category: Pandas API on Spark
 * Difficulty: Intermediate
 */

export default {
  id: 310,
  title: "Converting to Spark and Pandas",
  category: "Pandas API on Spark",
  difficulty: "intermediate",
  description: "Learn how to convert between Pandas-on-Spark, PySpark, and regular Pandas. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a Pandas-on-Spark DataFrame
psdf = ps.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85, 92, 78],
    "grade": ["B", "A", "C"]
})

print("Pandas-on-Spark DataFrame:")
print(psdf)
`,

  template: `# Convert to PySpark DataFrame
spark_df = psdf.___()
print("\\nConverted to PySpark DataFrame:")
print(f"Type: {type(spark_df)}")
assert "pyspark.sql" in str(type(spark_df)), "Should be PySpark DataFrame"
print("✓ Converted to PySpark DataFrame")

# Now we can use PySpark operations
from pyspark.sql.functions import col
spark_filtered = spark_df.filter(col("score") > 80)
print(f"Filtered count: {spark_filtered.count()}")
print("✓ PySpark operations work")

# Convert PySpark back to Pandas-on-Spark
psdf2 = spark_df.___()
print("\\nConverted back to Pandas-on-Spark:")
print(f"Type: {type(psdf2)}")
print("✓ Converted back to Pandas-on-Spark")

# Convert to regular Pandas (collects data to driver)
# WARNING: Only do this for small datasets!
pandas_df = psdf.___()
print("\\nConverted to regular Pandas:")
print(f"Type: {type(pandas_df)}")
assert "pandas" in str(type(pandas_df)).lower(), "Should be pandas DataFrame"
print("✓ Converted to regular Pandas")

# Use regular pandas operations
print("\\nPandas describe:")
print(pandas_df.describe())
print("✓ Regular pandas operations work")

print("\\n🎉 Koan complete! You can convert between Spark and Pandas.")`,

  solution: `spark_df = psdf.to_spark()

psdf2 = spark_df.pandas_api()

pandas_df = psdf.to_pandas()`,

  hints: [
    ".to_spark() converts Pandas-on-Spark to PySpark DataFrame",
    ".pandas_api() converts PySpark to Pandas-on-Spark",
    ".to_pandas() converts to regular pandas (collects to driver)",
    "to_pandas() brings all data to memory - use carefully!",
    "Pandas-on-Spark is distributed, regular pandas is not",
    "Choose the API that best fits your needs"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [],
};
