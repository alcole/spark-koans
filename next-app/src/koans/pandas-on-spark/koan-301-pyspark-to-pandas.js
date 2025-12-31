/**
 * Koan 301: Converting to Pandas API on Spark
 * Category: Pandas API on Spark
 * Difficulty: Beginner
 */

export default {
  id: 301,
  title: "Converting to Pandas API on Spark",
  category: "Pandas API on Spark",
  difficulty: "beginner",
  description: "Learn how to convert PySpark DataFrames to use the Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
# Create a PySpark DataFrame
data = [("Alice", 25, "NYC"), ("Bob", 30, "LA"), ("Charlie", 35, "Chicago")]
spark_df = spark.createDataFrame(data, ["name", "age", "city"])
print("PySpark DataFrame created")
`,

  template: `# Convert PySpark DataFrame to Pandas-on-Spark DataFrame
import pyspark.pandas as ps

# Method 1: Convert from PySpark DataFrame
psdf = spark_df.___()
print("✓ Converted to Pandas-on-Spark DataFrame")

# Check the type
print(f"Type: {type(psdf)}")
assert "pandas" in str(type(psdf)).lower(), "Should be a pandas-on-spark DataFrame"
print("✓ Type verified")

# Pandas-on-Spark supports familiar pandas operations
print("\\nDataFrame head:")
print(psdf.___(2))  # Show first 2 rows
print("✓ head() works like pandas")

# Method 2: Create directly from data
psdf2 = ps.___({"name": ["Dave", "Eve"], "age": [28, 32]})
print("✓ Created Pandas-on-Spark DataFrame from dict")

print("\\n🎉 Koan complete! You can now use pandas syntax with Spark.")`,

  solution: `psdf = spark_df.pandas_api()

print(psdf.head(2))

psdf2 = ps.DataFrame({"name": ["Dave", "Eve"], "age": [28, 32]})`,

  hints: [
    "Use .pandas_api() method on a PySpark DataFrame",
    "Import pyspark.pandas (aliased as ps) for pandas-like API",
    "head() shows first n rows, just like pandas",
    "ps.DataFrame() creates a new Pandas-on-Spark DataFrame from dict"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [1, 2],
  nextKoans: [302],
};
