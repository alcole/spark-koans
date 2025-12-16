/**
 * Koan 4: Adding Columns
 * Category: Basics
 * Difficulty: Beginner
 */

export default {
  id: 4,
  title: "Adding Columns",
  category: "Basics",
  difficulty: "beginner",
  description: "Learn how to add new calculated columns. Replace ___ with the correct code.",

  setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])
`,

  template: `# Add a new column 'age_in_months' that multiplies age by 12
from pyspark.sql.functions import col

result = df.___("age_in_months", col("___") * 12)

# Should still have 3 rows
assert result.count() == 3
print("✓ Row count unchanged")

# Should now have 3 columns
assert len(result.columns) == 3, f"Expected 3 columns, got {len(result.columns)}"
print("✓ New column added")

# Check calculation is correct
first_row = result.filter(col("name") == "Alice").collect()[0]
assert first_row["age_in_months"] == 408, f"Expected 408, got {first_row['age_in_months']}"
print("✓ Calculation is correct (34 * 12 = 408)")

print("\\n🎉 Koan complete! You've learned to add columns.")`,

  solution: `result = df.withColumn("age_in_months", col("age") * 12)`,

  hints: [
    "The method name suggests adding 'with' a new 'Column'",
    "First argument is the new column name, second is the expression",
    "Reference the 'age' column to multiply it"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [1, 2, 3],
  nextKoans: [5],
};
