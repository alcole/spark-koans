/**
 * Koan 9: Renaming Columns
 * Category: Column Operations
 */

export default {
    id: 9,
    title: "Renaming Columns",
    category: "Column Operations",
    description: "Rename columns in a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Rename 'name' to 'employee_name'
from pyspark.sql.functions import col

result = df.___(___, "employee_name")

assert "employee_name" in result.columns, "Should have employee_name column"
assert "name" not in result.columns, "Should not have name column anymore"
print("✓ Renamed name to employee_name")

# Rename using alias in select
result2 = df.select(col("name").___("full_name"), col("age"))
assert "full_name" in result2.columns, "Should have full_name column"
print("✓ Used alias in select")

print("\\n🎉 Koan complete! You've learned to rename columns.")`,
    solution: `result = df.withColumnRenamed("name", "employee_name")\nresult2 = df.select(col("name").alias("full_name"), col("age"))`,
    hints: [
      "withColumnRenamed takes the old name and new name as arguments",
      "The syntax is withColumnRenamed(old_name, new_name)",
      "alias() is used within select() to rename on the fly"
    ]
  };
