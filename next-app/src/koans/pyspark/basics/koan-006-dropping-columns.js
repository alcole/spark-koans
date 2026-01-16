/**
 * Koan 6: Dropping Columns
 * Category: Basics
 */

const koan = {
    id: 6,
    title: "Dropping Columns",
    category: "Basics",
    description: "Remove columns from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34, "NYC", "F"), ("Bob", 45, "LA", "M")]
df = spark.createDataFrame(data, ["name", "age", "city", "gender"])
`,
    template: `# Drop the 'gender' column
result = df.___("gender")

assert "gender" not in result.columns, "gender column should be dropped"
assert len(result.columns) == 3, f"Expected 3 columns, got {len(result.columns)}"
print("✓ Dropped gender column")

# Drop multiple columns
result2 = df.___("city", "gender")
assert len(result2.columns) == 2, f"Expected 2 columns, got {len(result2.columns)}"
print("✓ Dropped multiple columns")

print("\\n🎉 Koan complete! You've learned to drop columns.")`,
    solution: `result = df.drop("gender")\nresult2 = df.drop("city", "gender")`,
    hints: [
      "The opposite of 'select' for removing columns is 'drop'",
      "You can drop multiple columns by passing multiple arguments"
    ]
  };

export default koan;
