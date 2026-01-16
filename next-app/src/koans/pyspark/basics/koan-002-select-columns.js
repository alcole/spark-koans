/**
 * Koan 2: Selecting Columns
 * Category: Basics
 * Difficulty: Beginner
 */

const koan = {
  id: 2,
  title: "Selecting Columns",
  category: "Basics",
  difficulty: "beginner",
  description: "Learn how to select specific columns from a DataFrame. Replace ___ with the correct code.",

  setup: `
data = [("Alice", 34, "NYC"), ("Bob", 45, "LA"), ("Charlie", 29, "Chicago")]
df = spark.createDataFrame(data, ["name", "age", "city"])
`,

  template: `# Select only the 'name' and 'city' columns
result = df.___("name", "___")

# Result should have exactly 2 columns
assert len(result.columns) == 2, f"Expected 2 columns, got {len(result.columns)}"
print("✓ Correct number of columns selected")

# Result should contain 'name' and 'city'
assert "name" in result.columns, "Missing 'name' column"
assert "city" in result.columns, "Missing 'city' column"
print("✓ Correct columns selected")

print("\\n🎉 Koan complete! You've learned to select columns.")`,

  solution: `result = df.select("name", "city")`,

  hints: [
    "Think about what action you want: you want to 'select' columns",
    "The method takes column names as strings"
  ],

  examCoverage: ["DEA", "DAA", "MLA"],
  prerequisiteKoans: [1],
  nextKoans: [3],
};

export default koan;
