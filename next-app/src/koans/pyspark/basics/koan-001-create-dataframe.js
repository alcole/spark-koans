/**
 * Koan 1: Creating a DataFrame
 * Category: Basics
 * Difficulty: Beginner
 */

const koan = {
  id: 1,
  title: "Creating a DataFrame",
  category: "Basics",
  difficulty: "beginner",
  description: "Learn how to create a DataFrame from Python data structures. Replace ___ with the correct code.",

  setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
`,

  template: `# Create a DataFrame from the data and columns
df = spark.___(___, ___)

# The DataFrame should have 3 rows
assert df.count() == 3, f"Expected 3 rows, got {df.count()}"
print("✓ DataFrame created with correct row count")

# The DataFrame should have 2 columns
assert len(df.columns) == 2, f"Expected 2 columns, got {len(df.columns)}"
print("✓ DataFrame has correct number of columns")

print("\\n🎉 Koan complete! You've learned to create a DataFrame.")`,

  solution: `df = spark.createDataFrame(data, columns)`,

  hints: [
    "DataFrames are created from SparkSession",
    "The method name describes what you're doing: create + DataFrame",
    "Pass the data first, then the column names"
  ],

  // Metadata for tracking
  examCoverage: ["DEA", "DAA", "MLA"],
  prerequisiteKoans: [],
  nextKoans: [2],
};

export default koan;
