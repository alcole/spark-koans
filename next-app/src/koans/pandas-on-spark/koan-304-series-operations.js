/**
 * Koan 304: Series Operations
 * Category: Pandas API on Spark
 * Difficulty: Beginner
 */

export default {
  id: 304,
  title: "Series Operations",
  category: "Pandas API on Spark",
  difficulty: "beginner",
  description: "Learn how to work with Series in Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a DataFrame
psdf = ps.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "score": [85, 92, 78, 88]
})
`,

  template: `# Extract a Series (single column)
scores = psdf["___"]
print("Scores Series:")
print(scores)
print(f"Type: {type(scores)}")
print("✓ Extracted Series from DataFrame")

# Get Series statistics
mean_score = scores.___()
print(f"\\nMean score: {mean_score}")
assert mean_score > 80, f"Mean should be > 80"
print("✓ mean() calculated")

max_score = scores.___()
print(f"Max score: {max_score}")
assert max_score == 92, f"Max should be 92"
print("✓ max() found")

# Apply operations to Series
# Add 5 bonus points to all scores
adjusted_scores = scores + ___
print("\\nAdjusted scores (bonus +5):")
print(adjusted_scores)
print("✓ Arithmetic operations work on Series")

# Filter Series
high_scores = scores[scores >= ___]
print("\\nScores >= 85:")
print(high_scores)
assert len(high_scores) == 3, f"Expected 3 high scores"
print("✓ Boolean filtering works on Series")

# Get unique values
names_series = psdf["name"]
unique_names = names_series.___()
print(f"\\nUnique names count: {len(unique_names)}")
print("✓ unique() works on Series")

print("\\n🎉 Koan complete! You can now work with Series.")`,

  solution: `scores = psdf["score"]

mean_score = scores.mean()

max_score = scores.max()

adjusted_scores = scores + 5

high_scores = scores[scores >= 85]

unique_names = names_series.unique()`,

  hints: [
    "Access a column with df['column_name'] to get a Series",
    "Series has methods like mean(), max(), min(), sum()",
    "Arithmetic operations work element-wise",
    "Filter Series with boolean conditions: series[series > value]",
    "unique() returns unique values in a Series"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [305],
};
