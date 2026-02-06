/**
 * Koan 202: Map Types
 * Category: Complex Types
 */

const koan = {
  id: 202,
  title: "Map Types",
  category: "Complex Types",
  difficulty: "advanced",
  description: "Create and query map (dictionary) columns. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", "math", 95, "science", 88),
    ("Bob", "math", 72, "science", 91),
    ("Carol", "math", 85, "science", 79)
]
df = spark.createDataFrame(data, ["name", "subject1", "score1", "subject2", "score2"])
`,
  template: `# Build a map column from key-value pairs
from pyspark.sql.functions import col, create_map, lit

# Create a map column: {subject1: score1, subject2: score2}
result = df.select(
    "name",
    ___(col("subject1"), col("score1"), col("subject2"), col("score2")).alias("scores")
)

assert "scores" in result.columns
print("\\u2713 Created map column")

# Access a value from the map by key
alice = result.filter(col("name") == "Alice").collect()[0]
scores = alice["scores"]
assert scores["math"] == 95, f"Expected math=95, got {scores['math']}"
assert scores["science"] == 88, f"Expected science=88, got {scores['science']}"
print("\\u2713 Map values are accessible by key")

# Use map_keys and map_values to inspect the map
from pyspark.sql.functions import map_keys, map_values

keys_df = result.select("name", ___(col("scores")).alias("subjects"))
first_keys = keys_df.collect()[0]["subjects"]
assert "math" in first_keys and "science" in first_keys
print("\\u2713 Extracted map keys")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to work with map types.")`,
  solution: `result = df.select(
    "name",
    create_map(col("subject1"), col("score1"), col("subject2"), col("score2")).alias("scores")
)

keys_df = result.select("name", map_keys(col("scores")).alias("subjects"))`,
  hints: [
    "create_map() takes alternating key, value columns",
    "map_keys() extracts all keys from a map column as an array"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
