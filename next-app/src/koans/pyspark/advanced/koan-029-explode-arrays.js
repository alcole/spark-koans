/**
 * Koan 29: Explode Arrays
 * Category: Advanced
 */

export default {
    id: 29,
    title: "Explode Arrays",
    category: "Advanced",
    difficulty: "advanced",
    description: "Expand array columns into multiple rows. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.functions import explode, split, col

data = [("Alice", "python,sql,spark"), ("Bob", "java,scala")]
df = spark.createDataFrame(data, ["name", "skills_str"])

# First split the string into an array
df = df.withColumn("skills", split(col("skills_str"), ","))
`,
    template: `# Explode the skills array into separate rows
from pyspark.sql.functions import explode, col

result = df.select("name", ___(col("skills")).alias("skill"))

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
print("✓ Exploded to 5 rows")

alice_skills = [row["skill"] for row in result.filter(col("name") == "Alice").collect()]
assert len(alice_skills) == 3, f"Alice should have 3 skills, got {len(alice_skills)}"
assert "spark" in alice_skills
print("✓ Alice has 3 skills including spark")

print("\\n🎉 Koan complete! You've learned to explode arrays.")`,
    solution: `result = df.select("name", explode(col("skills")).alias("skill"))`,
    hints: [
      "explode() turns each array element into a separate row",
      "The original row is duplicated for each array element"
    ]
  };
