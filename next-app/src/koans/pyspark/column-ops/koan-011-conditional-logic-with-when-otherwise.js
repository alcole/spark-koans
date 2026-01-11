/**
 * Koan 11: Conditional Logic with when/otherwise
 * Category: Column Operations
 */

export default {
    id: 11,
    title: "Conditional Logic with when/otherwise",
    category: "Column Operations",
    difficulty: "intermediate",
    description: "Create columns with conditional logic. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 17), ("Diana", 65)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Create an 'age_group' column based on age
from pyspark.sql.functions import when, col

result = df.withColumn(
    "age_group",
    _____(col("age") < 18, "minor")
    .when(col("age") < 65, "adult")
    ._____("senior")
)

rows = result.collect()
groups = {row["name"]: row["age_group"] for row in rows}

assert groups["Charlie"] == "minor", f"Charlie should be minor, got {groups['Charlie']}"
print("✓ Charlie (17) is minor")

assert groups["Alice"] == "adult", f"Alice should be adult, got {groups['Alice']}"
print("✓ Alice (34) is adult")

assert groups["Diana"] == "senior", f"Diana should be senior, got {groups['Diana']}"
print("✓ Diana (65) is senior")

print("\\n🎉 Koan complete! You've learned conditional column logic.")`,
    solution: `result = df.withColumn("age_group", when(col("age") < 18, "minor").when(col("age") < 65, "adult").otherwise("senior"))`,
    hints: [
      "Start with 'when' for the first condition",
      "Chain additional .when() for more conditions",
      "End with .otherwise() for the default case"
    ]
  };
