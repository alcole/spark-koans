/**
 * Koan 202: Creating Managed Tables
 * Category: Unity Catalog
 * Difficulty: Beginner
 */

export default {
  id: 202,
  title: "Creating Managed Tables",
  category: "Unity Catalog",
  difficulty: "beginner",
  description: "Learn how to create managed tables in Unity Catalog. Replace ___ with the correct code.",

  setup: `
# Set up a catalog and schema
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_schema")
spark.sql("USE demo_catalog.demo_schema")

# Create sample data
data = [("Alice", 85), ("Bob", 92), ("Charlie", 78)]
df = spark.createDataFrame(data, ["name", "score"])
`,

  template: `# Create a managed table from the DataFrame
# Managed tables have their data managed by Unity Catalog
df.write.saveAsTable("___")
print("✓ Managed table 'students' created")

# Read back the table using the three-level namespace
result = spark.table("demo_catalog.demo_schema.___")
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Table data verified")

# Show table properties
spark.sql("___ TABLE demo_catalog.demo_schema.students").show()
print("✓ Table description displayed")

print("\\n🎉 Koan complete! You've created a managed table in Unity Catalog.")`,

  solution: `df.write.saveAsTable("students")
result = spark.table("demo_catalog.demo_schema.students")
spark.sql("DESCRIBE TABLE demo_catalog.demo_schema.students").show()`,

  hints: [
    "Use .saveAsTable() with just the table name (current schema is used)",
    "spark.table() reads a table by its full or relative name",
    "DESCRIBE TABLE shows table metadata and schema"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [201],
  nextKoans: [203],
};
