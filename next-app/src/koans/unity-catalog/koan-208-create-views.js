/**
 * Koan 208: Creating Views in Unity Catalog
 * Category: Unity Catalog
 * Difficulty: Beginner
 */

export default {
  id: 208,
  title: "Creating Views in Unity Catalog",
  category: "Unity Catalog",
  difficulty: "beginner",
  description: "Learn how to create and use views in Unity Catalog. Replace ___ with the correct code.",

  setup: `
# Set up namespace and base table
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.views_demo")
spark.sql("USE demo_catalog.views_demo")

data = [
  ("Alice", "Engineering", 85000),
  ("Bob", "Sales", 75000),
  ("Charlie", "Engineering", 95000),
  ("Diana", "Sales", 80000)
]
df = spark.createDataFrame(data, ["name", "department", "salary"])
df.write.mode("overwrite").saveAsTable("employees")
`,

  template: `# Create a view that filters high earners
spark.sql("""
  CREATE ___ high_earners AS
  SELECT name, department, salary
  FROM employees
  WHERE salary > 80000
""")
print("✓ View 'high_earners' created")

# Query the view
result = spark.sql("SELECT * FROM ___")
assert result.count() == 2, f"Expected 2 high earners, got {result.count()}"
print("✓ View query successful")

# Create a temporary view (session-scoped)
spark.sql("""
  CREATE ___ VIEW sales_team AS
  SELECT name, salary
  FROM employees
  WHERE department = 'Sales'
""")
print("✓ Temporary view created")

# Verify temporary view works
temp_result = spark.sql("SELECT * FROM sales_team")
assert temp_result.count() == 2, f"Expected 2 sales members"
print("✓ Temporary view query successful")

print("\\n🎉 Koan complete! You can now use views to simplify queries.")`,

  solution: `spark.sql("""
  CREATE VIEW high_earners AS
  SELECT name, department, salary
  FROM employees
  WHERE salary > 80000
""")

result = spark.sql("SELECT * FROM high_earners")

spark.sql("""
  CREATE TEMP VIEW sales_team AS
  SELECT name, salary
  FROM employees
  WHERE department = 'Sales'
""")`,

  hints: [
    "CREATE VIEW creates a persistent view in the catalog",
    "Views are stored queries that are executed when referenced",
    "CREATE TEMP VIEW creates a session-scoped view (not persisted)",
    "Views can reference tables using three-level namespace",
    "Use CREATE OR REPLACE VIEW to update existing views"
  ],

  examCoverage: ["DEA", "DEP", "DAA"],
  prerequisiteKoans: [202],
  nextKoans: [209],
};
