/**
 * Koan 210: Three-Level Namespace
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 210,
  title: "Three-Level Namespace Usage",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Master Unity Catalog's three-level namespace (catalog.schema.table). Replace ___ with the correct code.",

  setup: `
# Create multiple catalogs and schemas
spark.sql("CREATE CATALOG IF NOT EXISTS prod_catalog")
spark.sql("CREATE CATALOG IF NOT EXISTS dev_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS prod_catalog.sales")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev_catalog.sales")

# Create tables in different catalogs
spark.sql("USE prod_catalog.sales")
spark.createDataFrame([("Product A", 1000)], ["name", "revenue"]).write.mode("overwrite").saveAsTable("revenue")

spark.sql("USE dev_catalog.sales")
spark.createDataFrame([("Product A", 500)], ["name", "revenue"]).write.mode("overwrite").saveAsTable("revenue")
`,

  template: `# Query from production using full three-level name
prod_df = spark.sql("SELECT * FROM ___.sales.revenue")
prod_revenue = prod_df.collect()[0]["revenue"]
assert prod_revenue == 1000, f"Expected prod revenue 1000, got {prod_revenue}"
print("✓ Queried production table using full path")

# Query from development
dev_df = spark.sql("SELECT * FROM dev_catalog.___.___")
dev_revenue = dev_df.collect()[0]["revenue"]
assert dev_revenue == 500, f"Expected dev revenue 500, got {dev_revenue}"
print("✓ Queried development table using full path")

# Join tables across catalogs
result = spark.sql("""
  SELECT
    p.name,
    p.revenue as prod_revenue,
    d.revenue as dev_revenue
  FROM prod_catalog.sales.revenue p
  JOIN ___.sales.revenue d ON p.name = d.name
""")
print("✓ Joined tables across catalogs")

# Get current catalog and schema
current_catalog = spark.sql("SELECT ___()").collect()[0][0]
print(f"✓ Current catalog: {current_catalog}")

current_schema = spark.sql("SELECT current_database()").collect()[0][0]
print(f"✓ Current schema: {current_schema}")

print("\\n🎉 Koan complete! You've mastered Unity Catalog namespacing.")`,

  solution: `prod_df = spark.sql("SELECT * FROM prod_catalog.sales.revenue")

dev_df = spark.sql("SELECT * FROM dev_catalog.sales.revenue")

result = spark.sql("""
  SELECT
    p.name,
    p.revenue as prod_revenue,
    d.revenue as dev_revenue
  FROM prod_catalog.sales.revenue p
  JOIN dev_catalog.sales.revenue d ON p.name = d.name
""")

current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]`,

  hints: [
    "Use fully qualified names: catalog.schema.table",
    "This allows querying across different catalogs in one query",
    "current_catalog() returns the current catalog",
    "current_database() returns the current schema",
    "Three-level namespace enables data isolation and organization",
    "Critical for managing dev/test/prod environments"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [201, 202],
  nextKoans: [],
};
