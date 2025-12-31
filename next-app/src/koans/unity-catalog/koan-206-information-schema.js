/**
 * Koan 206: Querying Information Schema
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 206,
  title: "Querying Information Schema",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Learn how to query Unity Catalog metadata using information_schema. Replace ___ with the correct code.",

  setup: `
# Set up catalog with multiple objects
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.analytics")
spark.sql("USE demo_catalog.analytics")

# Create several tables
spark.createDataFrame([("a", 1)], ["col1", "col2"]).write.mode("overwrite").saveAsTable("table1")
spark.createDataFrame([("b", 2)], ["col1", "col2"]).write.mode("overwrite").saveAsTable("table2")
`,

  template: `# Query all tables in the current schema using information_schema
tables_df = spark.sql("""
  SELECT table_name
  FROM ___.tables
  WHERE table_schema = 'analytics'
""")
print("✓ Queried information_schema.tables")

table_count = tables_df.count()
assert table_count >= 2, f"Expected at least 2 tables, got {table_count}"
print(f"✓ Found {table_count} tables in schema")

# Query column information
columns_df = spark.sql("""
  SELECT table_name, column_name, data_type
  FROM information_schema.___
  WHERE table_schema = 'analytics' AND table_name = 'table1'
""")
print("✓ Queried column metadata")

col_count = columns_df.count()
assert col_count == 2, f"Expected 2 columns, got {col_count}"
print("✓ Column information retrieved")

# Query all schemas in the catalog
schemas_df = spark.sql("""
  SELECT schema_name
  FROM information_schema.___
  WHERE catalog_name = 'demo_catalog'
""")
print("✓ Queried schema information")

print("\\n🎉 Koan complete! You can now discover Unity Catalog metadata.")`,

  solution: `tables_df = spark.sql("""
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = 'analytics'
""")

columns_df = spark.sql("""
  SELECT table_name, column_name, data_type
  FROM information_schema.columns
  WHERE table_schema = 'analytics' AND table_name = 'table1'
""")

schemas_df = spark.sql("""
  SELECT schema_name
  FROM information_schema.schemata
  WHERE catalog_name = 'demo_catalog'
""")`,

  hints: [
    "information_schema is a special schema available in every catalog",
    "Key views: tables, columns, schemata, catalogs",
    "Filter by catalog_name, table_schema, table_name as needed",
    "Great for data discovery and documentation"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [201, 202],
  nextKoans: [207],
};
