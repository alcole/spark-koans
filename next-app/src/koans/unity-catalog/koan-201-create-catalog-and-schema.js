/**
 * Koan 201: Creating Catalogs and Schemas
 * Category: Unity Catalog
 * Difficulty: Beginner
 */

export default {
  id: 201,
  title: "Creating Catalogs and Schemas",
  category: "Unity Catalog",
  difficulty: "beginner",
  description: "Learn how to create catalogs and schemas in Unity Catalog's three-level namespace. Replace ___ with the correct code.",

  setup: `
# Unity Catalog uses a three-level namespace: catalog.schema.table
# First, let's see what's available
spark.sql("SHOW CATALOGS").show()
`,

  template: `# Create a new catalog
spark.sql("___ CATALOG my_catalog")
print("✓ Catalog 'my_catalog' created")

# Create a schema (database) within the catalog
spark.sql("CREATE ___ my_catalog.my_schema")
print("✓ Schema 'my_catalog.my_schema' created")

# Verify the schema was created
schemas = spark.sql("SHOW SCHEMAS IN ___").collect()
schema_names = [row.databaseName for row in schemas]
assert "my_schema" in schema_names, f"Schema not found. Available: {schema_names}"
print("✓ Schema verified in catalog")

# Set the current schema for subsequent operations
spark.sql("___ my_catalog.my_schema")
print("✓ Current schema set to my_catalog.my_schema")

print("\\n🎉 Koan complete! You've created a Unity Catalog namespace.")`,

  solution: `spark.sql("CREATE CATALOG my_catalog")
spark.sql("CREATE SCHEMA my_catalog.my_schema")
schemas = spark.sql("SHOW SCHEMAS IN my_catalog").collect()
spark.sql("USE my_catalog.my_schema")`,

  hints: [
    "Use CREATE CATALOG to create a new catalog",
    "Use CREATE SCHEMA with the full path: catalog.schema",
    "SHOW SCHEMAS IN catalog_name lists schemas",
    "USE catalog.schema sets the current namespace"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [1],
  nextKoans: [202],
};
