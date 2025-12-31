/**
 * Koan 207: Table Properties and Comments
 * Category: Unity Catalog
 * Difficulty: Beginner
 */

export default {
  id: 207,
  title: "Table Properties and Comments",
  category: "Unity Catalog",
  difficulty: "beginner",
  description: "Learn how to add metadata to tables using properties and comments. Replace ___ with the correct code.",

  setup: `
# Set up namespace
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.metadata_demo")
spark.sql("USE demo_catalog.metadata_demo")
`,

  template: `# Create a table with a comment
spark.sql("""
  CREATE TABLE products (
    product_id INT,
    product_name STRING,
    price DOUBLE
  )
  ___ 'Product catalog table containing all available products'
""")
print("✓ Table created with comment")

# Add a property to the table
spark.sql("""
  ALTER TABLE products
  SET ___ ('owner' = 'data_team', 'pii' = 'false')
""")
print("✓ Table properties set")

# Add a comment to a specific column
spark.sql("""
  ALTER TABLE products
  ALTER COLUMN price ___ 'Price in USD'
""")
print("✓ Column comment added")

# View table properties
spark.sql("DESCRIBE EXTENDED products").show()
print("✓ Table metadata displayed")

print("\\n🎉 Koan complete! You can now document tables with metadata.")`,

  solution: `spark.sql("""
  CREATE TABLE products (
    product_id INT,
    product_name STRING,
    price DOUBLE
  )
  COMMENT 'Product catalog table containing all available products'
""")

spark.sql("""
  ALTER TABLE products
  SET TBLPROPERTIES ('owner' = 'data_team', 'pii' = 'false')
""")

spark.sql("""
  ALTER TABLE products
  ALTER COLUMN price COMMENT 'Price in USD'
""")`,

  hints: [
    "Use COMMENT after CREATE TABLE to add table descriptions",
    "SET TBLPROPERTIES adds key-value metadata to tables",
    "ALTER COLUMN ... COMMENT adds descriptions to columns",
    "DESCRIBE EXTENDED shows all metadata including properties"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [202],
  nextKoans: [208],
};
