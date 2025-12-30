/**
 * Koan 203: Creating External Tables
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 203,
  title: "Creating External Tables",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Learn the difference between managed and external tables in Unity Catalog. Replace ___ with the correct code.",

  setup: `
# Set up namespace
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_schema")
spark.sql("USE demo_catalog.demo_schema")

# Create and save data to a specific location
data = [("Product A", 100), ("Product B", 250), ("Product C", 175)]
df = spark.createDataFrame(data, ["product", "price"])
df.write.format("delta").save("/data/products_external")
`,

  template: `# Create an external table pointing to existing data
# External tables don't manage the underlying data files
spark.sql("""
  CREATE TABLE products_ext
  USING ___
  ___ '/data/products_external'
""")
print("✓ External table 'products_ext' created")

# Query the external table
result = spark.sql("SELECT * FROM products_ext")
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ External table data accessible")

# Check if table is external
table_info = spark.sql("DESCRIBE ___ products_ext").collect()
properties = {row.col_name: row.data_type for row in table_info}
print("✓ Table metadata retrieved")

print("\\n🎉 Koan complete! You understand managed vs external tables.")`,

  solution: `spark.sql("""
  CREATE TABLE products_ext
  USING delta
  LOCATION '/data/products_external'
""")
table_info = spark.sql("DESCRIBE EXTENDED products_ext").collect()`,

  hints: [
    "External tables use USING to specify the format (delta, parquet, etc.)",
    "The LOCATION clause points to where the data is stored",
    "DESCRIBE EXTENDED shows additional metadata including location",
    "Dropping external tables doesn't delete the underlying data"
  ],

  examCoverage: ["DEA", "DEP"],
  prerequisiteKoans: [202],
  nextKoans: [204],
};
