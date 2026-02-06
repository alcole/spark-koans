/**
 * Koan 109: Create Table with Builder
 * Category: Delta Lake
 */

const koan = {
    id: 109,
    title: "Create Table with Builder",
    category: "Delta Lake",
    description: "Create a Delta table with an explicit schema using the builder API. Replace ___ with the correct code.",
    setup: `
_reset_delta_tables()
`,
    template: `from delta.tables import DeltaTable

# Create a Delta table with explicit schema
DeltaTable._____(spark) \\
    .tableName("products") \\
    .addColumn("id", "INT") \\
    .addColumn("___", "STRING") \\
    .addColumn("price", "DOUBLE") \\
    .execute()

# Verify table was created
assert DeltaTable.isDeltaTable(spark, "products"), "Table should exist"
print("✓ Table 'products' created")

# Get the table and verify schema
dt = DeltaTable.forName(spark, "products")
df = dt.toDF()

assert "name" in df.columns, "Should have 'name' column"
assert "price" in df.columns, "Should have 'price' column"
print("✓ Table has correct columns")

assert df.count() == 0, "New table should be empty"
print("✓ Table is empty (ready for data)")

print("\\n🎉 Koan complete! You've learned the DeltaTable builder.")`,
    solution: `DeltaTable.create(spark).tableName("products").addColumn("id", "INT").addColumn("name", "STRING").addColumn("price", "DOUBLE").execute()`,
    hints: [
      "Use DeltaTable.create(spark) to start the builder",
      "The missing column name is 'name'",
      "Don't forget .execute() at the end"
    ]
  };

export default koan;
