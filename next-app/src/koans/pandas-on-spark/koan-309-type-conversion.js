/**
 * Koan 309: Type Conversion and Casting
 * Category: Pandas API on Spark
 * Difficulty: Beginner
 */

export default {
  id: 309,
  title: "Type Conversion and Casting",
  category: "Pandas API on Spark",
  difficulty: "beginner",
  description: "Learn how to convert data types in Pandas API on Spark. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a DataFrame with mixed types
psdf = ps.DataFrame({
    "id": ["1", "2", "3", "4"],  # String that should be int
    "price": ["10.5", "20.3", "15.7", "30.2"],  # String that should be float
    "quantity": [10, 20, 15, 30]  # Already int
})

print("Original DataFrame:")
print(psdf)
print("\\nOriginal dtypes:")
print(psdf.dtypes)
`,

  template: `# Convert string to integer
psdf["id"] = psdf["id"].astype(___)
print("\\nAfter converting id to int:")
print(psdf.dtypes["id"])
assert psdf.dtypes["id"] == "int64" or "int" in str(psdf.dtypes["id"]), "id should be integer"
print("✓ Converted to int")

# Convert string to float
psdf["price"] = psdf["price"].astype(___)
print("\\nAfter converting price to float:")
print(psdf.dtypes["price"])
assert "float" in str(psdf.dtypes["price"]), "price should be float"
print("✓ Converted to float")

# Convert to string
psdf["quantity_str"] = psdf["quantity"].astype(___)
print("\\nCreated quantity_str:")
print(psdf[["quantity", "quantity_str"]])
assert psdf.dtypes["quantity_str"] == "object" or "str" in str(psdf.dtypes["quantity_str"]), "Should be string"
print("✓ Converted to string")

# Check current dtypes
print("\\nFinal dtypes:")
print(psdf.___)
print("✓ All conversions complete")

# Perform calculations with converted types
total_value = (psdf["price"] * psdf["quantity"]).sum()
print(f"\\nTotal value: ${total_value:.2f}")
assert total_value > 0, "Total value should be calculated"
print("✓ Calculations work with converted types")

print("\\n🎉 Koan complete! You can now convert data types.")`,

  solution: `psdf["id"] = psdf["id"].astype(int)

psdf["price"] = psdf["price"].astype(float)

psdf["quantity_str"] = psdf["quantity"].astype(str)

print(psdf.dtypes)`,

  hints: [
    "astype(type) converts Series to specified type",
    "Common types: int, float, str",
    "Type names: 'int64', 'float64', 'object' (string)",
    "dtypes shows data types of all columns",
    "Type conversion enables proper arithmetic operations"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301],
  nextKoans: [310],
};
