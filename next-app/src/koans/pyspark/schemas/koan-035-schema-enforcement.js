/**
 * Koan 35: Schema Enforcement and Inspection
 * Category: Schemas
 */

const koan = {
  id: 35,
  title: "Schema Enforcement and Inspection",
  category: "Schemas",
  difficulty: "advanced",
  description: "Inspect and compare DataFrame schemas. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

data1 = [("Alice", 30, 75000.50), ("Bob", 25, 62000.00)]
schema1 = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])
df1 = spark.createDataFrame(data1, schema1)

data2 = [("Carol", 35, 89000.75), ("Dave", 28, 71000.25)]
schema2 = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])
df2 = spark.createDataFrame(data2, schema2)

data3 = [("Eve", "thirty", 80000.00)]
schema3 = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df3 = spark.createDataFrame(data3, schema3)
`,
  template: `# Inspect a DataFrame's schema
schema = df1.___

assert len(schema.fields) == 3, "Should have 3 fields"
print("\\u2713 Accessed DataFrame schema")

# Check a specific field's type
age_field = schema["age"]
assert isinstance(age_field.dataType, ___), f"age should be IntegerType"
print("\\u2713 Inspected field data type")

# Compare schemas of two DataFrames
schemas_match = df1.schema == df2.___
assert schemas_match == True, "df1 and df2 should have matching schemas"
print("\\u2713 Matching schemas detected")

# Detect schema mismatch
schemas_differ = df1.schema == df3.schema
assert schemas_differ == ___, "df1 and df3 have different schemas (age is String vs Integer)"
print("\\u2713 Schema mismatch detected")

# Find the mismatched field
df1_age_type = type(df1.schema["age"].dataType).__name__
df3_age_type = type(df3.schema["age"].dataType).__name__
assert df1_age_type != df3_age_type, f"Types should differ: {df1_age_type} vs {df3_age_type}"
print(f"\\u2713 Found mismatch: df1.age is {df1_age_type}, df3.age is {df3_age_type}")

print("\\n\\ud83c\\udf89 Koan complete! You've learned schema enforcement and inspection.")`,
  solution: `schema = df1.schema

age_field = schema["age"]
assert isinstance(age_field.dataType, IntegerType)

schemas_match = df1.schema == df2.schema

schemas_differ = df1.schema == df3.schema
assert schemas_differ == False`,
  hints: [
    "Access a DataFrame's schema with the .schema property",
    "Schema fields have a .dataType attribute",
    "Schemas with different field types will not be equal"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
