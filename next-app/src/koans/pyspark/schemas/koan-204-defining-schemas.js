/**
 * Koan 204: Defining Schemas with StructType
 * Category: Schemas
 */

const koan = {
  id: 204,
  title: "Defining Schemas with StructType",
  category: "Schemas",
  difficulty: "intermediate",
  description: "Define explicit schemas using StructType and StructField. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", 30, 75000.50),
    ("Bob", 25, 62000.00),
    ("Carol", 35, 89000.75)
]
`,
  template: `# Define an explicit schema instead of relying on inference
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    ___("name", StringType(), False),
    StructField("age", ___(), True),
    StructField("salary", DoubleType(), ___)
])

df = spark.createDataFrame(data, schema)

# Verify the schema was applied
assert df.columns == ["name", "age", "salary"], f"Expected [name, age, salary], got {df.columns}"
print("\\u2713 DataFrame created with explicit schema")

# Check data types via the schema
field_names = [f.name for f in df.schema.fields]
assert field_names == ["name", "age", "salary"]
print("\\u2713 Schema field names correct")

field_types = [type(f.dataType).__name__ for f in df.schema.fields]
assert field_types == ["StringType", "IntegerType", "DoubleType"], f"Got types: {field_types}"
print("\\u2713 Schema data types correct")

# Check nullable
assert df.schema.fields[0].nullable == False, "name should be non-nullable"
assert df.schema.fields[1].nullable == True, "age should be nullable"
print("\\u2713 Nullable constraints correct")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to define schemas with StructType.")`,
  solution: `schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])`,
  hints: [
    "StructField takes (name, dataType, nullable)",
    "IntegerType() represents integer columns",
    "The third argument to StructField controls whether nulls are allowed"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
