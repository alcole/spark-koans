/**
 * Koan 41: assertSchemaEqual
 * Category: Testing
 */

const koan = {
  id: 211,
  title: "assertSchemaEqual",
  category: "Testing",
  difficulty: "advanced",
  description: "Use assertSchemaEqual to validate DataFrame schemas. Replace ___ with the correct code.",
  setup: `
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema_a = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])
df_a = spark.createDataFrame([("Alice", 30, 75000.0)], schema_a)

schema_b = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])
df_b = spark.createDataFrame([("Bob", 25, 60000.0)], schema_b)

schema_c = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df_c = spark.createDataFrame([("Carol", "35", 80000.0)], schema_c)
`,
  template: `# assertSchemaEqual compares only schemas, not data
from pyspark.testing.utils import assertSchemaEqual

# Schemas match (ignoring nullable by default)
assertSchemaEqual(df_a.schema, df_b.___)
print("\\u2713 Schemas match (nullable differences ignored)")

# By default, nullable differences are ignored
# Set ignoreNullable=False to enforce strict nullable matching
try:
    assertSchemaEqual(df_a.schema, df_b.schema, ignoreNullable=___)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Nullable mismatch detected with ignoreNullable=False")

# Type mismatches are always caught
try:
    assertSchemaEqual(df_a.schema, df_c.schema)
    assert False, "Should have raised AssertionError"
except AssertionError:
    print("\\u2713 Type mismatch detected (IntegerType vs StringType)")

# Best practice: check schema FIRST, then data
# If schemas don't match, data comparison is meaningless
assertSchemaEqual(df_a.schema, df_b.schema)  # schema ok (nullable ignored)
print("\\u2713 Schema check passed - safe to compare data")

print("\\n\\ud83c\\udf89 Koan complete! You've learned assertSchemaEqual.")`,
  solution: `assertSchemaEqual(df_a.schema, df_b.schema)

assertSchemaEqual(df_a.schema, df_b.schema, ignoreNullable=False)`,
  hints: [
    "assertSchemaEqual compares StructType schemas",
    "ignoreNullable=True (default) ignores nullable differences",
    "Set ignoreNullable=False to strictly compare nullable flags"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
