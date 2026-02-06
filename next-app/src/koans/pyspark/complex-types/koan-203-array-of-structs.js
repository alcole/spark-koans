/**
 * Koan 33: Arrays of Structs
 * Category: Complex Types
 */

const koan = {
  id: 203,
  title: "Arrays of Structs",
  category: "Complex Types",
  difficulty: "advanced",
  description: "Work with arrays containing struct elements. Replace ___ with the correct code.",
  setup: `
data = [
    ("Alice", [{"item": "laptop", "price": 999}, {"item": "mouse", "price": 25}]),
    ("Bob", [{"item": "keyboard", "price": 75}, {"item": "monitor", "price": 450}, {"item": "webcam", "price": 60}]),
    ("Carol", [{"item": "headset", "price": 150}])
]
df = spark.createDataFrame(data, ["customer", "orders"])
`,
  template: `# Work with arrays of structs
from pyspark.sql.functions import col, explode, size

# Count items per customer using size()
counted = df.select("customer", ___(col("orders")).alias("num_items"))

bob_items = counted.filter(col("customer") == "Bob").collect()[0]["num_items"]
assert bob_items == 3, f"Expected Bob to have 3 items, got {bob_items}"
print("\\u2713 Counted array elements with size()")

# Explode the array to get one row per order
exploded = df.select("customer", ___(col("orders")).alias("order"))

assert exploded.count() == 6, f"Expected 6 rows after explode, got {exploded.count()}"
print("\\u2713 Exploded array of structs")

# Access struct fields after exploding
items = exploded.select("customer", col("order").getField("item").alias("item"), col("order").getField("price").alias("price"))
laptop = items.filter(col("item") == "laptop").collect()[0]
assert laptop["price"] == 999, f"Expected laptop price 999, got {laptop['price']}"
print("\\u2713 Accessed struct fields after explode")

print("\\n\\ud83c\\udf89 Koan complete! You've learned to work with arrays of structs.")`,
  solution: `counted = df.select("customer", size(col("orders")).alias("num_items"))

exploded = df.select("customer", explode(col("orders")).alias("order"))`,
  hints: [
    "size() returns the number of elements in an array column",
    "explode() creates one row per array element, even for struct arrays"
  ],
  examCoverage: ["DEA", "DEP"]
};

export default koan;
