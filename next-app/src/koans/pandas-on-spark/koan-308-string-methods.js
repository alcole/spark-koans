/**
 * Koan 308: String Methods (.str accessor)
 * Category: Pandas API on Spark
 * Difficulty: Intermediate
 */

export default {
  id: 308,
  title: "String Methods (.str accessor)",
  category: "Pandas API on Spark",
  difficulty: "intermediate",
  description: "Learn how to use the .str accessor for string operations. Replace ___ with the correct code.",

  setup: `
import pyspark.pandas as ps

# Create a DataFrame with string data
psdf = ps.DataFrame({
    "name": ["  Alice  ", "BOB", "charlie", "DIANA"],
    "email": ["alice@example.com", "bob@test.com", "charlie@demo.org", "diana@example.com"]
})
`,

  template: `# Convert to lowercase
names_lower = psdf["name"].str.___()
print("Lowercase names:")
print(names_lower)
print("✓ .str.lower() works")

# Convert to uppercase
names_upper = psdf["name"].str.___()
print("\\nUppercase names:")
print(names_upper)
print("✓ .str.upper() works")

# Strip whitespace
names_clean = psdf["name"].str.___()
print("\\nCleaned names (stripped):")
print(names_clean)
assert "  " not in names_clean.tolist()[0], "Whitespace should be removed"
print("✓ .str.strip() removes whitespace")

# Check if string contains a pattern
has_example = psdf["email"].str.___(\"example\")
print("\\nEmails containing 'example':")
print(has_example)
example_count = has_example.sum()
assert example_count == 2, f"Expected 2 emails with 'example'"
print("✓ .str.contains() works")

# Extract domain from email (split on @)
domains = psdf["email"].str.split("@").str.___(-1)
print("\\nEmail domains:")
print(domains)
print("✓ .str.split() and indexing works")

# String length
name_lengths = psdf["name"].str.___()
print("\\nName lengths:")
print(name_lengths)
print("✓ .str.len() works")

print("\\n🎉 Koan complete! You can now manipulate strings with .str accessor.")`,

  solution: `names_lower = psdf["name"].str.lower()

names_upper = psdf["name"].str.upper()

names_clean = psdf["name"].str.strip()

has_example = psdf["email"].str.contains("example")

domains = psdf["email"].str.split("@").str.get(-1)

name_lengths = psdf["name"].str.len()`,

  hints: [
    ".str accessor provides pandas string methods",
    "Common methods: lower(), upper(), strip()",
    "contains(pattern) checks if string contains a substring",
    "split(delimiter) splits strings into lists",
    "Use .get(index) to access split results",
    "len() returns string length"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [301, 304],
  nextKoans: [309],
};
