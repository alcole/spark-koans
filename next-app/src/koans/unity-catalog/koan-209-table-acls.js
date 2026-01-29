/**
 * Koan 209: Inspecting Table ACLs
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 209,
  title: "Inspecting Table ACLs",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Learn how to inspect Access Control Lists (ACLs) on Unity Catalog objects. Replace ___ with the correct code.",

  setup: `
# Set up namespace and table
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.acl_demo")
spark.sql("USE demo_catalog.acl_demo")

data = [("record1", 100), ("record2", 200)]
df = spark.createDataFrame(data, ["id", "value"])
df.write.mode("overwrite").saveAsTable("secure_data")

# Grant some permissions for testing
spark.sql("GRANT SELECT ON TABLE secure_data TO \`analyst@example.com\`")
spark.sql("GRANT MODIFY ON TABLE secure_data TO \`engineer@example.com\`")
`,

  template: `# Show grants on the table
grants_df = spark.sql("___ GRANTS ON TABLE secure_data")
grants_df.show()
print("✓ Grants displayed")

grant_count = grants_df.count()
assert grant_count >= 2, f"Expected at least 2 grants, got {grant_count}"
print(f"✓ Found {grant_count} grant entries")

# Check grants for a specific principal
user_grants = spark.sql("""
  SHOW ___ ON TABLE secure_data
""").filter("principal = 'analyst@example.com'")
print("✓ Filtered grants for specific user")

# Show all grants on the schema
schema_grants = spark.sql("SHOW GRANTS ON ___ acl_demo")
print("✓ Schema grants displayed")

print("\\n🎉 Koan complete! You can now audit Unity Catalog permissions.")`,

  solution: `grants_df = spark.sql("SHOW GRANTS ON TABLE secure_data")

user_grants = spark.sql("""
  SHOW GRANTS ON TABLE secure_data
""").filter("principal = 'analyst@example.com'")

schema_grants = spark.sql("SHOW GRANTS ON SCHEMA acl_demo")`,

  hints: [
    "SHOW GRANTS ON TABLE displays all permissions on a table",
    "SHOW GRANTS ON SCHEMA shows schema-level permissions",
    "The result includes principal, privilege, and grant option",
    "Filter the results DataFrame to find specific principals",
    "Use this for security auditing and compliance"
  ],

  examCoverage: ["DEP"],
  prerequisiteKoans: [204],
  nextKoans: [210],
};
