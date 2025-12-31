/**
 * Koan 204: Granting Permissions
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 204,
  title: "Granting Permissions",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Learn how to grant permissions on Unity Catalog objects. Replace ___ with the correct code.",

  setup: `
# Set up namespace and table
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_schema")
spark.sql("USE demo_catalog.demo_schema")

data = [("Alice", 1000), ("Bob", 2000)]
df = spark.createDataFrame(data, ["name", "salary"])
df.write.mode("overwrite").saveAsTable("employees")
`,

  template: `# Grant SELECT permission to a user
spark.sql("___ SELECT ON TABLE employees TO \`user@example.com\`")
print("✓ SELECT permission granted")

# Grant MODIFY permission (allows INSERT, UPDATE, DELETE)
spark.sql("GRANT ___ ON TABLE employees TO \`analyst_group\`")
print("✓ MODIFY permission granted")

# Grant multiple permissions at once
spark.sql("GRANT SELECT, ___ ON TABLE employees TO \`data_engineers\`")
print("✓ Multiple permissions granted")

# Grant permission on the entire schema
spark.sql("GRANT SELECT ON ___ demo_schema TO \`readonly_users\`")
print("✓ Schema-level permission granted")

print("\\n🎉 Koan complete! You've learned Unity Catalog permission management.")`,

  solution: `spark.sql("GRANT SELECT ON TABLE employees TO \`user@example.com\`")
spark.sql("GRANT MODIFY ON TABLE employees TO \`analyst_group\`")
spark.sql("GRANT SELECT, MODIFY ON TABLE employees TO \`data_engineers\`")
spark.sql("GRANT SELECT ON SCHEMA demo_schema TO \`readonly_users\`")`,

  hints: [
    "GRANT <privilege> ON <object_type> <object_name> TO <principal>",
    "Common privileges: SELECT, MODIFY, CREATE, USAGE",
    "Use backticks for email addresses and special characters",
    "Schema permissions apply to all tables within that schema",
    "Catalog permissions provide access to all schemas within"
  ],

  examCoverage: ["DEP"],
  prerequisiteKoans: [202],
  nextKoans: [205],
};
