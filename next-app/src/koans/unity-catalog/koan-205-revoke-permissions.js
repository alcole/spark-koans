/**
 * Koan 205: Revoking Permissions
 * Category: Unity Catalog
 * Difficulty: Intermediate
 */

export default {
  id: 205,
  title: "Revoking Permissions",
  category: "Unity Catalog",
  difficulty: "intermediate",
  description: "Learn how to revoke permissions on Unity Catalog objects. Replace ___ with the correct code.",

  setup: `
# Set up namespace and table
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_schema")
spark.sql("USE demo_catalog.demo_schema")

data = [("Customer A", "Premium"), ("Customer B", "Basic")]
df = spark.createDataFrame(data, ["customer", "tier"])
df.write.mode("overwrite").saveAsTable("customers")

# Grant some permissions first
spark.sql("GRANT SELECT ON TABLE customers TO \`temp_user\`")
spark.sql("GRANT MODIFY ON TABLE customers TO \`temp_user\`")
`,

  template: `# Revoke SELECT permission from a user
spark.sql("___ SELECT ON TABLE customers FROM \`temp_user\`")
print("✓ SELECT permission revoked")

# Revoke MODIFY permission
spark.sql("REVOKE ___ ON TABLE customers FROM \`temp_user\`")
print("✓ MODIFY permission revoked")

# Grant and then revoke multiple permissions
spark.sql("GRANT SELECT, MODIFY ON TABLE customers TO \`test_group\`")
spark.sql("___ SELECT, MODIFY ON TABLE customers ___ \`test_group\`")
print("✓ Multiple permissions revoked")

print("\\n🎉 Koan complete! You can now manage permission lifecycles.")`,

  solution: `spark.sql("REVOKE SELECT ON TABLE customers FROM \`temp_user\`")
spark.sql("REVOKE MODIFY ON TABLE customers FROM \`temp_user\`")
spark.sql("REVOKE SELECT, MODIFY ON TABLE customers FROM \`test_group\`")`,

  hints: [
    "REVOKE syntax mirrors GRANT: REVOKE <privilege> ON <object> FROM <principal>",
    "You can revoke multiple privileges in one statement",
    "Use backticks for principals with special characters",
    "Revoking non-existent permissions is typically a no-op"
  ],

  examCoverage: ["DEP"],
  prerequisiteKoans: [204],
  nextKoans: [206],
};
