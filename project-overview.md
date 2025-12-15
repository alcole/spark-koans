# Databricks Koans - Project Overview

Learn Databricks through test-driven exercises in the browser.

---

## Current State

### Implemented (Fully Mockable in Browser)

| Section | Koans | Status |
|---------|-------|--------|
| **PySpark Basics** | 7 | ✅ Complete |
| **Delta Lake** | 10 | ✅ Complete |

**Total: 17 koans**

### Files

```
pyspark-koans-prototype/
├── pyspark-delta-koans-complete.jsx   # Full React app with both sections
├── pyspark-koans-expanded.jsx         # PySpark only (30 koans)
├── pyspark-koans.jsx                  # Original prototype (7 koans)
├── delta_lake_shim.py                 # Standalone Delta mock (Python)
├── delta_lake_koans.js                # Delta koans as JS module
└── docs/
    ├── pyspark-koans.md               # PySpark koan reference
    ├── delta-lake-koans.md            # Delta Lake koan reference
    └── project-overview.md            # This file
```

---

## Koan Inventory

### PySpark Basics (IDs 1-7)

| ID | Title | Difficulty | Exams |
|----|-------|------------|-------|
| 1 | Creating a DataFrame | 🟢 Beginner | DEA, DAA, MLA |
| 2 | Selecting Columns | 🟢 Beginner | DEA, DAA, MLA |
| 3 | Filtering Rows | 🟢 Beginner | DEA, DAA, MLA |
| 4 | Adding Columns | 🟢 Beginner | DEA, DAA, MLA |
| 5 | Grouping and Aggregating | 🟡 Intermediate | DEA, DAA, MLA |
| 6 | Joining DataFrames | 🟡 Intermediate | DEA, DAA |
| 7 | Window Functions | 🟡 Intermediate | DEA, DEP, DAA |

### Delta Lake (IDs 101-110)

| ID | Title | Difficulty | Exams |
|----|-------|------------|-------|
| 101 | Creating a Delta Table | 🟢 Beginner | DEA, DAA |
| 102 | Time Travel - Version | 🟢 Beginner | DEA, DEP, DAA |
| 103 | MERGE - Upsert Pattern | 🟡 Intermediate | DEA, DEP |
| 104 | MERGE - Selective Update | 🟡 Intermediate | DEP |
| 105 | Table History | 🟢 Beginner | DEA, DEP, DAA |
| 106 | OPTIMIZE and Z-ORDER | 🟡 Intermediate | DEA, DEP |
| 107 | Delete with Condition | 🟢 Beginner | DEA, DEP |
| 108 | Update with Condition | 🟢 Beginner | DEA, DEP |
| 109 | Create Table with Builder | 🟡 Intermediate | DEP |
| 110 | VACUUM Old Files | 🟡 Intermediate | DEA, DEP |

---

## Planned Sections

### Fully Mockable (Browser Only)

| Section | Est. Koans | Priority | Notes |
|---------|------------|----------|-------|
| PySpark - String Functions | 8 | High | upper, lower, concat, substring, etc. |
| PySpark - Date/Time Functions | 8 | High | date_add, datediff, extraction |
| PySpark - Null Handling | 5 | High | isNull, fillna, coalesce |
| PySpark - Complex Types | 8 | Medium | arrays, maps, explode |
| Spark SQL - Basic Queries | 10 | High | SELECT, WHERE, GROUP BY |
| Spark SQL - Joins & CTEs | 8 | Medium | JOIN syntax, WITH clauses |
| MLflow - Experiment Tracking | 8 | Medium | log_param, log_metric |
| MLflow - Model Registry | 6 | Medium | register_model, stages |
| DABs - YAML Structure | 8 | Medium | bundle.yml validation |

### Conceptual/Simulated (Parked for Now)

| Section | Est. Koans | Notes |
|---------|------------|-------|
| Unity Catalog | 10 | Could use UC OSS for real interaction |
| Structured Streaming | 10 | Hard to mock properly |
| Delta Live Tables | 8 | Decorator patterns only |
| Auto Loader | 5 | Conceptual only |
| Spark ML | 15 | Some parts mockable |

---

## Architecture

### Current (Browser-Only)

```
┌─────────────────────────────────────────────────────────┐
│                     Browser                             │
├─────────────────────────────────────────────────────────┤
│  React UI                                               │
│  ├── Sidebar (categories, progress)                     │
│  ├── Code editor (textarea)                             │
│  └── Output panel                                       │
├─────────────────────────────────────────────────────────┤
│  Pyodide (Python in WebAssembly)                        │
│  ├── pandas (real library)                              │
│  ├── PySpark Shim (~400 lines)                          │
│  │   ├── SparkSession                                   │
│  │   ├── DataFrame                                      │
│  │   ├── Column expressions                             │
│  │   ├── Window functions                               │
│  │   └── Aggregations                                   │
│  └── Delta Lake Shim (~300 lines)                       │
│      ├── DeltaTable                                     │
│      ├── MergeBuilder                                   │
│      ├── Time travel (version snapshots)                │
│      └── History tracking                               │
└─────────────────────────────────────────────────────────┘
```

### Future (Optional Real Backend)

```
┌────────────────────────────────────────────────────────────────┐
│                     Browser                                    │
│  ├── Mode: [Simulated / Local UC / Hosted]                     │
│  └── Pyodide + Shims (or real API calls)                       │
└────────────────────────┬───────────────────────────────────────┘
                         │ REST API (when "Real" mode)
                         ▼
┌────────────────────────────────────────────────────────────────┐
│                Unity Catalog OSS (Optional)                    │
│  ├── Real catalog/schema/table metadata                        │
│  ├── Real GRANT/REVOKE                                         │
│  └── PostgreSQL backend                                        │
└────────────────────────────────────────────────────────────────┘
```

---

## Running Locally

```bash
# Create Vite project
npm create vite@latest databricks-koans -- --template react

# Enter directory
cd databricks-koans

# Install dependencies
npm install

# Copy pyspark-delta-koans-complete.jsx content to src/App.jsx

# Run dev server
npm run dev

# Open http://localhost:5173
```

---

## Exam Coverage Summary

| Exam | Code | Current Coverage | Target |
|------|------|------------------|--------|
| Data Engineer Associate | DEA | 14/17 koans | 40+ |
| Data Engineer Professional | DEP | 9/17 koans | 60+ |
| Data Analyst Associate | DAA | 10/17 koans | 30+ |
| ML Associate | MLA | 5/17 koans | 25+ |
| ML Professional | MLP | 0/17 koans | 20+ |

---

## Next Steps

1. **Expand PySpark** - Add string, date, null handling koans
2. **Add Spark SQL** - Build SQL parser/executor for DDL/DML
3. **Add MLflow** - Mock experiment tracking and model registry
4. **UI Improvements** - Exam filters, Monaco editor, persistence
5. **Deployment** - Set up on Vercel, custom domain

---

## References

- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Unity Catalog OSS](https://github.com/unitycatalog/unitycatalog)
- [Databricks Certifications](https://www.databricks.com/learn/certification)
