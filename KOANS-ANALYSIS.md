# Koans Analysis - Duplicates and Consolidation

## Current State

### File Inventory

| File | Koan Count | ID Range | Description |
|------|------------|----------|-------------|
| `pyspark-koans.jsx` | 7 | 1-7 | Original prototype with basic PySpark operations |
| `pyspark-koans-expanded.jsx` | 30 | 1-30 | Extended version with comprehensive PySpark coverage |
| `pyspark-delta-koans-complete.jsx` | 17 | 1-7, 101-110 | Combined: 7 PySpark + 10 Delta Lake |

## Duplicate Analysis

### Koans appearing in multiple files:

| ID | Title | Files |
|----|-------|-------|
| 1 | Creating a DataFrame | All 3 files |
| 2 | Selecting Columns | All 3 files |
| 3 | Filtering Rows | All 3 files |
| 4 | Adding Columns | `pyspark-koans.jsx`, `pyspark-delta-koans-complete.jsx` |
| 5 | Grouping and Aggregating | `pyspark-koans.jsx`, `pyspark-delta-koans-complete.jsx` |
| 6 | Joining DataFrames | `pyspark-koans.jsx`, `pyspark-delta-koans-complete.jsx` |
| 7 | Window Functions | `pyspark-koans.jsx`, `pyspark-delta-koans-complete.jsx` |

**Note**: The first 7 koans from `pyspark-koans.jsx` are identical to those in `pyspark-delta-koans-complete.jsx`.

The expanded version reorganizes content:
- Keeps koans 1-3 the same
- Inserts new koans 4-7 (Sorting, Limiting, Dropping, Distinct)
- Moves "Adding Columns" to ID 8
- Adds extensive new content (string functions, joins, window functions, null handling, etc.)

## Unique Koans Breakdown

### PySpark Koans (30 unique from expanded version)

#### Basics (IDs 1-10)
1. Creating a DataFrame
2. Selecting Columns
3. Filtering Rows
4. Sorting Data (new)
5. Limiting Results (new)
6. Dropping Columns (new)
7. Distinct Values (new)
8. Adding Columns
9. Renaming Columns (new)
10. Literal Values (new)

#### Transformations (IDs 11-16)
11. Conditional Logic with when/otherwise (new)
12. Type Casting (new)
13. String Functions - Case (new)
14. String Functions - Concatenation (new)
15. String Functions - Substring and Length (new)
16. String Functions - Trim and Pad (new)

#### Aggregations (IDs 17-19)
17. Grouping and Aggregating
18. Multiple Aggregations (new)
19. Aggregate Without Grouping (new)

#### Joins (IDs 20-22)
20. Inner Join (new)
21. Left Outer Join (new)
22. Join on Multiple Columns (new)

#### Window Functions (IDs 23-25)
23. Window Functions - Running Total
24. Window Functions - Row Number (new)
25. Window Functions - Lag and Lead (new)

#### Data Handling (IDs 26-30)
26. Handling Nulls - Detection (new)
27. Handling Nulls - Fill and Drop (new)
28. Union DataFrames (new)
29. Explode Arrays (new)
30. Pivot Tables (new)

### Delta Lake Koans (10 unique from complete version)

IDs 101-110:
101. Creating a Delta Table
102. Time Travel - Version
103. MERGE - Upsert Pattern
104. MERGE - Selective Update
105. Table History
106. OPTIMIZE and Z-ORDER
107. Delete with Condition
108. Update with Condition
109. Create Table with Builder
110. VACUUM Old Files

## Consolidation Strategy

### Recommended: Single Consolidated File

**File**: `all-koans-complete.jsx`
**Total**: 40 unique koans (30 PySpark + 10 Delta Lake)

**Structure**:
- IDs 1-30: All PySpark koans from `pyspark-koans-expanded.jsx`
- IDs 101-110: All Delta Lake koans from `pyspark-delta-koans-complete.jsx`
- Categories:
  - "Basics" (IDs 1-10)
  - "Transformations" (IDs 11-16)
  - "Aggregations" (IDs 17-19)
  - "Joins" (IDs 20-22)
  - "Window Functions" (IDs 23-25)
  - "Data Handling" (IDs 26-30)
  - "Delta Lake" (IDs 101-110)

### Benefits
1. **No duplicates** - Each koan appears exactly once
2. **Comprehensive** - All content from all three files
3. **Organized** - Clear progression from basic to advanced
4. **Complete** - Both PySpark and Delta Lake coverage

## File Recommendations

### Keep
- **`all-koans-complete.jsx`** (to be created) - Single source of truth

### Archive/Reference
- `pyspark-koans.jsx` - Original prototype (historical reference)
- `pyspark-koans-expanded.jsx` - Contains all PySpark koans
- `pyspark-delta-koans-complete.jsx` - Contains Delta Lake koans

### Documentation
- `pyspark-koans.md` - Reference docs for koans 1-7
- `delta-lake-koans.md` - Reference docs for koans 101-110
- `project-overview.md` - Project roadmap

## Next Steps

1. ✅ Create `all-koans-complete.jsx` with all 40 unique koans
2. Test consolidated file to ensure no breaking changes
3. Update README.md to reference new consolidated file
4. Consider moving old files to `/archive/` directory
