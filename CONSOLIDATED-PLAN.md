# Consolidated Koans File Plan

## Summary

I've analyzed all three koan files and identified:
- **7 basic PySpark koans** in `pyspark-koans.jsx`
- **30 comprehensive PySpark koans** in `pyspark-koans-expanded.jsx` (includes new koans for sorting, string functions, joins, window functions, null handling, etc.)
- **17 koans** in `pyspark-delta-koans-complete.jsx` (7 PySpark + 10 Delta Lake)

## Duplicates Found

The first 7 koans (Creating DataFrame, Selecting, Filtering, Adding Columns, Grouping, Joining, Window Functions) appear in multiple files with slight variations.

## Recommended Consolidation

**File**: `all-koans-consolidated.jsx` (~3500 lines)

### Contents:
- **30 PySpark Koans** (IDs 1-30) from `pyspark-koans-expanded.jsx`
  - Basics (1-7): Create, Select, Filter, Sort, Limit, Drop, Distinct
  - Column Operations (8-12): Add, Rename, Literals, Conditionals, Casting
  - String Functions (13-16): Case, Concat, Substring, Trim
  - Aggregations (17-19): GroupBy, Multiple Aggs, Global Aggs
  - Joins (20-22): Inner, Left Outer, Multi-column
  - Window Functions (23-25): Running Total, Row Number, Lag/Lead
  - Null Handling (26-27): Detection, Fill/Drop
  - Advanced (28-30): Union, Explode, Pivot

- **10 Delta Lake Koans** (IDs 101-110) from `pyspark-delta-koans-complete.jsx`
  - Creating Delta Tables
  - Time Travel
  - MERGE (Upsert & Selective)
  - History
  - OPTIMIZE & Z-ORDER
  - DELETE
  - UPDATE
  - Builder API
  - VACUUM

- **Complete Shim** from `pyspark-delta-koans-complete.jsx`
  - Full PySpark support
  - Complete Delta Lake implementation
  - ~1300 lines of Python shim code

- **Enhanced UI** from `pyspark-delta-koans-complete.jsx`
  - Sidebar with category filtering
  - Progress tracking
  - Better navigation

## Total Stats

- **40 unique koans**
- **7 categories**: Basics, Column Operations, String Functions, Aggregations, Joins, Window Functions, Null Handling, Advanced, Delta Lake
- **File size**: ~3,500 lines
- **No duplicates**

## Would you like me to create this file?

Options:
1. **Yes, create the full consolidated file** - I'll create `all-koans-consolidated.jsx` with all 40 koans
2. **Create a smaller version first** - Start with just the 30 PySpark koans, test it, then add Delta Lake
3. **Keep separate files** - Maintain `pyspark-koans-expanded.jsx` (30) and `delta-koans.jsx` (10) as separate files

Let me know which you prefer!
