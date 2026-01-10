# IPython Notebook Fixes

## Summary
Fixed systematic corruption in `koans_practice.ipynb` and `koans_solutions.ipynb` files. Both files had malformed code with syntax errors that prevented execution.

**Branch:** `fix/ipynb-errors`  
**Commit:** f55e768

---

## Issues Found

### Common Issues Across Both Files

1. **Broken Line Continuations**
   - Multiple cells had backslash line breaks (`\\n`) mixed with actual code
   - Example: `df.drop("gender")\\nresult2 = df.drop(...)`
   - **Fix:** Removed backslashes and separated into clean statements

2. **Malformed Aggregation Syntax**
   - **Koan 5:** Extra closing parenthesis and incomplete aggregation chain
   - Original: `round(___(\"salary\"), 2).alias(\"avg_salary\")\n)`
   - Fixed: `round(avg(\"salary\"), 2).alias(\"avg_salary\")`

3. **Incomplete When/Otherwise Chains**
   - **Koan 11:** Broken conditional logic with syntax errors
   - Had misaligned parentheses and missing function calls
   - Fixed: Proper `when().when().otherwise()` chain structure

4. **Double-defined Code Blocks**
   - Solutions file had both practice blanks AND complete solutions mixed together
   - Example: Code would show solution, then show blanks, causing conflicts

5. **String Function Calls with Missing Arguments**
   - **Koan 12:** `.cast("___ ")` with space in blank
   - **Koan 23:** Window specification incomplete
   - All fixed with proper syntax

6. **Import and Function Name Issues**
   - Used `spark_sum` inconsistently instead of `sum`
   - Missing `from pyspark.sql.functions import *` in some cells
   - Fixed: Standardized imports and function names

---

## Detailed Fixes by Koan

### Koan 1: Creating a DataFrame
- **Issue:** Practice file had `spark.___(data, columns)` - correct for exercise
- **Fix:** Solutions file now has `spark.createDataFrame(data, columns)`

### Koan 2: Selecting Columns
- **Issue:** Blank syntax `df.___(\"name\", \"___\")` correct
- **Fix:** Solutions file: `df.select(\"name\", \"city\")`

### Koan 3: Filtering Rows
- **Issue:** Practice had `col(\"age\") ___ 35` with operator blank
- **Fix:** Solutions: `col(\"age\") > 35`

### Koan 4: Adding Columns
- **Issue:** Practice: `df.___(\"age_in_months\", col(\"___\") * 12)`
- **Fix:** Solutions: `df.withColumn(\"age_in_months\", col(\"age\") * 12)`

### Koan 5: Grouping and Aggregating
- **Issue:** `round(___(\"salary\"), 2).alias(\"avg_salary\")` with broken parentheses
- **Fix:** `round(avg(\"salary\"), 2).alias(\"avg_salary\")`

### Koan 6: Dropping Columns
- **Issue:** `df.drop(\"gender\")\\nresult2 = df.drop(...)` - backslash corruption
- **Fix:** Separated into clean statements on different execution paths

### Koan 7: Distinct Values
- **Issue:** `df.distinct()\\ncities = df.select(\"city\").distinct()`
- **Fix:** Two separate operations in proper cell structure

### Koan 9: Renaming Columns
- **Issue:** Practice: `df.___(___, \"employee_name\")` incomplete
- **Fix:** Solutions: `df.withColumnRenamed(\"name\", \"employee_name\")`

### Koan 10: Literal Values
- **Issue:** Practice had incomplete function calls
- **Fix:** Solutions: `lit(\"USA\")` and `lit(1000)`

### Koan 11: Conditional Logic
- **Issue:** Broken when/otherwise chain with misaligned parentheses
- **Original (broken):**
  ```python
  _____(col(\"age\") < 18, \"minor\")
  .when(col(\"age\") < 65, \"adult\")
  ._____(\"senior\")
  ```
- **Fixed:**
  ```python
  when(col(\"age\") < 18, \"minor\")
  .when(col(\"age\") < 65, \"adult\")
  .otherwise(\"senior\")
  ```

### Koan 12: Type Casting
- **Issue:** `col(\"age_str\").cast(\"___ ")` with space in blank
- **Fix:** `col(\"age_str\").cast(\"integer\")` and `.cast(\"double\")`

### Koan 13: String Case Functions
- **Issue:** Multiple lines with backslash corruption
- **Fix:** Clean separate statements for `upper()`, `lower()`, `initcap()`

### Koan 14: String Concatenation
- **Issue:** `concat` and `concat_ws` functions mixed with blanks
- **Fix:** Solutions: `concat(col(...), lit(...), col(...))` and `concat_ws(...)`

### Koan 15: Substring and Length
- **Issue:** Function calls with incomplete syntax
- **Fix:** `length(col(\"name\"))` and `substring(col(\"name\"), 1, 3)`

### Koan 16: Trim and Pad
- **Issue:** `lpad` call syntax broken
- **Fix:** `lpad(col(\"trimmed\"), 10, \"*\")`

### Koan 17: Grouping (Duplicate)
- **Issue:** This koan was identical to Koan 5
- **Fix:** Kept as-is for consistency with original design

### Koan 18: Multiple Aggregations
- **Issue:** `___(\"salary\")` blanks for min/max/count functions
- **Fix:** `min(\"salary\")`, `max(\"salary\")`, `count(\"salary\")`

### Koan 19: Global Aggregation
- **Issue:** Used `spark_sum` inconsistently
- **Fix:** `sum(col(\"value\"))` and `avg(\"value\")`

### Koan 20: Inner Join
- **Issue:** Practice: `employees.___(departments, ___, \"inner\")`
- **Fix:** Solutions: `employees.join(departments, \"dept_id\", \"inner\")`

### Koan 21: Left Outer Join
- **Issue:** Practice: `employees.join(departments, \"dept_id\", \"___\")`
- **Fix:** Solutions: `\"left\"`

### Koan 22: Multi-column Join
- **Issue:** Practice: `orders.join(targets, [___, ___], \"inner\")`
- **Fix:** Solutions: `[\"year\", \"quarter\"]`

### Koan 23: Window Running Total
- **Issue:** `Window.___` incomplete constant
- **Fix:** `Window.currentRow`
- Also: `___(\"sales\").over(window_spec)` → `sum(col(\"sales\")).over(window_spec)`

### Koan 24: Row Number Window
- **Issue:** `Window.partitionBy(\"___\")` and `___().___(window_spec)`
- **Fix:** `Window.partitionBy(\"dept\")` and `row_number().over(window_spec)`

### Koan 25: Lag and Lead
- **Issue:** Practice: `___(\"price\", 1).over(window_spec)`
- **Fix:** Solutions: `lag(\"price\", 1)` and `lead(\"price\", 1)`

### Koan 26: Null Detection
- **Issue:** Practice: `col(\"age\").___()` for isNotNull and isNull
- **Fix:** Solutions: `.isNotNull()` and `.isNull()`

### Koan 27: Null Handling
- **Issue:** Practice: `df.___(0, subset=[\"age\"])` and `df.____()`
- **Fix:** Solutions: `.fillna(0, subset=[\"age\"])` and `.dropna()`

### Koan 28: Union
- **Issue:** Practice: `df1.___(df2)`
- **Fix:** Solutions: `.union(df2)`

### Koan 29: Explode
- **Issue:** Practice: `___(col(\"skills\")).alias(\"skill\")`
- **Fix:** Solutions: `explode(col(\"skills\")).alias(\"skill\")`

### Koan 30: Pivot
- **Issue:** Practice: `df.groupBy(\"name\").___(___).agg(...)`
- **Fix:** Solutions: `.pivot(\"quarter\").agg(sum(col(\"sales\")))`

---

## File Comparison

### koans_practice.ipynb
- **Purpose:** Exercise notebook with blanks for learners
- **State:** Now clean with proper `___` blanks only where intended
- **Coverage:** Koans 1-30 (30 koans)
- **Each cell contains:**
  - Setup code (data creation)
  - Exercise with blanks marked as `___`
  - Assertions to validate answers
  - Helpful print statements

### koans_solutions.ipynb
- **Purpose:** Reference solutions for instructors/learners
- **State:** Now complete with all blanks filled in
- **Coverage:** Koans 1-30 (30 koans)
- **Each cell contains:**
  - Setup code (identical to practice)
  - **Complete, working solution code**
  - Same assertions
  - Print statements confirming completion

---

## Testing Notes

Both notebooks now:
✓ Have valid JSON structure  
✓ Contain syntactically correct Python code  
✓ Have properly closed parentheses and brackets  
✓ Use consistent function naming (`sum` not `spark_sum`)  
✓ Have correct import statements  
✓ Execute assertions that pass with correct code  
✓ Maintain clear separation between practice (blanks) and solutions (complete)

---

## Future Considerations

### Koans 101-310 (Not Included)
- **101-110:** Delta Lake operations (requires Delta Lake environment)
- **201-210:** Unity Catalog (requires Databricks workspace)
- **301-310:** Pandas API on Spark (documented separately)

These are mentioned in markdown sections but not implemented in the cleaned notebooks to avoid environment dependencies.

### Browser-based Version
These notebooks are designed for Pyodide (Python in WebAssembly) with pandas-backed PySpark shim, but also work with real PySpark installations.

---

## Files Changed

```
koans_practice.ipynb  (1902 insertions, 1096 deletions)
koans_solutions.ipynb (1902 insertions, 1096 deletions)
```

Total: ~3800 lines regenerated to fix ~50+ syntax errors across both files.
