# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **PySpark Koans**, a browser-based, test-driven learning environment for PySpark and Delta Lake. The project runs entirely in the browser using Pyodide (Python in WebAssembly) with a pandas-backed PySpark shim that emulates Spark APIs without requiring a real Spark cluster.

## Architecture

### Core Components

1. **PySpark Shim (~665 lines in app.jsx)**: A complete mock implementation of PySpark APIs backed by pandas
   - SparkSession and DataFrame classes
   - Column expressions and operators
   - Functions module (F.*) for aggregations
   - GroupedData for groupBy operations
   - Row objects for data access

2. **React Application (app.jsx)**: Interactive learning interface
   - Monaco-style code editor (textarea)
   - Koan progression system with sidebar navigation
   - Real-time Python execution via Pyodide
   - Output panel with success/error states

3. **Koan System**: Test-driven exercises with three components:
   - Starter code with blanks (___) to fill
   - Assertions that verify correctness
   - Success messages when complete

### Data Flow

```
User edits code → Click Run → Pyodide executes Python →
Shim intercepts PySpark calls → pandas performs computation →
Assertions verify correctness → Success/error feedback
```

## File Structure

- `app.jsx` / `App.jsx` - Main React component with embedded PySpark shim
- `pyspark-koans.jsx` - Original 7-koan prototype
- `pyspark-koans-expanded.jsx` - Expanded 30-koan version (not yet complete)
- `pyspark-delta-koans-complete.jsx` - Combined PySpark + Delta Lake version
- `pyspark-koans.md` - Reference documentation for PySpark koans (7 exercises)
- `delta-lake-koans.md` - Reference documentation for Delta Lake koans (10 exercises)
- `project-overview.md` - High-level project roadmap and planned features
- `README.md` - Architecture overview and deployment instructions

## Running the Application

Since this is a prototype without a build system currently:

1. **Local Development (with Vite)**:
   ```bash
   # Create new Vite project
   npm create vite@latest pyspark-koans -- --template react
   cd pyspark-koans
   npm install

   # Copy one of the .jsx files to src/App.jsx
   # Then run:
   npm run dev
   ```

2. **Direct HTML approach**:
   - Wrap the React component in an HTML file
   - Load React, ReactDOM, and Babel standalone from CDN
   - Load Pyodide from CDN (https://cdn.jsdelivr.net/pyodide/v0.24.1/full/)

## Shim Architecture

The PySpark shim translates Spark operations to pandas equivalents:

| PySpark Operation | Shim Implementation |
|-------------------|---------------------|
| `spark.createDataFrame()` | Wraps pandas DataFrame in custom DataFrame class |
| `df.select()` | pandas column selection with expression evaluation |
| `df.filter()` | pandas boolean indexing via `_evaluate_condition()` |
| `df.withColumn()` | pandas column assignment with `_evaluate_expr()` |
| `df.groupBy().agg()` | pandas groupby with aggregate mapping |
| `df.join()` | pandas merge |
| `col("x") > 5` | Column class builds expression tuples for lazy evaluation |

### Expression Evaluation

The shim uses tuple-based expression trees:
- `col("age") > 30` → `('gt', 'age', 30)`
- `F.sum("salary")` → `('sum', 'salary')`
- Expressions evaluated lazily in `_evaluate_expr()` and `_evaluate_condition()`

## Koan Categories

- **Basics**: Creating DataFrames, selecting, filtering
- **Transformations**: withColumn, sorting, renaming, distinct
- **Aggregations**: groupBy, multiple aggregations
- **Joins**: Inner joins, multiple DataFrames
- **Data Cleaning**: fillna, dropna, null handling
- **Advanced**: Chaining transformations, window functions

## Delta Lake Implementation

The Delta Lake shim (in `pyspark-delta-koans-complete.jsx`) adds:
- DeltaTable class with MERGE, UPDATE, DELETE operations
- Time travel via version snapshots stored in memory
- History tracking for all operations
- OPTIMIZE and VACUUM simulation

## Development Guidelines

### Adding New Koans

1. Add koan object to `KOANS` array with:
   - `id`, `title`, `category`, `difficulty`
   - `description`, `hint`
   - `starterCode` (with `___` blanks)
   - `solution` (reference only)
   - `expectedOutput` (assertion message)

2. Ensure starter code includes:
   - Setup (data creation)
   - Exercise (with blanks)
   - Assertions with clear error messages
   - Success message starting with "✓"

### Extending the Shim

To add new PySpark features:

1. **DataFrame method**: Add to `DataFrame` class, map to pandas equivalent
2. **Column operation**: Add to `Column` class, return new Column with expression tuple
3. **Function**: Add to `functions` class, return Column with operation tuple
4. **Evaluation**: Update `_evaluate_expr()` or `_evaluate_condition()` to handle new tuple operation

Example - adding `substring`:
```javascript
// In functions class
static substring(column, pos, len) {
  return Column("substring", ('substring', column.name, pos, len))
}

// In _evaluate_expr()
else if (op == 'substring') {
  return this._pdf[expr[1]].str.slice(expr[2], expr[2] + expr[3])
}
```

## Limitations

1. **Not real Spark** - Single-threaded, in-memory, no distributed semantics
2. **No partitioning** - Operations like `repartition()` are no-ops
3. **Limited window functions** - Complex window specs may not work
4. **No UDFs** - Cannot register custom Python functions
5. **No Spark SQL** - No `spark.sql()` support (yet)
6. **Memory bound** - All data must fit in browser memory

## Exam Coverage

Koans are tagged for Databricks certification exams:
- **DEA** - Data Engineer Associate
- **DEP** - Data Engineer Professional
- **DAA** - Data Analyst Associate
- **MLA** - Machine Learning Associate

## Future Enhancements

- Monaco Editor integration (currently using textarea)
- Progress persistence (localStorage)
- Spark SQL parser/executor for DDL/DML
- MLflow mock for experiment tracking
- Unity Catalog OSS integration option
- User-submitted koans
