# PySpark Koans - Browser Edition

A test-driven learning environment for PySpark that runs entirely in the browser using Pyodide (Python in WebAssembly) with a pandas-backed PySpark shim.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Browser                                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────────────────────┐   │
│  │   React App      │    │         Pyodide Runtime          │   │
│  │                  │    │  ┌────────────────────────────┐  │   │
│  │  - Monaco Editor │───▶│  │    PySpark Shim Layer      │  │   │
│  │  - Koan State    │    │  │                            │  │   │
│  │  - Progress      │◀───│  │  SparkSession (mock)       │  │   │
│  │                  │    │  │  DataFrame → pandas        │  │   │
│  │                  │    │  │  Column expressions        │  │   │
│  │                  │    │  │  GroupedData → groupby     │  │   │
│  │                  │    │  │  F.* functions             │  │   │
│  └──────────────────┘    │  └────────────────────────────┘  │   │
│                          │               │                   │   │
│                          │               ▼                   │   │
│                          │  ┌────────────────────────────┐  │   │
│                          │  │         pandas             │  │   │
│                          │  │   (actual computation)     │  │   │
│                          │  └────────────────────────────┘  │   │
│                          └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## How It Works

### 1. PySpark Shim Layer

The shim provides PySpark-compatible APIs backed by pandas:

| PySpark API | Shim Implementation |
|-------------|---------------------|
| `spark.createDataFrame()` | Creates pandas DataFrame wrapped in custom class |
| `df.select()` | pandas column selection |
| `df.filter()` | pandas boolean indexing |
| `df.groupBy().agg()` | pandas groupby + aggregation |
| `df.join()` | pandas merge |
| `col("name")` | Column expression builder |
| `F.sum()`, `F.avg()` etc. | Deferred aggregation expressions |

### 2. Koan Structure

Each koan follows this pattern:

```python
# Setup - creates test data
data = [...]
df = spark.createDataFrame(data, columns)

# Exercise - learner fills in ___
result_df = df.___(...)

# Verification - assertions that must pass
assert result_df.count() == expected
print("✓ Koan complete!")
```

### 3. Execution Flow

1. User edits code in the editor
2. Click "Run" sends code to Pyodide
3. Pyodide executes Python with shim loaded
4. Assertions pass → koan marked complete
5. Progress persists in component state

## Supported PySpark Features

### DataFrame Operations
- [x] `select()`, `selectExpr()`
- [x] `filter()`, `where()`
- [x] `withColumn()`, `withColumnRenamed()`
- [x] `drop()`
- [x] `orderBy()`, `sort()`
- [x] `limit()`
- [x] `distinct()`, `dropDuplicates()`
- [x] `union()`, `unionAll()`
- [x] `join()` (inner, left, right, outer)

### Aggregations
- [x] `groupBy().agg()`
- [x] `F.sum()`, `F.avg()`, `F.count()`
- [x] `F.min()`, `F.max()`
- [x] `F.first()`, `F.last()`
- [x] `F.countDistinct()`

### Column Operations
- [x] Arithmetic: `+`, `-`, `*`, `/`
- [x] Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
- [x] Logical: `&`, `|`
- [x] `alias()`, `cast()`
- [x] `isNull()`, `isNotNull()`
- [x] `isin()`, `contains()`
- [x] String: `upper()`, `lower()`, `trim()`

### Data Handling
- [x] `collect()`, `take()`, `first()`
- [x] `count()`
- [x] `show()`, `printSchema()`
- [x] `fillna()`, `dropna()`
- [x] `toPandas()`

### Not Implemented (would need real Spark)
- [ ] Window functions
- [ ] UDFs
- [ ] Spark SQL
- [ ] Partitioning operations
- [ ] Caching (mocked as no-op)

## Project Structure

```
pyspark-koans/
├── app.jsx              # Main React component (this prototype)
├── README.md            # This file
└── future/
    ├── koans/           # Koan definitions (separate files)
    │   ├── 01-basics/
    │   ├── 02-transformations/
    │   ├── 03-aggregations/
    │   └── ...
    ├── shim/            # PySpark shim modules
    │   ├── dataframe.py
    │   ├── column.py
    │   ├── functions.py
    │   └── types.py
    └── components/      # React components
        ├── Editor.jsx
        ├── KoanList.jsx
        └── OutputPanel.jsx
```

## Deployment (Vercel)

### As a Next.js App

```bash
npx create-next-app@latest pyspark-koans --typescript
cd pyspark-koans

# Copy app.jsx to pages/index.tsx (with types)
# Add to next.config.js:
module.exports = {
  webpack: (config) => {
    config.resolve.fallback = { fs: false };
    return config;
  }
}
```

### Static Export

The app is entirely client-side, so it can be exported as static files:

```bash
next build && next export
```

## Extending

### Adding New Koans

```javascript
const newKoan = {
  id: 13,
  title: "Window Functions",
  category: "advanced",
  difficulty: "advanced",
  description: "Learn to use window functions for running totals.",
  hint: "Use Window.partitionBy() and F.sum().over()",
  starterCode: `...`,
  expectedOutput: "✓ Koan 13 complete!"
};

KOANS.push(newKoan);
```

### Adding Shim Features

To support a new PySpark feature:

1. Add method to appropriate class in PYSPARK_SHIM
2. Map to equivalent pandas operation
3. Add test koan to verify it works
4. Document in supported features

## Limitations

1. **Not real Spark** - The shim approximates behavior on small data
2. **No distributed semantics** - No partitions, shuffles, stages
3. **Limited SQL** - No `spark.sql()` support yet
4. **Memory bound** - Data must fit in browser memory
5. **No file I/O** - Can't read parquet/csv from URLs (yet)

## Future Enhancements

- [ ] Monaco Editor with PySpark syntax highlighting
- [ ] Inline documentation/tooltips
- [ ] Progress persistence (localStorage)
- [ ] Shareable koan links
- [ ] User-submitted koans
- [ ] Delta Lake koans track
- [ ] DABs koans track (YAML-based)

## License

MIT
# spark-koans
