# PySpark Koans - Modular Next.js Architecture

Interactive, browser-based learning platform for PySpark and Delta Lake. Built with Next.js for scalability to 100+ koans.

## 🏗️ Architecture Overview

### Key Design Decisions

- **Next.js** for file-based routing and automatic code splitting
- **One file per koan** for easy maintenance and scalability
- **Modular Python shims** separated by feature domain
- **Reusable React components** for consistent UX
- **Static export** for deployment anywhere

## 📁 Project Structure

```
next-app/
├── pages/
│   ├── index.js                    # Landing page / Dashboard
│   ├── koans/
│   │   └── [id].js                 # Individual koan page (dynamic route)
│   └── _app.js                     # App wrapper
│
├── src/
│   ├── koans/                      # Koan definitions
│   │   ├── pyspark/
│   │   │   ├── basics/
│   │   │   │   ├── koan-001-create-dataframe.js
│   │   │   │   ├── koan-002-select-columns.js
│   │   │   │   └── ...
│   │   │   ├── strings/
│   │   │   ├── aggregations/
│   │   │   └── ...
│   │   ├── delta/
│   │   │   ├── koan-101-create-table.js
│   │   │   └── ...
│   │   └── index.js                # Koan registry
│   │
│   ├── shims/                      # Python shim modules
│   │   ├── pyspark/
│   │   │   ├── core.py            # Row, Column, DataFrame, SparkSession
│   │   │   ├── functions.py       # SQL functions
│   │   │   ├── window.py          # Window functions
│   │   │   └── io.py              # DataFrameReader/Writer
│   │   ├── delta/
│   │   │   ├── core.py            # DeltaTable
│   │   │   └── storage.py         # In-memory Delta storage
│   │   └── index.py               # Main shim entry point
│   │
│   ├── components/                 # React components
│   │   ├── KoanEditor.jsx         # Code editor
│   │   ├── OutputPanel.jsx        # Output display
│   │   ├── Sidebar.jsx            # Navigation
│   │   ├── KoanHeader.jsx         # Koan title/description
│   │   ├── HintPanel.jsx          # Hints
│   │   └── Controls.jsx           # Action buttons
│   │
│   └── hooks/                      # Custom React hooks
│       ├── usePyodide.js          # Pyodide initialization
│       ├── useKoanProgress.js     # Progress tracking
│       └── useKoanExecution.js    # Code execution
│
├── public/
│   └── shims/                      # Compiled Python shims
│       ├── pyspark-shim.py
│       └── delta-shim.py
│
├── package.json
├── next.config.js                  # Next.js configuration
└── tailwind.config.js              # Tailwind CSS config
```

## 🚀 Getting Started

### Installation

```bash
cd next-app
npm install
```

### Development

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000)

### Build for Production

```bash
npm run build     # Build Next.js app
npm run export    # Export as static site
```

Output will be in `out/` directory - deploy to any static host.

## 📝 Adding New Koans

### 1. Create Koan File

```javascript
// src/koans/pyspark/strings/koan-013-case.js
export default {
  id: 13,
  title: "String Functions - Case",
  category: "String Functions",
  difficulty: "beginner",
  description: "Learn string case transformations",

  setup: `
data = [("alice",), ("BOB",)]
df = spark.createDataFrame(data, ["name"])
`,

  template: `# Convert to uppercase
result = df.withColumn("upper_name", ___(col("name")))

assert result.collect()[0]["upper_name"] == "ALICE"
print("✓ Converted to uppercase")
print("\\n🎉 Koan complete!")`,

  solution: `result = df.withColumn("upper_name", upper(col("name")))`,

  hints: [
    "Use the upper() function",
    "Import it from pyspark.sql.functions"
  ],

  examCoverage: ["DEA", "DAA"],
  prerequisiteKoans: [1, 2],
  nextKoans: [14],
};
```

### 2. Register in Index

```javascript
// src/koans/index.js
import koan13 from './pyspark/strings/koan-013-case';

const koansById = {
  // ... existing koans
  13: koan13,
  // ... more koans
};
```

### 3. That's It!

Next.js automatically:
- Creates route `/koans/13`
- Pre-renders page at build time
- Code-splits the koan bundle

## 🔧 Extending the Shim

### Adding a New PySpark Function

```python
# src/shims/pyspark/functions.py

def my_new_function(col_expr, param):
    """New function description"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        # Implement using pandas
        return pdf[col_expr.name].apply(lambda x: ...)

    new_col = Column(col_expr.name)
    new_col._transform_func = transform
    return new_col
```

### Adding Delta Lake Features

```python
# src/shims/delta/core.py

class DeltaTable:
    def my_new_operation(self, condition):
        """New Delta operation"""
        df = self._table_data.get_df()
        # Implement operation
        self._table_data._add_version(df, "MY_OPERATION")
```

## 📦 Deployment

### GitHub Pages

```bash
# next.config.js
module.exports = {
  basePath: '/pyspark-koans',
  output: 'export',
};

# Deploy
npm run export
# Push out/ directory to gh-pages branch
```

### Netlify / Vercel

```bash
npm run export
# Point deployment to out/ directory
```

### Any Static Host

The `out/` directory contains pure static HTML/CSS/JS - deploy anywhere!

## 🧪 Testing Strategy

### Component Testing

```bash
npm install --save-dev @testing-library/react jest
# Test components in isolation
```

### Koan Testing

```javascript
// Validate koan definitions
test('koan has required fields', () => {
  const koan = getKoan(1);
  expect(koan.id).toBeDefined();
  expect(koan.template).toContain('___');
  expect(koan.solution).toBeDefined();
});
```

## 🎯 Roadmap to 100+ Koans

### Phase 1: Core PySpark (40 koans)
- ✅ Basics (7 koans)
- ✅ Column Operations (5 koans)
- ✅ String Functions (4 koans)
- ✅ Aggregations (3 koans)
- ✅ Joins (3 koans)
- ✅ Window Functions (3 koans)
- ✅ Null Handling (2 koans)
- ✅ Advanced (3 koans)
- ✅ Delta Lake (10 koans)

### Phase 2: Expanded PySpark (30 koans)
- Date/Time Functions (8 koans)
- Complex Types - Arrays & Maps (8 koans)
- UDFs & Custom Functions (6 koans)
- Performance Optimization (8 koans)

### Phase 3: Spark SQL (20 koans)
- Basic Queries (10 koans)
- Joins & CTEs (10 koans)

### Phase 4: MLflow & MLOps (14 koans)
- Experiment Tracking (8 koans)
- Model Registry (6 koans)

### Phase 5: DABs (8 koans)
- YAML Configuration (8 koans)

**Total: 112 koans** 🎉

## 🤝 Contributing

1. Add your koan file in appropriate category
2. Register in `src/koans/index.js`
3. Test locally with `npm run dev`
4. Submit PR

## 📄 License

MIT

## 🙏 Acknowledgments

- Pyodide team for Python in the browser
- Databricks for PySpark
- Ruby Koans for the learning methodology
