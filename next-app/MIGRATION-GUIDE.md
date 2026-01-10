# Migration Guide: Monolithic → Modular Next.js

This guide explains how to migrate the existing monolithic koan files to the new modular Next.js architecture.

## 📊 Current vs New Structure

### Before (Monolithic)
```
pyspark-koans/
├── pyspark-koans.jsx (7 koans + shim + UI) ~800 lines
├── pyspark-koans-expanded.jsx (30 koans + shim + UI) ~2000 lines
└── pyspark-delta-koans-complete.jsx (17 koans + shim + UI) ~1640 lines
```

**Problems:**
- 3 files with duplicate content
- Can't easily share/link specific koans
- No code splitting = large initial bundle
- Hard to maintain as it grows

### After (Modular)
```
next-app/
├── src/koans/ (40 separate koan files) ~100 lines each
├── src/shims/ (modular Python shims) ~300-500 lines each
├── src/components/ (reusable UI) ~50-100 lines each
└── pages/koans/[id].js (dynamic routing)
```

**Benefits:**
- Zero duplication
- Automatic routing & code splitting
- Easy to add new koans
- Maintainable at scale (100+ koans)

## 🔄 Migration Steps

### Step 1: Extract Koans from Monolithic Files

#### Script to Extract Koans

```javascript
// scripts/extract-koans.js
const fs = require('fs');

// Read the monolithic file
const content = fs.readFileSync('./pyspark-koans-expanded.jsx', 'utf8');

// Extract KOANS array
const koansMatch = content.match(/const KOANS = \[([\s\S]*?)\];/);
const koansText = koansMatch[1];

// Split into individual koans (basic regex - adjust as needed)
const koanObjects = koansText.split(/},\s*{/);

koanObjects.forEach((koanText, index) => {
  // Parse koan object
  const idMatch = koanText.match(/id:\s*(\d+)/);
  if (!idMatch) return;

  const id = idMatch[1].padStart(3, '0');
  const titleMatch = koanText.match(/title:\s*"([^"]+)"/);
  const title = titleMatch ? titleMatch[1] : `koan-${id}`;

  // Determine category folder
  const categoryMatch = koanText.match(/category:\s*"([^"]+)"/);
  const category = categoryMatch ? categoryMatch[1].toLowerCase().replace(/\s+/g, '-') : 'misc';

  // Create koan file
  const koanContent = `
export default {
  ${koanText}
};
`.trim();

  // Write to file
  const folder = `./src/koans/pyspark/${category}`;
  const filename = `koan-${id}-${title.toLowerCase().replace(/\s+/g, '-')}.js`;

  fs.mkdirSync(folder, { recursive: true });
  fs.writeFileSync(`${folder}/${filename}`, koanContent);

  console.log(`✓ Created ${folder}/${filename}`);
});
```

Run with:
```bash
node scripts/extract-koans.js
```

### Step 2: Extract Python Shims

#### Split Monolithic Shim

The old shim is one giant string. Split it into modules:

```python
# Old (monolithic)
const FULL_SHIM = `
import pandas as pd

class Row(dict):
    ...

class Column:
    ...

class DataFrame:
    ...

def col(name):
    ...

# 1300 lines total!
`;
```

```python
# New (modular)

# src/shims/pyspark/core.py
import pandas as pd
class Row(dict): ...
class Column: ...
class DataFrame: ...

# src/shims/pyspark/functions.py
def col(name): ...
def lit(value): ...
def avg(col_name): ...

# src/shims/pyspark/window.py
class Window: ...
def row_number(): ...
```

**Benefits:**
- Easier to find and modify specific functionality
- Can load shims incrementally (base + features as needed)
- Better code organization

### Step 3: Build Shim Bundle

Since Pyodide needs a single Python script, combine modules at build time:

```javascript
// scripts/build-shim.js
const fs = require('fs');
const path = require('path');

function buildShim(shimDir, outputFile) {
  const files = [
    'core.py',
    'functions.py',
    'window.py',
    'io.py'
  ];

  let combined = '# PySpark Shim - Auto-generated\n\n';

  files.forEach(file => {
    const content = fs.readFileSync(path.join(shimDir, file), 'utf8');
    combined += `\n# ===== ${file} =====\n`;
    combined += content;
    combined += '\n';
  });

  fs.writeFileSync(outputFile, combined);
  console.log(`✓ Built ${outputFile}`);
}

buildShim('./src/shims/pyspark', './public/shims/pyspark-shim.py');
buildShim('./src/shims/delta', './public/shims/delta-shim.py');
```

Add to package.json:
```json
{
  "scripts": {
    "build": "node scripts/build-shim.js && next build"
  }
}
```

### Step 4: Create Koan Index

```javascript
// src/koans/index.js

// Auto-import all koans (manual for now, can automate later)
import koan1 from './pyspark/basics/koan-001-create-dataframe';
import koan2 from './pyspark/basics/koan-002-select-columns';
// ... import all koans

const koansById = {
  1: koan1,
  2: koan2,
  // ... etc
};

export function getKoan(id) {
  return koansById[parseInt(id)] || null;
}

export function getAllKoanIds() {
  return Object.keys(koansById).map(Number).sort((a, b) => a - b);
}
```

### Step 5: Convert React Components

#### Old (Monolithic)
```jsx
// One giant component with everything inline
export default function PySparkKoans() {
  // 200 lines of state management
  // Inline JSX for editor, output, sidebar, hints
  // All logic mixed together
}
```

#### New (Modular)
```jsx
// pages/koans/[id].js - Composition of small components
export default function KoanPage({ koan }) {
  return (
    <>
      <Sidebar ... />
      <KoanHeader ... />
      <KoanEditor ... />
      <OutputPanel ... />
      <Controls ... />
    </>
  );
}
```

Each component is ~50-100 lines and reusable.

### Step 6: Set Up Next.js Routing

No work needed! Next.js automatically creates routes:
- `pages/koans/[id].js` → `/koans/1`, `/koans/2`, etc.
- `pages/categories/[category].js` → `/categories/basics`, `/categories/delta-lake`
- `pages/index.js` → `/`

### Step 7: Migrate Progress Tracking

```javascript
// Old: useState in component
const [completedKoans, setCompletedKoans] = useState(new Set());

// New: Custom hook with localStorage
const { progress, markComplete, isComplete } = useKoanProgress();
```

Benefits:
- Reusable across pages
- Persists to localStorage automatically
- Can add sync to cloud later

## ✅ Verification Checklist

After migration, verify:

- [ ] All 40 koans load individually at `/koans/{id}`
- [ ] Python shim loads correctly (check browser console)
- [ ] Code execution works (test a few koans)
- [ ] Progress tracking persists across page reloads
- [ ] Navigation between koans works
- [ ] Hints and solutions display correctly
- [ ] Build produces static export: `npm run export`
- [ ] Static export deploys successfully

## 🔧 Automated Migration Tools

### Full Migration Script

```bash
# scripts/migrate-all.sh

echo "Step 1: Extract koans from monolithic files..."
node scripts/extract-koans.js

echo "Step 2: Build Python shims..."
node scripts/build-shim.js

echo "Step 3: Generate koan index..."
node scripts/generate-index.js

echo "Step 4: Verify all koans..."
node scripts/verify-koans.js

echo "✓ Migration complete!"
echo "Run 'npm run dev' to test"
```

## 📈 Progressive Migration Strategy

Don't migrate everything at once! Instead:

### Phase 1: Set Up Infrastructure
1. Create Next.js project
2. Set up one example koan
3. Verify Pyodide + shim loading
4. Test deployment

### Phase 2: Migrate Core Koans
1. Migrate basics category (7 koans)
2. Test thoroughly
3. Deploy to test environment

### Phase 3: Bulk Migration
1. Extract remaining koans with script
2. Review and test each category
3. Deploy incrementally

### Phase 4: Cleanup
1. Archive old monolithic files
2. Update documentation
3. Final production deployment

## 🆘 Troubleshooting

### Issue: Shim import errors

**Problem:** `ModuleNotFoundError: No module named 'pyspark'`

**Solution:** Ensure `index.py` creates a module hierarchy:
```python
# src/shims/index.py
import sys
from types import ModuleType

# Create pyspark module
pyspark = ModuleType('pyspark')
sys.modules['pyspark'] = pyspark

# Import submodules
from pyspark.core import *
from pyspark.functions import *
```

### Issue: Next.js SSR errors with Pyodide

**Problem:** `window is not defined` during build

**Solution:** Guard browser-only code:
```javascript
useEffect(() => {
  if (typeof window !== 'undefined') {
    // Pyodide code here
  }
}, []);
```

### Issue: Slow initial load

**Problem:** 10MB+ Pyodide + pandas takes time

**Solution:**
1. Show loading spinner
2. Use CDN for Pyodide (fast)
3. Lazy load Delta shim only when needed

## 🎯 Next Steps

After migration:
1. Add 30 more koans (Phase 2 expansion)
2. Implement user accounts (optional)
3. Add social sharing features
4. Create API for progress sync
5. Build mobile-friendly PWA version

---

**Need help?** Check the README or open an issue!
