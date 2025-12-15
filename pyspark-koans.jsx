import React, { useState, useEffect, useRef } from 'react';

// Koan definitions - each teaches one concept
const KOANS = [
  {
    id: 1,
    title: "Creating a DataFrame",
    category: "Basics",
    description: "Create a DataFrame from a list of tuples. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
`,
    template: `# Create a DataFrame from the data and columns
df = spark.___(___, ___)

# The DataFrame should have 3 rows
assert df.count() == 3, f"Expected 3 rows, got {df.count()}"
print("✓ DataFrame created with correct row count")

# The DataFrame should have 2 columns
assert len(df.columns) == 2, f"Expected 2 columns, got {len(df.columns)}"
print("✓ DataFrame has correct number of columns")

print("\\n🎉 Koan complete! You've learned to create a DataFrame.")`,
    solution: `df = spark.createDataFrame(data, columns)`,
    hints: [
      "DataFrames are created from SparkSession",
      "The method name describes what you're doing: create + DataFrame",
      "Pass the data first, then the column names"
    ]
  },
  {
    id: 2,
    title: "Selecting Columns",
    category: "Basics",
    description: "Select specific columns from a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34, "NYC"), ("Bob", 45, "LA"), ("Charlie", 29, "Chicago")]
df = spark.createDataFrame(data, ["name", "age", "city"])
`,
    template: `# Select only the 'name' and 'city' columns
result = df.___("name", "___")

# Result should have exactly 2 columns
assert len(result.columns) == 2, f"Expected 2 columns, got {len(result.columns)}"
print("✓ Correct number of columns selected")

# Result should contain 'name' and 'city'
assert "name" in result.columns, "Missing 'name' column"
assert "city" in result.columns, "Missing 'city' column"
print("✓ Correct columns selected")

print("\\n🎉 Koan complete! You've learned to select columns.")`,
    solution: `result = df.select("name", "city")`,
    hints: [
      "Think about what action you want: you want to 'select' columns",
      "The method takes column names as strings"
    ]
  },
  {
    id: 3,
    title: "Filtering Rows",
    category: "Basics",
    description: "Filter rows based on a condition. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 52)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Filter to only include people over 35
from pyspark.sql.functions import col

result = df.___(col("age") ___ 35)

# Should have 2 people over 35
assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
print("✓ Correct number of rows filtered")

# Collect and verify
rows = result.collect()
ages = [row["age"] for row in rows]
assert all(age > 35 for age in ages), "Some ages are not > 35"
print("✓ All remaining rows have age > 35")

print("\\n🎉 Koan complete! You've learned to filter rows.")`,
    solution: `result = df.filter(col("age") > 35)`,
    hints: [
      "You want to 'filter' the DataFrame",
      "Use a comparison operator to check if age is greater than 35",
      "The col() function references a column by name"
    ]
  },
  {
    id: 4,
    title: "Adding Columns",
    category: "Transformations",
    description: "Add a new calculated column to a DataFrame. Replace ___ with the correct code.",
    setup: `
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])
`,
    template: `# Add a new column 'age_in_months' that multiplies age by 12
from pyspark.sql.functions import col

result = df.___("age_in_months", col("___") * 12)

# Should still have 3 rows
assert result.count() == 3
print("✓ Row count unchanged")

# Should now have 3 columns
assert len(result.columns) == 3, f"Expected 3 columns, got {len(result.columns)}"
print("✓ New column added")

# Check calculation is correct
first_row = result.filter(col("name") == "Alice").collect()[0]
assert first_row["age_in_months"] == 408, f"Expected 408, got {first_row['age_in_months']}"
print("✓ Calculation is correct (34 * 12 = 408)")

print("\\n🎉 Koan complete! You've learned to add columns.")`,
    solution: `result = df.withColumn("age_in_months", col("age") * 12)`,
    hints: [
      "The method name suggests adding 'with' a new 'Column'",
      "First argument is the new column name, second is the expression",
      "Reference the 'age' column to multiply it"
    ]
  },
  {
    id: 5,
    title: "Grouping and Aggregating",
    category: "Transformations",
    description: "Group data and calculate aggregates. Replace ___ with the correct code.",
    setup: `
data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Engineering", "Charlie", 6000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 5500)
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
`,
    template: `# Group by department and calculate average salary
from pyspark.sql.functions import avg, round

result = df.___("department").agg(
    round(___("salary"), 2).alias("avg_salary")
)

# Should have 2 departments
assert result.count() == 2, f"Expected 2 groups, got {result.count()}"
print("✓ Correct number of groups")

# Check Engineering average (6000 + 6500 + 5500) / 3 = 6000
eng_row = result.filter(col("department") == "Engineering").collect()[0]
assert eng_row["avg_salary"] == 6000.0, f"Expected 6000.0, got {eng_row['avg_salary']}"
print("✓ Engineering average salary is correct")

# Check Sales average (5000 + 4500) / 2 = 4750
sales_row = result.filter(col("department") == "Sales").collect()[0]
assert sales_row["avg_salary"] == 4750.0, f"Expected 4750.0, got {sales_row['avg_salary']}"
print("✓ Sales average salary is correct")

print("\\n🎉 Koan complete! You've learned to group and aggregate.")`,
    solution: `result = df.groupBy("department").agg(round(avg("salary"), 2).alias("avg_salary"))`,
    hints: [
      "First you need to group the data using 'groupBy'",
      "Then aggregate using 'avg' function for average",
      "The avg function takes a column name"
    ]
  },
  {
    id: 6,
    title: "Joining DataFrames",
    category: "Transformations",
    description: "Join two DataFrames together. Replace ___ with the correct code.",
    setup: `
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Sales"),
    (103, "Marketing")
], ["dept_id", "dept_name"])
`,
    template: `# Join employees with departments on dept_id
result = employees.___(departments, ___, "inner")

# Should have 3 rows (all employees have matching departments)
assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
print("✓ Correct number of joined rows")

# Should have columns from both DataFrames
assert "name" in result.columns, "Missing 'name' column"
assert "dept_name" in result.columns, "Missing 'dept_name' column"
print("✓ Columns from both DataFrames present")

# Alice should be in Engineering
alice = result.filter(col("name") == "Alice").collect()[0]
assert alice["dept_name"] == "Engineering", f"Expected Engineering, got {alice['dept_name']}"
print("✓ Join matched correctly (Alice -> Engineering)")

print("\\n🎉 Koan complete! You've learned to join DataFrames.")`,
    solution: `result = employees.join(departments, "dept_id", "inner")`,
    hints: [
      "The method to combine DataFrames is called 'join'",
      "Specify the column to join on as a string",
      "The join type is already provided: 'inner'"
    ]
  },
  {
    id: 7,
    title: "Window Functions",
    category: "Advanced",
    description: "Use window functions to calculate running totals. Replace ___ with the correct code.",
    setup: `
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

data = [
    ("2024-01-01", 100),
    ("2024-01-02", 150),
    ("2024-01-03", 200),
    ("2024-01-04", 175)
]
df = spark.createDataFrame(data, ["date", "sales"])
`,
    template: `# Create a window that orders by date and includes all previous rows
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.___)

# Add running total column
result = df.withColumn("running_total", ___("sales").over(window_spec))

# Check the running totals
rows = result.orderBy("date").collect()

assert rows[0]["running_total"] == 100, "Day 1 should be 100"
print("✓ Day 1: 100")

assert rows[1]["running_total"] == 250, "Day 2 should be 250 (100+150)"
print("✓ Day 2: 250 (100+150)")

assert rows[2]["running_total"] == 450, "Day 3 should be 450 (100+150+200)"
print("✓ Day 3: 450 (100+150+200)")

assert rows[3]["running_total"] == 625, "Day 4 should be 625 (100+150+200+175)"
print("✓ Day 4: 625 (100+150+200+175)")

print("\\n🎉 Koan complete! You've learned window functions.")`,
    solution: `window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)\nresult = df.withColumn("running_total", spark_sum("sales").over(window_spec))`,
    hints: [
      "For a running total, you want from the start up to the 'currentRow'",
      "Use spark_sum (aliased from sum) to add up values",
      "The .over() method applies the function to the window"
    ]
  }
];

// PySpark shim that will be injected into Pyodide
const PYSPARK_SHIM = `
import pandas as pd
from typing import List, Any, Optional, Union
from dataclasses import dataclass
import json

class Row(dict):
    """PySpark-like Row class"""
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self.__dict__.update(kwargs)
    
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'Row' object has no attribute '{name}'")
    
    def asDict(self):
        return dict(self)

class Column:
    """PySpark-like Column class for expressions"""
    def __init__(self, name: str, expr: Optional[str] = None):
        self.name = name
        self.expr = expr or name
        self._alias = None
    
    def __gt__(self, other):
        return Column(self.name, f"({self.expr}) > {repr(other)}")
    
    def __lt__(self, other):
        return Column(self.name, f"({self.expr}) < {repr(other)}")
    
    def __ge__(self, other):
        return Column(self.name, f"({self.expr}) >= {repr(other)}")
    
    def __le__(self, other):
        return Column(self.name, f"({self.expr}) <= {repr(other)}")
    
    def __eq__(self, other):
        return Column(self.name, f"({self.expr}) == {repr(other)}")
    
    def __ne__(self, other):
        return Column(self.name, f"({self.expr}) != {repr(other)}")
    
    def __mul__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) * ({other.expr})")
        return Column(self.name, f"({self.expr}) * {repr(other)}")
    
    def __add__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) + ({other.expr})")
        return Column(self.name, f"({self.expr}) + {repr(other)}")
    
    def __sub__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) - ({other.expr})")
        return Column(self.name, f"({self.expr}) - {repr(other)}")
    
    def __truediv__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) / ({other.expr})")
        return Column(self.name, f"({self.expr}) / {repr(other)}")
    
    def alias(self, name: str):
        new_col = Column(self.name, self.expr)
        new_col._alias = name
        return new_col
    
    def over(self, window):
        new_col = Column(self.name, self.expr)
        new_col._window = window
        new_col._is_window_func = True
        return new_col

class WindowSpec:
    """PySpark-like Window specification"""
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"
    
    def __init__(self):
        self._partition_cols = []
        self._order_cols = []
        self._row_start = None
        self._row_end = None
    
    def partitionBy(self, *cols):
        new_spec = WindowSpec()
        new_spec._partition_cols = list(cols)
        new_spec._order_cols = self._order_cols
        new_spec._row_start = self._row_start
        new_spec._row_end = self._row_end
        return new_spec
    
    def orderBy(self, *cols):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = list(cols)
        new_spec._row_start = self._row_start
        new_spec._row_end = self._row_end
        return new_spec
    
    def rowsBetween(self, start, end):
        new_spec = WindowSpec()
        new_spec._partition_cols = self._partition_cols
        new_spec._order_cols = self._order_cols
        new_spec._row_start = start
        new_spec._row_end = end
        return new_spec

class Window:
    """PySpark-like Window class"""
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"
    
    @staticmethod
    def partitionBy(*cols):
        spec = WindowSpec()
        return spec.partitionBy(*cols)
    
    @staticmethod
    def orderBy(*cols):
        spec = WindowSpec()
        return spec.orderBy(*cols)

class GroupedData:
    """PySpark-like GroupedData class"""
    def __init__(self, df: 'DataFrame', group_cols: List[str]):
        self._df = df
        self._group_cols = group_cols
    
    def agg(self, *exprs):
        pdf = self._df._pdf.copy()
        result_cols = {col: 'first' for col in self._group_cols}
        
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg':
                    result_cols[source_col] = 'mean'
                elif func == 'sum':
                    result_cols[source_col] = 'sum'
                elif func == 'count':
                    result_cols[source_col] = 'count'
                elif func == 'min':
                    result_cols[source_col] = 'min'
                elif func == 'max':
                    result_cols[source_col] = 'max'
        
        grouped = pdf.groupby(self._group_cols, as_index=False)
        
        # Build aggregation result
        agg_results = {}
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func
                
                if func == 'avg':
                    agg_results[col_name] = grouped[source_col].mean()[source_col]
                elif func == 'sum':
                    agg_results[col_name] = grouped[source_col].sum()[source_col]
                elif func == 'count':
                    agg_results[col_name] = grouped[source_col].count()[source_col]
                elif func == 'min':
                    agg_results[col_name] = grouped[source_col].min()[source_col]
                elif func == 'max':
                    agg_results[col_name] = grouped[source_col].max()[source_col]
                
                if hasattr(expr, '_round_decimals'):
                    agg_results[col_name] = agg_results[col_name].round(expr._round_decimals)
        
        result_pdf = grouped[self._group_cols].first()
        for col_name, values in agg_results.items():
            result_pdf[col_name] = values.values
        
        return DataFrame(result_pdf)

class DataFrame:
    """PySpark-like DataFrame backed by pandas"""
    def __init__(self, pdf: pd.DataFrame):
        self._pdf = pdf
    
    @property
    def columns(self) -> List[str]:
        return list(self._pdf.columns)
    
    def count(self) -> int:
        return len(self._pdf)
    
    def show(self, n: int = 20, truncate: bool = True):
        print(self._pdf.head(n).to_string())
    
    def collect(self) -> List[Row]:
        return [Row(**row) for row in self._pdf.to_dict('records')]
    
    def select(self, *cols) -> 'DataFrame':
        col_names = []
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            elif isinstance(c, Column):
                col_names.append(c.name)
        return DataFrame(self._pdf[col_names].copy())
    
    def filter(self, condition: Column) -> 'DataFrame':
        if isinstance(condition, Column):
            mask = self._pdf.eval(condition.expr)
            return DataFrame(self._pdf[mask].copy())
        return self
    
    def where(self, condition: Column) -> 'DataFrame':
        return self.filter(condition)
    
    def withColumn(self, name: str, col: Column) -> 'DataFrame':
        pdf = self._pdf.copy()
        
        if hasattr(col, '_is_window_func') and col._is_window_func:
            # Handle window function
            window = col._window
            if col._agg_func == 'sum':
                if window._order_cols:
                    order_col = window._order_cols[0]
                    pdf = pdf.sort_values(order_col)
                pdf[name] = pdf[col._source_col].cumsum()
            return DataFrame(pdf)
        else:
            # Regular column expression
            pdf[name] = pdf.eval(col.expr)
        return DataFrame(pdf)
    
    def groupBy(self, *cols) -> GroupedData:
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return GroupedData(self, col_names)
    
    def join(self, other: 'DataFrame', on: Union[str, List[str]], how: str = 'inner') -> 'DataFrame':
        result = self._pdf.merge(other._pdf, on=on, how=how)
        return DataFrame(result)
    
    def orderBy(self, *cols, ascending=True) -> 'DataFrame':
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.sort_values(col_names, ascending=ascending).reset_index(drop=True))
    
    def sort(self, *cols, ascending=True) -> 'DataFrame':
        return self.orderBy(*cols, ascending=ascending)
    
    def drop(self, *cols) -> 'DataFrame':
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.drop(columns=col_names))
    
    def distinct(self) -> 'DataFrame':
        return DataFrame(self._pdf.drop_duplicates())
    
    def limit(self, n: int) -> 'DataFrame':
        return DataFrame(self._pdf.head(n).copy())
    
    def toPandas(self) -> pd.DataFrame:
        return self._pdf.copy()

class SparkSession:
    """PySpark-like SparkSession"""
    def __init__(self):
        pass
    
    def createDataFrame(self, data: List[tuple], schema: List[str]) -> DataFrame:
        pdf = pd.DataFrame(data, columns=schema)
        return DataFrame(pdf)

# Create global spark session
spark = SparkSession()

# Functions module
def col(name: str) -> Column:
    return Column(name)

def lit(value: Any) -> Column:
    return Column("_lit", repr(value))

def avg(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'avg'
    c._source_col = col_name
    return c

def sum(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'sum'
    c._source_col = col_name
    return c

def count(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'count'
    c._source_col = col_name
    return c

def min(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'min'
    c._source_col = col_name
    return c

def max(col_name: str) -> Column:
    c = Column(col_name)
    c._agg_func = 'max'
    c._source_col = col_name
    return c

def round(col_expr: Column, decimals: int = 0) -> Column:
    new_col = Column(col_expr.name, col_expr.expr)
    if hasattr(col_expr, '_agg_func'):
        new_col._agg_func = col_expr._agg_func
        new_col._source_col = col_expr._source_col
    new_col._round_decimals = decimals
    return new_col

# Make these available in pyspark.sql.functions namespace
class _Functions:
    col = staticmethod(col)
    lit = staticmethod(lit)
    avg = staticmethod(avg)
    sum = staticmethod(sum)
    count = staticmethod(count)
    min = staticmethod(min)
    max = staticmethod(max)
    round = staticmethod(round)

class _PySpark:
    class sql:
        functions = _Functions
        class window:
            Window = Window

pyspark = _PySpark()
`;

export default function PySparkKoans() {
  const [currentKoan, setCurrentKoan] = useState(0);
  const [code, setCode] = useState('');
  const [output, setOutput] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [pyodide, setPyodide] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [showHints, setShowHints] = useState(false);
  const [currentHint, setCurrentHint] = useState(0);
  const [completedKoans, setCompletedKoans] = useState(new Set());
  const [showSolution, setShowSolution] = useState(false);
  const textareaRef = useRef(null);

  const koan = KOANS[currentKoan];

  // Initialize Pyodide
  useEffect(() => {
    async function initPyodide() {
      setIsLoading(true);
      try {
        const pyodideInstance = await window.loadPyodide({
          indexURL: "https://cdn.jsdelivr.net/pyodide/v0.24.1/full/"
        });
        await pyodideInstance.loadPackage(['pandas']);
        
        // Inject PySpark shim
        await pyodideInstance.runPythonAsync(PYSPARK_SHIM);
        
        setPyodide(pyodideInstance);
        setOutput('✓ PySpark environment ready!\n\nClick "Run Code" to test your solution.');
      } catch (error) {
        setOutput(`Error loading Python environment: ${error.message}`);
      }
      setIsLoading(false);
    }
    
    // Load Pyodide script
    if (!window.loadPyodide) {
      const script = document.createElement('script');
      script.src = 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js';
      script.onload = initPyodide;
      document.head.appendChild(script);
    } else {
      initPyodide();
    }
  }, []);

  // Update code when koan changes
  useEffect(() => {
    setCode(koan.template);
    setOutput('');
    setShowHints(false);
    setCurrentHint(0);
    setShowSolution(false);
  }, [currentKoan]);

  const runCode = async () => {
    if (!pyodide) return;
    
    setIsRunning(true);
    setOutput('Running...\n');
    
    try {
      // Reset environment and re-inject shim
      await pyodide.runPythonAsync(PYSPARK_SHIM);
      
      // Run setup code
      await pyodide.runPythonAsync(koan.setup);
      
      // Capture stdout
      await pyodide.runPythonAsync(`
import sys
from io import StringIO
_stdout_capture = StringIO()
sys.stdout = _stdout_capture
`);
      
      // Run user code
      await pyodide.runPythonAsync(code);
      
      // Get captured output
      const capturedOutput = await pyodide.runPythonAsync(`
sys.stdout = sys.__stdout__
_stdout_capture.getvalue()
`);
      
      setOutput(capturedOutput);
      
      // Check if koan is complete
      if (capturedOutput.includes('🎉 Koan complete!')) {
        setCompletedKoans(prev => new Set([...prev, currentKoan]));
      }
    } catch (error) {
      setOutput(`Error:\n${error.message}\n\nTip: Check your syntax and make sure you replaced all ___ placeholders.`);
    }
    
    setIsRunning(false);
  };

  const nextHint = () => {
    setShowHints(true);
    if (currentHint < koan.hints.length - 1) {
      setCurrentHint(prev => prev + 1);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newCode = code.substring(0, start) + '    ' + code.substring(end);
      setCode(newCode);
      setTimeout(() => {
        e.target.selectionStart = e.target.selectionEnd = start + 4;
      }, 0);
    }
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      runCode();
    }
  };

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-orange-500 mb-2">PySpark Koans</h1>
          <p className="text-gray-400">Learn PySpark through test-driven exercises</p>
        </div>
        
        {/* Progress */}
        <div className="mb-6 flex gap-2 flex-wrap">
          {KOANS.map((k, idx) => (
            <button
              key={k.id}
              onClick={() => setCurrentKoan(idx)}
              className={`w-10 h-10 rounded-lg font-mono text-sm transition-all ${
                idx === currentKoan
                  ? 'bg-orange-600 text-white'
                  : completedKoans.has(idx)
                  ? 'bg-green-600 text-white'
                  : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
              }`}
            >
              {completedKoans.has(idx) ? '✓' : k.id}
            </button>
          ))}
          <div className="ml-4 text-gray-500 self-center">
            {completedKoans.size}/{KOANS.length} completed
          </div>
        </div>

        {/* Main Content */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Left: Koan Info */}
          <div className="space-y-4">
            <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xs px-2 py-1 bg-gray-800 rounded text-gray-400">
                  {koan.category}
                </span>
                <span className="text-xs text-gray-600">Koan {koan.id}</span>
              </div>
              <h2 className="text-xl font-semibold text-white mb-2">{koan.title}</h2>
              <p className="text-gray-400">{koan.description}</p>
            </div>

            {/* Setup Code */}
            <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
              <h3 className="text-sm font-medium text-gray-500 mb-2">Setup (read-only)</h3>
              <pre className="text-sm text-gray-400 font-mono whitespace-pre-wrap bg-gray-950 p-3 rounded">
                {koan.setup.trim()}
              </pre>
            </div>

            {/* Hints */}
            {showHints && (
              <div className="bg-yellow-900/20 border border-yellow-800/50 rounded-lg p-4">
                <h3 className="text-sm font-medium text-yellow-500 mb-2">
                  Hint {currentHint + 1}/{koan.hints.length}
                </h3>
                <p className="text-yellow-200/80">{koan.hints[currentHint]}</p>
              </div>
            )}

            {/* Solution */}
            {showSolution && (
              <div className="bg-green-900/20 border border-green-800/50 rounded-lg p-4">
                <h3 className="text-sm font-medium text-green-500 mb-2">Solution</h3>
                <pre className="text-sm text-green-200/80 font-mono whitespace-pre-wrap">
                  {koan.solution}
                </pre>
              </div>
            )}
          </div>

          {/* Right: Editor and Output */}
          <div className="space-y-4">
            {/* Editor */}
            <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
              <div className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
                <span className="text-sm text-gray-400">Your Code</span>
                <span className="text-xs text-gray-600">Ctrl/Cmd+Enter to run</span>
              </div>
              <textarea
                ref={textareaRef}
                value={code}
                onChange={(e) => setCode(e.target.value)}
                onKeyDown={handleKeyDown}
                className="w-full h-72 p-4 bg-gray-950 text-gray-100 font-mono text-sm resize-none focus:outline-none"
                spellCheck={false}
              />
            </div>

            {/* Controls */}
            <div className="flex gap-2 flex-wrap">
              <button
                onClick={runCode}
                disabled={isLoading || isRunning}
                className="px-4 py-2 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-700 disabled:text-gray-500 rounded-lg font-medium transition-colors"
              >
                {isLoading ? 'Loading Python...' : isRunning ? 'Running...' : 'Run Code'}
              </button>
              <button
                onClick={nextHint}
                disabled={currentHint >= koan.hints.length - 1 && showHints}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 rounded-lg transition-colors"
              >
                {showHints ? 'Next Hint' : 'Show Hint'}
              </button>
              <button
                onClick={() => setShowSolution(!showSolution)}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
              >
                {showSolution ? 'Hide Solution' : 'Show Solution'}
              </button>
              <button
                onClick={() => setCode(koan.template)}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
              >
                Reset
              </button>
            </div>

            {/* Output */}
            <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
              <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                <span className="text-sm text-gray-400">Output</span>
              </div>
              <pre className="p-4 h-48 overflow-auto font-mono text-sm whitespace-pre-wrap">
                {output || 'Output will appear here...'}
              </pre>
            </div>

            {/* Navigation */}
            <div className="flex justify-between">
              <button
                onClick={() => setCurrentKoan(Math.max(0, currentKoan - 1))}
                disabled={currentKoan === 0}
                className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
              >
                ← Previous
              </button>
              <button
                onClick={() => setCurrentKoan(Math.min(KOANS.length - 1, currentKoan + 1))}
                disabled={currentKoan === KOANS.length - 1}
                className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
              >
                Next →
              </button>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-8 text-center text-gray-600 text-sm">
          <p>Running PySpark syntax in browser via Pyodide + pandas shim</p>
        </div>
      </div>
    </div>
  );
}
