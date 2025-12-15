import React, { useState, useEffect, useRef } from 'react';

// PySpark shim code that runs in Pyodide - pandas-backed implementation
const PYSPARK_SHIM = `
import pandas as pd
import json
from functools import reduce

# Mock SparkSession
class SparkSession:
    class Builder:
        def __init__(self):
            self._app_name = "PySparkKoans"
        
        def appName(self, name):
            self._app_name = name
            return self
        
        def master(self, master):
            return self
        
        def getOrCreate(self):
            return SparkSession()
    
    builder = Builder()
    
    def createDataFrame(self, data, schema=None):
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], tuple) and schema:
                if isinstance(schema, list):
                    cols = schema
                else:
                    cols = [f.name for f in schema.fields]
                df = pd.DataFrame(data, columns=cols)
            elif isinstance(data[0], dict):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame(data, columns=schema if schema else None)
        else:
            df = pd.DataFrame(data)
        return DataFrame(df)
    
    def range(self, start, end=None, step=1):
        if end is None:
            end = start
            start = 0
        data = list(range(start, end, step))
        return DataFrame(pd.DataFrame({'id': data}))

spark = SparkSession()

# Column class for expressions
class Column:
    def __init__(self, name, expr=None):
        self.name = name
        self.expr = expr or name
    
    def __repr__(self):
        return f"Column<{self.name}>"
    
    def alias(self, new_name):
        col = Column(new_name, self.expr)
        col._alias = new_name
        return col
    
    def __add__(self, other):
        return Column(f"({self.name} + {other})", ('add', self.name, other))
    
    def __sub__(self, other):
        return Column(f"({self.name} - {other})", ('sub', self.name, other))
    
    def __mul__(self, other):
        return Column(f"({self.name} * {other})", ('mul', self.name, other))
    
    def __truediv__(self, other):
        return Column(f"({self.name} / {other})", ('div', self.name, other))
    
    def __eq__(self, other):
        return Column(f"({self.name} == {other})", ('eq', self.name, other))
    
    def __ne__(self, other):
        return Column(f"({self.name} != {other})", ('ne', self.name, other))
    
    def __gt__(self, other):
        return Column(f"({self.name} > {other})", ('gt', self.name, other))
    
    def __ge__(self, other):
        return Column(f"({self.name} >= {other})", ('ge', self.name, other))
    
    def __lt__(self, other):
        return Column(f"({self.name} < {other})", ('lt', self.name, other))
    
    def __le__(self, other):
        return Column(f"({self.name} <= {other})", ('le', self.name, other))
    
    def __and__(self, other):
        return Column(f"({self.name} & {other.name})", ('and', self.name, other.name))
    
    def __or__(self, other):
        return Column(f"({self.name} | {other.name})", ('or', self.name, other.name))
    
    def asc(self):
        return (self.name, True)
    
    def desc(self):
        return (self.name, False)
    
    def cast(self, dtype):
        return Column(self.name, ('cast', self.name, dtype))
    
    def isNull(self):
        return Column(f"({self.name} IS NULL)", ('isnull', self.name))
    
    def isNotNull(self):
        return Column(f"({self.name} IS NOT NULL)", ('isnotnull', self.name))
    
    def isin(self, *values):
        return Column(f"({self.name} IN {values})", ('isin', self.name, values))
    
    def contains(self, substr):
        return Column(f"({self.name} CONTAINS {substr})", ('contains', self.name, substr))
    
    def startswith(self, prefix):
        return Column(f"({self.name} STARTSWITH {prefix})", ('startswith', self.name, prefix))
    
    def endswith(self, suffix):
        return Column(f"({self.name} ENDSWITH {suffix})", ('endswith', self.name, suffix))

def col(name):
    return Column(name)

def lit(value):
    return Column(str(value), ('lit', value))

# Functions module
class functions:
    @staticmethod
    def col(name):
        return Column(name)
    
    @staticmethod
    def lit(value):
        return Column(str(value), ('lit', value))
    
    @staticmethod
    def sum(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"sum({column.name})", ('sum', column.name))
    
    @staticmethod
    def avg(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"avg({column.name})", ('avg', column.name))
    
    @staticmethod
    def mean(column):
        return functions.avg(column)
    
    @staticmethod
    def count(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"count({column.name})", ('count', column.name))
    
    @staticmethod
    def max(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"max({column.name})", ('max', column.name))
    
    @staticmethod
    def min(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"min({column.name})", ('min', column.name))
    
    @staticmethod
    def first(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"first({column.name})", ('first', column.name))
    
    @staticmethod
    def last(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"last({column.name})", ('last', column.name))
    
    @staticmethod
    def countDistinct(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"count_distinct({column.name})", ('count_distinct', column.name))
    
    @staticmethod
    def when(condition, value):
        return WhenExpr(condition, value)
    
    @staticmethod
    def coalesce(*cols):
        return Column("coalesce", ('coalesce', [c.name if isinstance(c, Column) else c for c in cols]))
    
    @staticmethod
    def concat(*cols):
        return Column("concat", ('concat', [c.name if isinstance(c, Column) else c for c in cols]))
    
    @staticmethod
    def upper(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"upper({column.name})", ('upper', column.name))
    
    @staticmethod
    def lower(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"lower({column.name})", ('lower', column.name))
    
    @staticmethod
    def trim(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"trim({column.name})", ('trim', column.name))
    
    @staticmethod
    def length(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"length({column.name})", ('length', column.name))
    
    @staticmethod
    def year(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"year({column.name})", ('year', column.name))
    
    @staticmethod
    def month(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"month({column.name})", ('month', column.name))
    
    @staticmethod
    def dayofmonth(column):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"day({column.name})", ('day', column.name))
    
    @staticmethod
    def expr(sql_expr):
        return Column(sql_expr, ('expr', sql_expr))
    
    @staticmethod
    def round(column, scale=0):
        if isinstance(column, str):
            column = Column(column)
        return Column(f"round({column.name}, {scale})", ('round', column.name, scale))

F = functions

class WhenExpr:
    def __init__(self, condition, value):
        self.cases = [(condition, value)]
        self._otherwise = None
    
    def when(self, condition, value):
        self.cases.append((condition, value))
        return self
    
    def otherwise(self, value):
        self._otherwise = value
        return Column("when_expr", ('when', self.cases, self._otherwise))

# DataFrame class
class DataFrame:
    def __init__(self, pdf):
        self._pdf = pdf.copy()
    
    def __repr__(self):
        return f"DataFrame[{', '.join(self._pdf.columns)}]"
    
    @property
    def columns(self):
        return list(self._pdf.columns)
    
    def show(self, n=20, truncate=True):
        print(self._pdf.head(n).to_string())
    
    def collect(self):
        return [Row(**row) for row in self._pdf.to_dict('records')]
    
    def take(self, n):
        return [Row(**row) for row in self._pdf.head(n).to_dict('records')]
    
    def first(self):
        if len(self._pdf) == 0:
            return None
        return Row(**self._pdf.iloc[0].to_dict())
    
    def head(self, n=1):
        if n == 1:
            return self.first()
        return self.take(n)
    
    def count(self):
        return len(self._pdf)
    
    def distinct(self):
        return DataFrame(self._pdf.drop_duplicates())
    
    def dropDuplicates(self, subset=None):
        return DataFrame(self._pdf.drop_duplicates(subset=subset))
    
    def select(self, *cols):
        result = pd.DataFrame()
        for c in cols:
            if isinstance(c, str):
                result[c] = self._pdf[c]
            elif isinstance(c, Column):
                col_name = getattr(c, '_alias', c.name)
                if c.expr and isinstance(c.expr, tuple):
                    result[col_name] = self._evaluate_expr(c.expr)
                else:
                    result[col_name] = self._pdf[c.name]
        return DataFrame(result)
    
    def _evaluate_expr(self, expr):
        if not isinstance(expr, tuple):
            return self._pdf[expr] if expr in self._pdf.columns else expr
        
        op = expr[0]
        
        if op == 'lit':
            return expr[1]
        elif op == 'add':
            return self._pdf[expr[1]] + expr[2]
        elif op == 'sub':
            return self._pdf[expr[1]] - expr[2]
        elif op == 'mul':
            return self._pdf[expr[1]] * expr[2]
        elif op == 'div':
            return self._pdf[expr[1]] / expr[2]
        elif op == 'upper':
            return self._pdf[expr[1]].str.upper()
        elif op == 'lower':
            return self._pdf[expr[1]].str.lower()
        elif op == 'trim':
            return self._pdf[expr[1]].str.strip()
        elif op == 'length':
            return self._pdf[expr[1]].str.len()
        elif op == 'round':
            return self._pdf[expr[1]].round(expr[2])
        else:
            return self._pdf[expr[1]] if len(expr) > 1 and expr[1] in self._pdf.columns else None
    
    def _evaluate_condition(self, expr):
        if not isinstance(expr, tuple):
            if isinstance(expr, Column):
                return self._evaluate_condition(expr.expr)
            return expr
        
        op = expr[0]
        
        if op == 'eq':
            return self._pdf[expr[1]] == expr[2]
        elif op == 'ne':
            return self._pdf[expr[1]] != expr[2]
        elif op == 'gt':
            return self._pdf[expr[1]] > expr[2]
        elif op == 'ge':
            return self._pdf[expr[1]] >= expr[2]
        elif op == 'lt':
            return self._pdf[expr[1]] < expr[2]
        elif op == 'le':
            return self._pdf[expr[1]] <= expr[2]
        elif op == 'and':
            left = self._evaluate_condition(('col', expr[1]))
            right = self._evaluate_condition(('col', expr[2]))
            return left & right
        elif op == 'or':
            left = self._evaluate_condition(('col', expr[1]))
            right = self._evaluate_condition(('col', expr[2]))
            return left | right
        elif op == 'isnull':
            return self._pdf[expr[1]].isnull()
        elif op == 'isnotnull':
            return self._pdf[expr[1]].notna()
        elif op == 'isin':
            return self._pdf[expr[1]].isin(expr[2])
        elif op == 'contains':
            return self._pdf[expr[1]].str.contains(expr[2], na=False)
        elif op == 'startswith':
            return self._pdf[expr[1]].str.startswith(expr[2])
        elif op == 'endswith':
            return self._pdf[expr[1]].str.endswith(expr[2])
        
        return True
    
    def filter(self, condition):
        if isinstance(condition, Column):
            mask = self._evaluate_condition(condition.expr)
            return DataFrame(self._pdf[mask])
        return self
    
    def where(self, condition):
        return self.filter(condition)
    
    def withColumn(self, name, column):
        result = self._pdf.copy()
        if isinstance(column, Column):
            if column.expr and isinstance(column.expr, tuple):
                result[name] = self._evaluate_expr(column.expr)
            else:
                result[name] = self._pdf[column.name]
        else:
            result[name] = column
        return DataFrame(result)
    
    def withColumnRenamed(self, existing, new):
        return DataFrame(self._pdf.rename(columns={existing: new}))
    
    def drop(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.drop(columns=col_names, errors='ignore'))
    
    def orderBy(self, *cols, ascending=True):
        sort_cols = []
        sort_asc = []
        for c in cols:
            if isinstance(c, tuple):
                sort_cols.append(c[0])
                sort_asc.append(c[1])
            elif isinstance(c, Column):
                sort_cols.append(c.name)
                sort_asc.append(ascending)
            else:
                sort_cols.append(c)
                sort_asc.append(ascending)
        return DataFrame(self._pdf.sort_values(by=sort_cols, ascending=sort_asc))
    
    def sort(self, *cols, ascending=True):
        return self.orderBy(*cols, ascending=ascending)
    
    def limit(self, n):
        return DataFrame(self._pdf.head(n))
    
    def groupBy(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return GroupedData(self._pdf, col_names)
    
    def agg(self, *exprs):
        result = {}
        for expr in exprs:
            if isinstance(expr, Column) and expr.expr:
                op, col_name = expr.expr[0], expr.expr[1]
                agg_name = getattr(expr, '_alias', f"{op}({col_name})")
                if op == 'sum':
                    result[agg_name] = [self._pdf[col_name].sum()]
                elif op == 'avg':
                    result[agg_name] = [self._pdf[col_name].mean()]
                elif op == 'count':
                    result[agg_name] = [self._pdf[col_name].count()]
                elif op == 'max':
                    result[agg_name] = [self._pdf[col_name].max()]
                elif op == 'min':
                    result[agg_name] = [self._pdf[col_name].min()]
        return DataFrame(pd.DataFrame(result))
    
    def join(self, other, on=None, how='inner'):
        if isinstance(on, str):
            on = [on]
        elif isinstance(on, Column):
            on = [on.name]
        elif isinstance(on, list):
            on = [c if isinstance(c, str) else c.name for c in on]
        
        return DataFrame(self._pdf.merge(other._pdf, on=on, how=how))
    
    def union(self, other):
        return DataFrame(pd.concat([self._pdf, other._pdf], ignore_index=True))
    
    def unionAll(self, other):
        return self.union(other)
    
    def intersect(self, other):
        merged = self._pdf.merge(other._pdf, how='inner')
        return DataFrame(merged.drop_duplicates())
    
    def subtract(self, other):
        merged = self._pdf.merge(other._pdf, how='left', indicator=True)
        result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
        return DataFrame(result)
    
    def na(self):
        return DataFrameNaFunctions(self)
    
    def fillna(self, value, subset=None):
        if subset:
            result = self._pdf.copy()
            for col in subset:
                result[col] = result[col].fillna(value)
            return DataFrame(result)
        return DataFrame(self._pdf.fillna(value))
    
    def dropna(self, how='any', subset=None):
        return DataFrame(self._pdf.dropna(how=how, subset=subset))
    
    def toPandas(self):
        return self._pdf.copy()
    
    def printSchema(self):
        for col in self._pdf.columns:
            dtype = str(self._pdf[col].dtype)
            print(f" |-- {col}: {dtype} (nullable = true)")
    
    def describe(self, *cols):
        if cols:
            return DataFrame(self._pdf[list(cols)].describe().reset_index())
        return DataFrame(self._pdf.describe().reset_index())
    
    def cache(self):
        return self
    
    def persist(self):
        return self
    
    def unpersist(self):
        return self
    
    def repartition(self, numPartitions, *cols):
        return self
    
    def coalesce(self, numPartitions):
        return self

class DataFrameNaFunctions:
    def __init__(self, df):
        self._df = df
    
    def fill(self, value, subset=None):
        return self._df.fillna(value, subset)
    
    def drop(self, how='any', subset=None):
        return self._df.dropna(how, subset)

class GroupedData:
    def __init__(self, pdf, group_cols):
        self._pdf = pdf
        self._group_cols = group_cols
    
    def agg(self, *exprs):
        agg_dict = {}
        rename_dict = {}
        
        for expr in exprs:
            if isinstance(expr, Column) and expr.expr:
                op, col_name = expr.expr[0], expr.expr[1]
                agg_name = getattr(expr, '_alias', f"{op}({col_name})")
                
                if op == 'sum':
                    agg_dict[col_name] = 'sum'
                elif op == 'avg':
                    agg_dict[col_name] = 'mean'
                elif op == 'count':
                    agg_dict[col_name] = 'count'
                elif op == 'max':
                    agg_dict[col_name] = 'max'
                elif op == 'min':
                    agg_dict[col_name] = 'min'
                elif op == 'first':
                    agg_dict[col_name] = 'first'
                elif op == 'last':
                    agg_dict[col_name] = 'last'
                elif op == 'count_distinct':
                    agg_dict[col_name] = 'nunique'
                
                rename_dict[col_name] = agg_name
        
        result = self._pdf.groupby(self._group_cols).agg(agg_dict).reset_index()
        result = result.rename(columns=rename_dict)
        return DataFrame(result)
    
    def count(self):
        result = self._pdf.groupby(self._group_cols).size().reset_index(name='count')
        return DataFrame(result)
    
    def sum(self, *cols):
        agg_dict = {c: 'sum' for c in cols}
        result = self._pdf.groupby(self._group_cols).agg(agg_dict).reset_index()
        return DataFrame(result)
    
    def avg(self, *cols):
        agg_dict = {c: 'mean' for c in cols}
        result = self._pdf.groupby(self._group_cols).agg(agg_dict).reset_index()
        return DataFrame(result)
    
    def max(self, *cols):
        agg_dict = {c: 'max' for c in cols}
        result = self._pdf.groupby(self._group_cols).agg(agg_dict).reset_index()
        return DataFrame(result)
    
    def min(self, *cols):
        agg_dict = {c: 'min' for c in cols}
        result = self._pdf.groupby(self._group_cols).agg(agg_dict).reset_index()
        return DataFrame(result)

class Row:
    def __init__(self, **kwargs):
        self._data = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    def __repr__(self):
        return f"Row({', '.join(f'{k}={repr(v)}' for k, v in self._data.items())})"
    
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data[key]
    
    def asDict(self):
        return self._data.copy()

# Schema types
class StringType:
    pass

class IntegerType:
    pass

class LongType:
    pass

class DoubleType:
    pass

class FloatType:
    pass

class BooleanType:
    pass

class DateType:
    pass

class TimestampType:
    pass

class StructField:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

class StructType:
    def __init__(self, fields=None):
        self.fields = fields or []
    
    def add(self, name, dataType, nullable=True):
        self.fields.append(StructField(name, dataType, nullable))
        return self

print("PySpark shim loaded successfully!")
`;

// Koan definitions
const KOANS = [
  {
    id: 1,
    title: "Creating Your First DataFrame",
    category: "basics",
    difficulty: "beginner",
    description: "Learn how to create a DataFrame from a list of tuples. Replace ___ with the correct values.",
    hint: "Use spark.createDataFrame() with a list of tuples and column names.",
    starterCode: `# Create a DataFrame with three people's names and ages
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["name", "age"]

df = spark.createDataFrame(___, ___)

# Verify your DataFrame
result = df.count()
assert result == 3, f"Expected 3 rows, got {result}"

names = [row.name for row in df.collect()]
assert "Alice" in names, "Alice should be in the DataFrame"

print("✓ Koan 1 complete! You created your first DataFrame.")`,
    solution: `data = spark.createDataFrame(data, columns)`,
    expectedOutput: "✓ Koan 1 complete! You created your first DataFrame."
  },
  {
    id: 2,
    title: "Selecting Columns",
    category: "basics",
    difficulty: "beginner", 
    description: "Use the select() method to choose specific columns from a DataFrame.",
    hint: "The select() method takes column names as strings or Column objects.",
    starterCode: `data = [("Alice", 30, "Engineer"), ("Bob", 25, "Designer"), ("Charlie", 35, "Manager")]
df = spark.createDataFrame(data, ["name", "age", "role"])

# Select only the name and role columns
result_df = df.___("name", "role")

# Verify
cols = result_df.columns
assert cols == ["name", "role"], f"Expected ['name', 'role'], got {cols}"

print("✓ Koan 2 complete! You selected specific columns.")`,
    solution: `result_df = df.select("name", "role")`,
    expectedOutput: "✓ Koan 2 complete! You selected specific columns."
  },
  {
    id: 3,
    title: "Filtering Rows",
    category: "basics",
    difficulty: "beginner",
    description: "Use the filter() method with column conditions to select specific rows.",
    hint: "Use col('column_name') to reference columns, then use comparison operators.",
    starterCode: `data = [("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Diana", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# Filter to get only people older than 27
result_df = df.___(col("age") > ___)

# Verify
count = result_df.count()
assert count == 3, f"Expected 3 rows, got {count}"

names = [row.name for row in result_df.collect()]
assert "Bob" not in names, "Bob (age 25) should not be in results"

print("✓ Koan 3 complete! You filtered rows by condition.")`,
    solution: `result_df = df.filter(col("age") > 27)`,
    expectedOutput: "✓ Koan 3 complete! You filtered rows by condition."
  },
  {
    id: 4,
    title: "Adding New Columns",
    category: "transformations",
    difficulty: "beginner",
    description: "Use withColumn() to add a new column based on existing data.",
    hint: "withColumn takes a new column name and a Column expression.",
    starterCode: `data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Add a column 'age_in_months' that is age * 12
result_df = df.___("age_in_months", col("age") * ___)

# Verify
row = result_df.filter(col("name") == "Alice").first()
assert row.age_in_months == 360, f"Expected 360, got {row.age_in_months}"

print("✓ Koan 4 complete! You added a computed column.")`,
    solution: `result_df = df.withColumn("age_in_months", col("age") * 12)`,
    expectedOutput: "✓ Koan 4 complete! You added a computed column."
  },
  {
    id: 5,
    title: "GroupBy and Aggregation",
    category: "aggregations",
    difficulty: "intermediate",
    description: "Group data by a column and compute aggregate statistics.",
    hint: "Use groupBy() followed by agg() with functions like F.sum(), F.avg().",
    starterCode: `data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Engineering", "Charlie", 7000),
    ("Engineering", "Diana", 6500),
    ("Engineering", "Eve", 7500)
]
df = spark.createDataFrame(data, ["department", "name", "salary"])

# Group by department and calculate average salary
result_df = df.___("department").agg(
    F.___("salary").alias("avg_salary")
)

# Verify
eng_row = result_df.filter(col("department") == "Engineering").first()
assert eng_row.avg_salary == 7000, f"Expected 7000, got {eng_row.avg_salary}"

print("✓ Koan 5 complete! You mastered groupBy and aggregation.")`,
    solution: `result_df = df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))`,
    expectedOutput: "✓ Koan 5 complete! You mastered groupBy and aggregation."
  },
  {
    id: 6,
    title: "Sorting Data",
    category: "transformations",
    difficulty: "beginner",
    description: "Use orderBy() to sort DataFrame rows.",
    hint: "Use col('column').desc() for descending order.",
    starterCode: `data = [("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Diana", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# Sort by age in descending order (oldest first)
result_df = df.___(col("age").___())

# Verify  
first_row = result_df.first()
assert first_row.name == "Charlie", f"Expected Charlie (oldest), got {first_row.name}"

print("✓ Koan 6 complete! You sorted data.")`,
    solution: `result_df = df.orderBy(col("age").desc())`,
    expectedOutput: "✓ Koan 6 complete! You sorted data."
  },
  {
    id: 7,
    title: "Joining DataFrames",
    category: "joins",
    difficulty: "intermediate",
    description: "Join two DataFrames on a common column.",
    hint: "Use df1.join(df2, on='column_name', how='inner')",
    starterCode: `employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101)
], ["id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Sales"),
    (103, "Marketing")
], ["dept_id", "dept_name"])

# Join employees with departments
result_df = employees.___(departments, on="___", how="inner")

# Verify
count = result_df.count()
assert count == 3, f"Expected 3 rows, got {count}"
assert "dept_name" in result_df.columns, "Should have dept_name column"

print("✓ Koan 7 complete! You joined DataFrames.")`,
    solution: `result_df = employees.join(departments, on="dept_id", how="inner")`,
    expectedOutput: "✓ Koan 7 complete! You joined DataFrames."
  },
  {
    id: 8,
    title: "Handling Null Values",
    category: "data_cleaning",
    difficulty: "intermediate",
    description: "Use fillna() to replace null values in a DataFrame.",
    hint: "fillna() can take a value to replace nulls, optionally with a subset of columns.",
    starterCode: `data = [("Alice", 30), ("Bob", None), ("Charlie", 35), ("Diana", None)]
df = spark.createDataFrame(data, ["name", "age"])

# Replace null ages with 0
result_df = df.___(0, subset=["___"])

# Verify
bob_row = result_df.filter(col("name") == "Bob").first()
assert bob_row.age == 0, f"Expected 0 for Bob's age, got {bob_row.age}"

print("✓ Koan 8 complete! You handled null values.")`,
    solution: `result_df = df.fillna(0, subset=["age"])`,
    expectedOutput: "✓ Koan 8 complete! You handled null values."
  },
  {
    id: 9,
    title: "Renaming Columns",
    category: "transformations",
    difficulty: "beginner",
    description: "Use withColumnRenamed() to rename existing columns.",
    hint: "withColumnRenamed(existing_name, new_name)",
    starterCode: `data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Rename 'name' to 'employee_name' and 'age' to 'years_old'
result_df = df.withColumnRenamed("name", "___").___("age", "years_old")

# Verify
cols = result_df.columns
assert "employee_name" in cols, "Should have employee_name column"
assert "years_old" in cols, "Should have years_old column"
assert "name" not in cols, "Should not have name column"

print("✓ Koan 9 complete! You renamed columns.")`,
    solution: `result_df = df.withColumnRenamed("name", "employee_name").withColumnRenamed("age", "years_old")`,
    expectedOutput: "✓ Koan 9 complete! You renamed columns."
  },
  {
    id: 10,
    title: "Distinct Values",
    category: "transformations",
    difficulty: "beginner",
    description: "Use distinct() to remove duplicate rows.",
    hint: "distinct() removes complete duplicate rows across all columns.",
    starterCode: `data = [
    ("Engineering", "Python"),
    ("Engineering", "Java"),
    ("Engineering", "Python"),  # duplicate
    ("Sales", "Excel"),
    ("Sales", "Excel")  # duplicate
]
df = spark.createDataFrame(data, ["department", "skill"])

# Get distinct combinations
result_df = df.___()

# Verify
count = result_df.count()
assert count == 3, f"Expected 3 distinct rows, got {count}"

print("✓ Koan 10 complete! You found distinct values.")`,
    solution: `result_df = df.distinct()`,
    expectedOutput: "✓ Koan 10 complete! You found distinct values."
  },
  {
    id: 11,
    title: "Multiple Aggregations",
    category: "aggregations",
    difficulty: "intermediate",
    description: "Compute multiple aggregations in a single groupBy operation.",
    hint: "Pass multiple aggregate functions to agg(), each with an alias.",
    starterCode: `data = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Engineering", 80000),
    ("Charlie", "Sales", 60000),
    ("Diana", "Sales", 65000),
    ("Eve", "Sales", 70000)
]
df = spark.createDataFrame(data, ["name", "department", "salary"])

# Group by department and get count, sum, and average salary
result_df = df.groupBy("department").agg(
    F.count("salary").alias("employee_count"),
    F.___(___).alias("total_salary"),
    F.avg("salary").alias("avg_salary")
)

# Verify
eng = result_df.filter(col("department") == "Engineering").first()
assert eng.employee_count == 2, f"Expected 2 engineers, got {eng.employee_count}"
assert eng.total_salary == 155000, f"Expected 155000 total, got {eng.total_salary}"

print("✓ Koan 11 complete! You mastered multiple aggregations.")`,
    solution: `F.sum("salary")`,
    expectedOutput: "✓ Koan 11 complete! You mastered multiple aggregations."
  },
  {
    id: 12,
    title: "Chaining Transformations",
    category: "advanced",
    difficulty: "advanced",
    description: "Chain multiple DataFrame transformations together fluently.",
    hint: "PySpark transformations can be chained: df.filter().select().orderBy()",
    starterCode: `data = [
    ("Alice", "Engineering", 75000, 5),
    ("Bob", "Engineering", 80000, 3),
    ("Charlie", "Sales", 60000, 7),
    ("Diana", "Sales", 65000, 4),
    ("Eve", "Marketing", 55000, 2)
]
df = spark.createDataFrame(data, ["name", "department", "salary", "years"])

# Chain: filter salary > 60000, select name and salary, order by salary desc
result_df = (df
    .___(col("salary") > 60000)
    .___("name", "salary")
    .___(col("salary").desc())
)

# Verify
rows = result_df.collect()
assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
assert rows[0].name == "Bob", f"Expected Bob first (highest salary), got {rows[0].name}"

print("✓ Koan 12 complete! You mastered transformation chaining.")`,
    solution: `result_df = (df.filter(col("salary") > 60000).select("name", "salary").orderBy(col("salary").desc()))`,
    expectedOutput: "✓ Koan 12 complete! You mastered transformation chaining."
  }
];

// Category metadata
const CATEGORIES = {
  basics: { name: "Basics", color: "#22c55e", icon: "📗" },
  transformations: { name: "Transformations", color: "#3b82f6", icon: "🔄" },
  aggregations: { name: "Aggregations", color: "#f59e0b", icon: "📊" },
  joins: { name: "Joins", color: "#8b5cf6", icon: "🔗" },
  data_cleaning: { name: "Data Cleaning", color: "#ec4899", icon: "🧹" },
  advanced: { name: "Advanced", color: "#ef4444", icon: "🚀" }
};

// Main App Component
export default function PySparkKoans() {
  const [pyodideReady, setPyodideReady] = useState(false);
  const [pyodide, setPyodide] = useState(null);
  const [currentKoan, setCurrentKoan] = useState(0);
  const [code, setCode] = useState(KOANS[0].starterCode);
  const [output, setOutput] = useState('');
  const [status, setStatus] = useState('idle'); // idle, running, success, error
  const [completedKoans, setCompletedKoans] = useState(new Set());
  const [showHint, setShowHint] = useState(false);
  const [showSidebar, setShowSidebar] = useState(true);
  const textareaRef = useRef(null);

  // Load Pyodide
  useEffect(() => {
    const loadPyodide = async () => {
      setOutput('Loading Python runtime...');
      try {
        const pyodideModule = await window.loadPyodide({
          indexURL: "https://cdn.jsdelivr.net/pyodide/v0.24.1/full/"
        });
        
        setOutput('Loading pandas...');
        await pyodideModule.loadPackage('pandas');
        
        setOutput('Initializing PySpark shim...');
        await pyodideModule.runPythonAsync(PYSPARK_SHIM);
        
        setPyodide(pyodideModule);
        setPyodideReady(true);
        setOutput('Ready! Edit the code and click Run to test your solution.');
      } catch (error) {
        setOutput(`Error loading Pyodide: ${error.message}`);
        setStatus('error');
      }
    };

    if (typeof window !== 'undefined' && !window.loadPyodide) {
      const script = document.createElement('script');
      script.src = 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js';
      script.onload = loadPyodide;
      document.head.appendChild(script);
    } else if (window.loadPyodide) {
      loadPyodide();
    }
  }, []);

  // Update code when koan changes
  useEffect(() => {
    setCode(KOANS[currentKoan].starterCode);
    setShowHint(false);
    setStatus('idle');
    setOutput('Edit the code above and click Run to test your solution.');
  }, [currentKoan]);

  const runCode = async () => {
    if (!pyodideReady || !pyodide) {
      setOutput('Python runtime not ready yet. Please wait...');
      return;
    }

    setStatus('running');
    setOutput('Running...');

    try {
      // Capture stdout
      pyodide.runPython(`
import sys
from io import StringIO
sys.stdout = StringIO()
`);

      await pyodide.runPythonAsync(code);

      const stdout = pyodide.runPython('sys.stdout.getvalue()');
      
      // Reset stdout
      pyodide.runPython('sys.stdout = sys.__stdout__');

      setOutput(stdout || 'Code executed successfully (no output)');
      
      // Check if koan was completed
      if (stdout.includes('✓')) {
        setStatus('success');
        setCompletedKoans(prev => new Set([...prev, currentKoan]));
      } else {
        setStatus('idle');
      }
    } catch (error) {
      pyodide.runPython('sys.stdout = sys.__stdout__');
      setOutput(`Error: ${error.message}`);
      setStatus('error');
    }
  };

  const nextKoan = () => {
    if (currentKoan < KOANS.length - 1) {
      setCurrentKoan(currentKoan + 1);
    }
  };

  const prevKoan = () => {
    if (currentKoan > 0) {
      setCurrentKoan(currentKoan - 1);
    }
  };

  const koan = KOANS[currentKoan];
  const category = CATEGORIES[koan.category];
  const progress = (completedKoans.size / KOANS.length) * 100;

  return (
    <div style={{
      minHeight: '100vh',
      backgroundColor: '#0a0a0f',
      color: '#e4e4e7',
      fontFamily: "'JetBrains Mono', 'Fira Code', monospace"
    }}>
      {/* Header */}
      <header style={{
        background: 'linear-gradient(180deg, #18181b 0%, #0a0a0f 100%)',
        borderBottom: '1px solid #27272a',
        padding: '16px 24px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <div style={{
            fontSize: '28px',
            fontWeight: 700,
            background: 'linear-gradient(135deg, #ff6b35 0%, #f7931a 100%)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent'
          }}>
            🔥 PySpark Koans
          </div>
          <span style={{
            fontSize: '12px',
            padding: '4px 8px',
            background: '#27272a',
            borderRadius: '4px',
            color: '#a1a1aa'
          }}>
            Browser Edition
          </span>
        </div>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: '24px' }}>
          <div style={{ fontSize: '13px', color: '#71717a' }}>
            {completedKoans.size} / {KOANS.length} completed
          </div>
          <div style={{
            width: '200px',
            height: '6px',
            background: '#27272a',
            borderRadius: '3px',
            overflow: 'hidden'
          }}>
            <div style={{
              width: `${progress}%`,
              height: '100%',
              background: 'linear-gradient(90deg, #ff6b35, #f7931a)',
              transition: 'width 0.3s ease'
            }} />
          </div>
        </div>
      </header>

      <div style={{ display: 'flex', height: 'calc(100vh - 73px)' }}>
        {/* Sidebar */}
        {showSidebar && (
          <aside style={{
            width: '280px',
            background: '#111113',
            borderRight: '1px solid #27272a',
            overflowY: 'auto',
            padding: '16px'
          }}>
            <div style={{ fontSize: '11px', color: '#71717a', marginBottom: '12px', textTransform: 'uppercase', letterSpacing: '1px' }}>
              Koans
            </div>
            {KOANS.map((k, idx) => {
              const cat = CATEGORIES[k.category];
              const isCompleted = completedKoans.has(idx);
              const isCurrent = idx === currentKoan;
              
              return (
                <button
                  key={k.id}
                  onClick={() => setCurrentKoan(idx)}
                  style={{
                    width: '100%',
                    padding: '10px 12px',
                    marginBottom: '4px',
                    background: isCurrent ? '#1f1f23' : 'transparent',
                    border: isCurrent ? '1px solid #3f3f46' : '1px solid transparent',
                    borderRadius: '6px',
                    cursor: 'pointer',
                    textAlign: 'left',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '10px',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <span style={{
                    width: '20px',
                    height: '20px',
                    borderRadius: '50%',
                    background: isCompleted ? '#22c55e' : '#27272a',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '11px',
                    color: isCompleted ? '#fff' : '#71717a'
                  }}>
                    {isCompleted ? '✓' : idx + 1}
                  </span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{
                      fontSize: '13px',
                      color: isCurrent ? '#fff' : '#a1a1aa',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis'
                    }}>
                      {k.title}
                    </div>
                    <div style={{
                      fontSize: '10px',
                      color: cat.color,
                      marginTop: '2px'
                    }}>
                      {cat.icon} {cat.name}
                    </div>
                  </div>
                </button>
              );
            })}
          </aside>
        )}

        {/* Main content */}
        <main style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          {/* Koan header */}
          <div style={{
            padding: '20px 24px',
            background: '#111113',
            borderBottom: '1px solid #27272a'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '8px' }}>
              <span style={{
                padding: '4px 10px',
                background: category.color + '20',
                color: category.color,
                borderRadius: '4px',
                fontSize: '11px',
                fontWeight: 600
              }}>
                {category.icon} {category.name}
              </span>
              <span style={{
                padding: '4px 10px',
                background: '#27272a',
                color: '#a1a1aa',
                borderRadius: '4px',
                fontSize: '11px'
              }}>
                {koan.difficulty}
              </span>
            </div>
            <h1 style={{
              fontSize: '22px',
              fontWeight: 600,
              color: '#fff',
              margin: '0 0 8px 0'
            }}>
              {koan.id}. {koan.title}
            </h1>
            <p style={{
              fontSize: '14px',
              color: '#a1a1aa',
              margin: 0,
              lineHeight: 1.6
            }}>
              {koan.description}
            </p>
          </div>

          {/* Editor and output */}
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            {/* Code editor */}
            <div style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
              <textarea
                ref={textareaRef}
                value={code}
                onChange={(e) => setCode(e.target.value)}
                disabled={!pyodideReady}
                spellCheck={false}
                style={{
                  width: '100%',
                  height: '100%',
                  padding: '20px',
                  background: '#0a0a0f',
                  border: 'none',
                  color: '#e4e4e7',
                  fontSize: '14px',
                  fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                  lineHeight: 1.7,
                  resize: 'none',
                  outline: 'none',
                  tabSize: 4
                }}
              />
            </div>

            {/* Output panel */}
            <div style={{
              height: '180px',
              background: '#111113',
              borderTop: '1px solid #27272a',
              display: 'flex',
              flexDirection: 'column'
            }}>
              <div style={{
                padding: '8px 16px',
                borderBottom: '1px solid #27272a',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between'
              }}>
                <span style={{ fontSize: '11px', color: '#71717a', textTransform: 'uppercase', letterSpacing: '1px' }}>
                  Output
                </span>
                <div style={{
                  width: '8px',
                  height: '8px',
                  borderRadius: '50%',
                  background: status === 'success' ? '#22c55e' : 
                              status === 'error' ? '#ef4444' : 
                              status === 'running' ? '#f59e0b' : '#71717a'
                }} />
              </div>
              <pre style={{
                flex: 1,
                margin: 0,
                padding: '12px 16px',
                overflow: 'auto',
                fontSize: '13px',
                color: status === 'success' ? '#22c55e' : 
                       status === 'error' ? '#ef4444' : '#a1a1aa',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word'
              }}>
                {output}
              </pre>
            </div>
          </div>

          {/* Action bar */}
          <div style={{
            padding: '16px 24px',
            background: '#111113',
            borderTop: '1px solid #27272a',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between'
          }}>
            <div style={{ display: 'flex', gap: '8px' }}>
              <button
                onClick={prevKoan}
                disabled={currentKoan === 0}
                style={{
                  padding: '10px 16px',
                  background: '#27272a',
                  border: 'none',
                  borderRadius: '6px',
                  color: currentKoan === 0 ? '#52525b' : '#fff',
                  fontSize: '13px',
                  cursor: currentKoan === 0 ? 'not-allowed' : 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                ← Previous
              </button>
              <button
                onClick={nextKoan}
                disabled={currentKoan === KOANS.length - 1}
                style={{
                  padding: '10px 16px',
                  background: '#27272a',
                  border: 'none',
                  borderRadius: '6px',
                  color: currentKoan === KOANS.length - 1 ? '#52525b' : '#fff',
                  fontSize: '13px',
                  cursor: currentKoan === KOANS.length - 1 ? 'not-allowed' : 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                Next →
              </button>
            </div>

            <div style={{ display: 'flex', gap: '8px' }}>
              <button
                onClick={() => setShowHint(!showHint)}
                style={{
                  padding: '10px 16px',
                  background: showHint ? '#f59e0b20' : '#27272a',
                  border: showHint ? '1px solid #f59e0b' : '1px solid transparent',
                  borderRadius: '6px',
                  color: showHint ? '#f59e0b' : '#a1a1aa',
                  fontSize: '13px',
                  cursor: 'pointer'
                }}
              >
                💡 Hint
              </button>
              <button
                onClick={() => setCode(KOANS[currentKoan].starterCode)}
                style={{
                  padding: '10px 16px',
                  background: '#27272a',
                  border: 'none',
                  borderRadius: '6px',
                  color: '#a1a1aa',
                  fontSize: '13px',
                  cursor: 'pointer'
                }}
              >
                ↺ Reset
              </button>
              <button
                onClick={runCode}
                disabled={!pyodideReady || status === 'running'}
                style={{
                  padding: '10px 24px',
                  background: !pyodideReady ? '#27272a' : 'linear-gradient(135deg, #ff6b35 0%, #f7931a 100%)',
                  border: 'none',
                  borderRadius: '6px',
                  color: !pyodideReady ? '#52525b' : '#fff',
                  fontSize: '13px',
                  fontWeight: 600,
                  cursor: !pyodideReady ? 'not-allowed' : 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px'
                }}
              >
                {status === 'running' ? '⏳ Running...' : '▶ Run'}
              </button>
            </div>
          </div>

          {/* Hint overlay */}
          {showHint && (
            <div style={{
              position: 'absolute',
              bottom: '80px',
              right: '24px',
              maxWidth: '400px',
              padding: '16px',
              background: '#1c1c1f',
              border: '1px solid #f59e0b40',
              borderRadius: '8px',
              boxShadow: '0 4px 20px rgba(0,0,0,0.4)'
            }}>
              <div style={{ fontSize: '11px', color: '#f59e0b', marginBottom: '8px', fontWeight: 600 }}>
                💡 HINT
              </div>
              <div style={{ fontSize: '13px', color: '#e4e4e7', lineHeight: 1.6 }}>
                {koan.hint}
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
