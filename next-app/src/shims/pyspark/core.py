"""
Core PySpark classes: Row, Column, DataFrame, SparkSession
This is the foundation that all other modules build upon.
"""

import pandas as pd
from typing import List, Any, Optional, Union

# ============ ROW CLASS ============
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


# ============ COLUMN CLASS ============
class Column:
    """PySpark-like Column class for expressions"""
    def __init__(self, name: str, expr: Optional[str] = None):
        self.name = name
        self.expr = expr or name
        self._alias = None

    # Comparison operators
    def __gt__(self, other):
        return Column(self.name, f"({self.expr}) > {repr(other)}")

    def __lt__(self, other):
        return Column(self.name, f"({self.expr}) < {repr(other)}")

    def __ge__(self, other):
        return Column(self.name, f"({self.expr}) >= {repr(other)}")

    def __le__(self, other):
        return Column(self.name, f"({self.expr}) <= {repr(other)}")

    def __eq__(self, other):
        if other is None:
            return Column(self.name, f"({self.expr}).isna()")
        return Column(self.name, f"({self.expr}) == {repr(other)}")

    def __ne__(self, other):
        if other is None:
            return Column(self.name, f"({self.expr}).notna()")
        return Column(self.name, f"({self.expr}) != {repr(other)}")

    # Arithmetic operators
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

    # Logical operators
    def __and__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) & ({other.expr})")
        return Column(self.name, f"({self.expr}) & {repr(other)}")

    def __or__(self, other):
        if isinstance(other, Column):
            return Column(self.name, f"({self.expr}) | ({other.expr})")
        return Column(self.name, f"({self.expr}) | {repr(other)}")

    # Column methods
    def alias(self, name):
        new_col = Column(self.name, self.expr)
        new_col._alias = name
        # Copy over any special attributes
        for attr in ['_agg_func', '_source_col', '_round_decimals', '_transform_func',
                     '_is_window_func', '_window', '_sort_desc']:
            if hasattr(self, attr):
                setattr(new_col, attr, getattr(self, attr))
        return new_col

    def cast(self, dataType):
        # For our simple shim, we'll just return a column with cast info
        new_col = Column(self.name, f"({self.expr}).astype('{dataType}')")
        return new_col

    def over(self, window):
        new_col = Column(self.name, self.expr)
        new_col._window = window
        new_col._is_window_func = True
        # Copy over aggregation info if present
        for attr in ['_agg_func', '_source_col', '_window_func', '_window_args']:
            if hasattr(self, attr):
                setattr(new_col, attr, getattr(self, attr))
        return new_col

    def isNull(self):
        return Column(self.name, f"({self.expr}).isna()")

    def isNotNull(self):
        return Column(self.name, f"({self.expr}).notna()")

    def desc(self):
        new_col = Column(self.name, self.expr)
        new_col._sort_desc = True
        return new_col

    def asc(self):
        new_col = Column(self.name, self.expr)
        new_col._sort_desc = False
        return new_col


# ============ GROUPED DATA ============
class GroupedData:
    """Result of DataFrame.groupBy()"""
    def __init__(self, df, group_cols):
        self._df = df
        self._group_cols = group_cols

    def agg(self, *exprs):
        pdf = self._df._pdf.copy()
        grouped = pdf.groupby(self._group_cols, as_index=False)

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

    def pivot(self, pivot_col):
        # Simple pivot implementation
        from .dataframe import PivotBuilder
        return PivotBuilder(self._df, self._group_cols, pivot_col)


# ============ DATAFRAME CLASS ============
class DataFrame:
    """PySpark-like DataFrame backed by pandas"""
    def __init__(self, pdf):
        self._pdf = pdf.copy()

    @property
    def columns(self):
        return list(self._pdf.columns)

    @property
    def write(self):
        from .io import DataFrameWriter
        return DataFrameWriter(self)

    def count(self):
        return len(self._pdf)

    def show(self, n=20, truncate=True):
        print(self._pdf.head(n).to_string())

    def collect(self):
        rows = []
        for _, row in self._pdf.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            rows.append(Row(**row_dict))
        return rows

    def first(self):
        if len(self._pdf) == 0:
            return None
        row = self._pdf.iloc[0]
        return Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})

    def select(self, *cols):
        result_cols = {}
        for c in cols:
            if isinstance(c, str):
                result_cols[c] = self._pdf[c]
            elif isinstance(c, Column):
                col_name = c._alias or c.name
                if hasattr(c, '_transform_func'):
                    result_cols[col_name] = c._transform_func(self._pdf)
                else:
                    try:
                        result_cols[col_name] = self._pdf.eval(c.expr)
                    except:
                        result_cols[col_name] = self._pdf[c.name]
        return DataFrame(pd.DataFrame(result_cols))

    def filter(self, condition):
        if isinstance(condition, Column):
            mask = self._pdf.eval(condition.expr)
            return DataFrame(self._pdf[mask].reset_index(drop=True))
        return self

    def where(self, condition):
        return self.filter(condition)

    def withColumn(self, name, col):
        pdf = self._pdf.copy()

        # Handle window functions
        if hasattr(col, '_is_window_func') and col._is_window_func:
            window = col._window
            if window._order_cols:
                sort_cols = [oc if isinstance(oc, str) else oc.name for oc in window._order_cols]
                pdf = pdf.sort_values(sort_cols).reset_index(drop=True)

            if hasattr(col, '_agg_func') and col._agg_func == 'sum':
                source_col = col._source_col
                if window._partition_cols:
                    pdf[name] = pdf.groupby(window._partition_cols)[source_col].cumsum()
                else:
                    pdf[name] = pdf[source_col].cumsum()
            return DataFrame(pdf)

        # Handle transform functions
        if hasattr(col, '_transform_func'):
            pdf[name] = col._transform_func(pdf)
            return DataFrame(pdf)

        # Handle expressions
        try:
            pdf[name] = pdf.eval(col.expr)
        except:
            if col.name in pdf.columns:
                pdf[name] = pdf[col.name]
        return DataFrame(pdf)

    def withColumnRenamed(self, existing, new):
        pdf = self._pdf.copy()
        pdf = pdf.rename(columns={existing: new})
        return DataFrame(pdf)

    def groupBy(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return GroupedData(self, col_names)

    def agg(self, *exprs):
        # Aggregation without grouping
        result = {}
        for expr in exprs:
            if hasattr(expr, '_agg_func'):
                col_name = expr._alias or expr.name
                source_col = expr._source_col
                func = expr._agg_func

                if func == 'avg':
                    result[col_name] = [self._pdf[source_col].mean()]
                elif func == 'sum':
                    result[col_name] = [self._pdf[source_col].sum()]
                elif func == 'count':
                    result[col_name] = [self._pdf[source_col].count()]
                elif func == 'min':
                    result[col_name] = [self._pdf[source_col].min()]
                elif func == 'max':
                    result[col_name] = [self._pdf[source_col].max()]

                if hasattr(expr, '_round_decimals'):
                    result[col_name] = [round(result[col_name][0], expr._round_decimals)]

        return DataFrame(pd.DataFrame(result))

    def join(self, other, on, how='inner'):
        result = self._pdf.merge(other._pdf, on=on, how=how)
        return DataFrame(result)

    def orderBy(self, *cols):
        sort_cols = []
        asc_list = []
        for c in cols:
            if isinstance(c, str):
                sort_cols.append(c)
                asc_list.append(True)
            else:
                sort_cols.append(c.name)
                asc_list.append(not getattr(c, '_sort_desc', False))
        return DataFrame(self._pdf.sort_values(sort_cols, ascending=asc_list).reset_index(drop=True))

    def drop(self, *cols):
        col_names = [c if isinstance(c, str) else c.name for c in cols]
        return DataFrame(self._pdf.drop(columns=col_names))

    def distinct(self):
        return DataFrame(self._pdf.drop_duplicates().reset_index(drop=True))

    def dropDuplicates(self, subset=None):
        return DataFrame(self._pdf.drop_duplicates(subset=subset).reset_index(drop=True))

    def limit(self, n):
        return DataFrame(self._pdf.head(n).copy())

    def union(self, other):
        result = pd.concat([self._pdf, other._pdf], ignore_index=True)
        return DataFrame(result)

    def fillna(self, value, subset=None):
        pdf = self._pdf.copy()
        if subset:
            pdf[subset] = pdf[subset].fillna(value)
        else:
            pdf = pdf.fillna(value)
        return DataFrame(pdf)

    def dropna(self, how='any', subset=None):
        pdf = self._pdf.copy()
        if subset:
            pdf = pdf.dropna(how=how, subset=subset)
        else:
            pdf = pdf.dropna(how=how)
        return DataFrame(pdf.reset_index(drop=True))

    def toPandas(self):
        return self._pdf.copy()


# ============ SPARK SESSION ============
class SparkSession:
    """Simplified SparkSession for browser environment"""
    @property
    def read(self):
        from .io import DataFrameReader
        return DataFrameReader(self)

    def createDataFrame(self, data, schema):
        pdf = pd.DataFrame(data, columns=schema)
        return DataFrame(pdf)

    # NOTE: sql() and table() methods are added by unity-catalog-shim.py
    # when it's loaded. This avoids import errors when Unity Catalog is not loaded.


# Create global spark session
spark = SparkSession()
