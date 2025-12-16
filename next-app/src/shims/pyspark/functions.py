"""
PySpark SQL functions (pyspark.sql.functions)
Includes: col, lit, aggregations, string functions, date functions, etc.
"""

import pandas as pd
from .core import Column


# ============ COLUMN REFERENCE ============
def col(name):
    """Reference a column by name"""
    return Column(name)


def lit(value):
    """Create a literal value column"""
    c = Column("_lit", repr(value))
    c._lit_value = value

    def transform(pdf):
        return pd.Series([value] * len(pdf))

    c._transform_func = transform
    return c


# ============ AGGREGATE FUNCTIONS ============
def avg(col_name):
    """Calculate average"""
    c = Column(col_name)
    c._agg_func = 'avg'
    c._source_col = col_name
    return c


def sum(col_name):
    """Calculate sum"""
    c = Column(col_name)
    c._agg_func = 'sum'
    c._source_col = col_name
    return c


def count(col_name):
    """Count values"""
    c = Column(col_name)
    c._agg_func = 'count'
    c._source_col = col_name
    return c


def min(col_name):
    """Find minimum value"""
    c = Column(col_name)
    c._agg_func = 'min'
    c._source_col = col_name
    return c


def max(col_name):
    """Find maximum value"""
    c = Column(col_name)
    c._agg_func = 'max'
    c._source_col = col_name
    return c


def round(col_expr, decimals=0):
    """Round numeric values"""
    new_col = Column(col_expr.name, col_expr.expr)
    if hasattr(col_expr, '_agg_func'):
        new_col._agg_func = col_expr._agg_func
        new_col._source_col = col_expr._source_col
    new_col._round_decimals = decimals
    if hasattr(col_expr, '_alias'):
        new_col._alias = col_expr._alias
    return new_col


# ============ STRING FUNCTIONS ============
def upper(col_expr):
    """Convert string to uppercase"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.upper()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.upper()")
    new_col._transform_func = transform
    return new_col


def lower(col_expr):
    """Convert string to lowercase"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.lower()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.lower()")
    new_col._transform_func = transform
    return new_col


def initcap(col_expr):
    """Convert string to title case"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.title()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.title()")
    new_col._transform_func = transform
    return new_col


def concat(*cols):
    """Concatenate multiple columns"""
    def transform(pdf):
        result = pdf[cols[0].name].astype(str)
        for c in cols[1:]:
            if hasattr(c, '_lit_value'):
                result = result + str(c._lit_value)
            else:
                result = result + pdf[c.name].astype(str)
        return result

    new_col = Column("concat_result")
    new_col._transform_func = transform
    return new_col


def concat_ws(sep, *cols):
    """Concatenate with separator"""
    def transform(pdf):
        string_cols = []
        for c in cols:
            if hasattr(c, 'name'):
                string_cols.append(pdf[c.name].astype(str))
            else:
                string_cols.append(pd.Series([str(c)] * len(pdf)))
        return pd.Series([sep.join(row) for row in zip(*string_cols)])

    new_col = Column("concat_ws_result")
    new_col._transform_func = transform
    return new_col


def length(col_expr):
    """Get string length"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.len()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.len()")
    new_col._transform_func = transform
    return new_col


def substring(col_expr, pos, length):
    """Extract substring (1-indexed!)"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        # PySpark substring is 1-indexed, Python is 0-indexed
        return pdf[col_expr.name].str.slice(pos - 1, pos - 1 + length)

    new_col = Column(col_expr.name, f"{col_expr.name}.str.slice({pos-1}, {pos-1+length})")
    new_col._transform_func = transform
    return new_col


def trim(col_expr):
    """Trim whitespace from both ends"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.strip()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.strip()")
    new_col._transform_func = transform
    return new_col


def ltrim(col_expr):
    """Trim whitespace from left"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.lstrip()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.lstrip()")
    new_col._transform_func = transform
    return new_col


def rtrim(col_expr):
    """Trim whitespace from right"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.rstrip()

    new_col = Column(col_expr.name, f"{col_expr.name}.str.rstrip()")
    new_col._transform_func = transform
    return new_col


def lpad(col_expr, length, pad):
    """Left pad to length"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.pad(length, side='left', fillchar=pad)

    new_col = Column(col_expr.name)
    new_col._transform_func = transform
    return new_col


def rpad(col_expr, length, pad):
    """Right pad to length"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.pad(length, side='right', fillchar=pad)

    new_col = Column(col_expr.name)
    new_col._transform_func = transform
    return new_col


# ============ CONDITIONAL FUNCTIONS ============
def when(condition, value):
    """Start a conditional expression"""
    return ConditionalColumn(condition, value)


class ConditionalColumn:
    """Builder for when/otherwise expressions"""
    def __init__(self, condition, value):
        self.conditions = [(condition, value)]
        self.otherwise_value = None

    def when(self, condition, value):
        self.conditions.append((condition, value))
        return self

    def otherwise(self, value):
        self.otherwise_value = value
        return self._build_column()

    def _build_column(self):
        def transform(pdf):
            result = pd.Series([self.otherwise_value] * len(pdf))
            for condition, value in reversed(self.conditions):
                mask = pdf.eval(condition.expr)
                result[mask] = value
            return result

        col = Column("when_result")
        col._transform_func = transform
        return col


# ============ ARRAY/COLLECTION FUNCTIONS ============
def explode(col_expr):
    """Explode an array column into multiple rows"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    # This is a special case that needs to be handled at DataFrame level
    # For now, we'll mark it and handle in select()
    new_col = Column(col_expr.name)
    new_col._is_explode = True
    return new_col


def split(col_expr, pattern):
    """Split string into array"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)

    def transform(pdf):
        return pdf[col_expr.name].str.split(pattern)

    new_col = Column(col_expr.name)
    new_col._transform_func = transform
    return new_col


# ============ NULL HANDLING ============
def isnull(col_expr):
    """Check if column is null"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)
    return col_expr.isNull()


def isnan(col_expr):
    """Check if column is NaN"""
    if isinstance(col_expr, str):
        col_expr = col(col_expr)
    return Column(col_expr.name, f"({col_expr.expr}).isna()")


def coalesce(*cols):
    """Return first non-null value"""
    def transform(pdf):
        result = None
        for c in cols:
            if result is None:
                if hasattr(c, 'name'):
                    result = pdf[c.name]
                else:
                    result = pd.Series([c] * len(pdf))
            else:
                mask = result.isna()
                if hasattr(c, 'name'):
                    result[mask] = pdf[c.name][mask]
                else:
                    result[mask] = c
        return result

    new_col = Column("coalesce_result")
    new_col._transform_func = transform
    return new_col
