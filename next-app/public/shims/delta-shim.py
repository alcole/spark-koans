"""
Delta Lake Shim for Browser Environment
Provides DeltaTable class and extends PySpark shim with Delta-compatible read/write
"""

import re
import pandas as pd
from datetime import datetime

# ============ GLOBAL DELTA STORAGE ============
# Stores Delta tables as: path -> { 'versions': [df0, df1, ...], 'history': [...] }
_delta_tables = {}


def _reset_delta_tables():
    """Clear all Delta tables - use at start of each koan"""
    global _delta_tables
    _delta_tables = {}


# ============ DATAFRAME WRITER ============
class DataFrameWriter:
    """Writer for saving DataFrames"""
    def __init__(self, df):
        self._df = df
        self._format = None
        self._mode = 'error'

    def format(self, fmt):
        self._format = fmt
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def save(self, path):
        global _delta_tables

        if self._format == 'delta':
            pdf = self._df._pdf.copy()

            if path in _delta_tables and self._mode == 'overwrite':
                # Add new version
                versions = _delta_tables[path]['versions']
                versions.append(pdf)
                _delta_tables[path]['history'].append({
                    'version': len(versions) - 1,
                    'operation': 'WRITE',
                    'timestamp': datetime.now().isoformat()
                })
            elif path not in _delta_tables:
                # Create new table
                _delta_tables[path] = {
                    'versions': [pdf],
                    'history': [{
                        'version': 0,
                        'operation': 'CREATE TABLE',
                        'timestamp': datetime.now().isoformat()
                    }]
                }
            else:
                raise ValueError(f"Table at {path} already exists. Use mode('overwrite').")
        else:
            raise NotImplementedError(f"Format {self._format} not supported")


# ============ DATAFRAME READER ============
class DataFrameReader:
    """Reader for loading DataFrames"""
    def __init__(self, spark):
        self._spark = spark
        self._format = None
        self._options = {}

    def format(self, fmt):
        self._format = fmt
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def load(self, path):
        global _delta_tables

        if self._format == 'delta':
            if path not in _delta_tables:
                raise ValueError(f"Delta table not found at {path}")

            versions = _delta_tables[path]['versions']

            # Handle time travel
            if 'versionAsOf' in self._options:
                version = int(self._options['versionAsOf'])
                if version < 0 or version >= len(versions):
                    raise ValueError(f"Version {version} not found")
                pdf = versions[version].copy()
            else:
                # Latest version
                pdf = versions[-1].copy()

            return DataFrame(pdf)
        else:
            raise NotImplementedError(f"Format {self._format} not supported")


def _validate_eval_condition(condition, pdf):
    """Validate a condition string before passing to pandas eval."""
    if not isinstance(condition, str):
        raise TypeError(f"Condition must be a string, got {type(condition).__name__}")
    if not re.search(r'[=!<>]=|[<>]', condition):
        raise ValueError(
            f"Invalid condition '{condition}': must contain a comparison operator (==, !=, <, >, <=, >=)"
        )
    _LITERALS = {'True', 'False', 'None', 'and', 'or', 'not', 'in'}
    identifiers = [t for t in re.findall(r'\b([a-zA-Z_]\w*)\b', condition) if t not in _LITERALS]
    if not any(col in pdf.columns for col in identifiers):
        raise ValueError(
            f"No valid column names in condition '{condition}'. "
            f"Available columns: {list(pdf.columns)}"
        )


# ============ MERGE BUILDER ============
class MergeBuilder:
    """Builder for MERGE operations"""
    def __init__(self, delta_table, source_df, condition):
        self._delta_table = delta_table
        self._source_df = source_df
        self._condition = condition
        self._when_matched = None
        self._when_matched_set = None
        self._when_not_matched = None

    def whenMatchedUpdateAll(self):
        self._when_matched = 'update_all'
        return self

    def whenMatchedUpdate(self, set=None):
        self._when_matched = 'update'
        self._when_matched_set = set
        return self

    def whenMatchedDelete(self):
        self._when_matched = 'delete'
        return self

    def whenNotMatchedInsertAll(self):
        self._when_not_matched = 'insert_all'
        return self

    def whenNotMatchedInsert(self, values=None):
        self._when_not_matched = 'insert'
        self._when_not_matched_values = values
        return self

    def execute(self):
        global _delta_tables

        path = self._delta_table._path
        target_pdf = _delta_tables[path]['versions'][-1].copy()
        source_pdf = self._source_df._pdf.copy()

        # Parse condition like "target.name = source.name"
        condition = self._condition
        match = re.match(r'^\s*target\.(\w+)\s*==?\s*source\.(\w+)\s*$', condition)
        if match:
            target_col = match.group(1)
            source_col = match.group(2)
        else:
            raise ValueError(
                f"Invalid merge condition: '{condition}'. "
                f"Expected format: 'target.<column> = source.<column>'"
            )

        # Find matching and non-matching rows
        matched_target_indices = []
        new_rows = []

        for _, source_row in source_pdf.iterrows():
            source_val = source_row[source_col]
            match_idx = target_pdf[target_pdf[target_col] == source_val].index.tolist()

            if match_idx:
                matched_target_indices.extend(match_idx)

                # Handle whenMatched
                if self._when_matched == 'update_all':
                    for idx in match_idx:
                        for col in source_pdf.columns:
                            if col in target_pdf.columns:
                                target_pdf.loc[idx, col] = source_row[col]
                elif self._when_matched == 'update' and self._when_matched_set:
                    for idx in match_idx:
                        for target_col_name, expr in self._when_matched_set.items():
                            if expr.startswith("source."):
                                src_col = expr.replace("source.", "")
                                target_pdf.loc[idx, target_col_name] = source_row[src_col]
                            elif expr.startswith("'") and expr.endswith("'"):
                                # Literal string value
                                target_pdf.loc[idx, target_col_name] = expr[1:-1]
                            else:
                                target_pdf.loc[idx, target_col_name] = expr
                elif self._when_matched == 'delete':
                    pass  # Will remove matched rows below
            else:
                # No match - handle whenNotMatched
                if self._when_not_matched == 'insert_all':
                    new_row = {}
                    for col in target_pdf.columns:
                        if col in source_row:
                            new_row[col] = source_row[col]
                        else:
                            new_row[col] = None
                    new_rows.append(new_row)

        # Remove matched rows if delete was specified
        if self._when_matched == 'delete':
            target_pdf = target_pdf.drop(matched_target_indices).reset_index(drop=True)

        # Add new rows
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            target_pdf = pd.concat([target_pdf, new_df], ignore_index=True)

        # Store new version
        _delta_tables[path]['versions'].append(target_pdf)
        _delta_tables[path]['history'].append({
            'version': len(_delta_tables[path]['versions']) - 1,
            'operation': 'MERGE',
            'timestamp': datetime.now().isoformat()
        })


# ============ DELTA TABLE CLASS ============
class DeltaTable:
    """Delta Lake table representation"""
    def __init__(self, path):
        self._path = path

    @staticmethod
    def forPath(spark, path):
        if path not in _delta_tables:
            raise ValueError(f"Delta table not found at {path}")
        return DeltaTable(path)

    @staticmethod
    def isDeltaTable(spark, path):
        return path in _delta_tables

    def toDF(self):
        if self._path not in _delta_tables:
            raise ValueError(f"Delta table not found at {self._path}")
        pdf = _delta_tables[self._path]['versions'][-1].copy()
        return DataFrame(pdf)

    def history(self, limit=None):
        if self._path not in _delta_tables:
            raise ValueError(f"Delta table not found at {self._path}")

        history = _delta_tables[self._path]['history']
        if limit:
            history = history[:limit]

        # Return as DataFrame
        return DataFrame(pd.DataFrame(history))

    def merge(self, source_df, condition):
        return MergeBuilder(self, source_df, condition)

    def delete(self, condition=None):
        global _delta_tables

        if self._path not in _delta_tables:
            raise ValueError(f"Delta table not found at {self._path}")

        pdf = _delta_tables[self._path]['versions'][-1].copy()

        if condition:
            _validate_eval_condition(condition, pdf)
            mask = pdf.eval(condition)
            pdf = pdf[~mask].reset_index(drop=True)
        else:
            pdf = pd.DataFrame(columns=pdf.columns)

        _delta_tables[self._path]['versions'].append(pdf)
        _delta_tables[self._path]['history'].append({
            'version': len(_delta_tables[self._path]['versions']) - 1,
            'operation': 'DELETE',
            'timestamp': datetime.now().isoformat()
        })

    def update(self, condition=None, set_values=None):
        global _delta_tables

        if self._path not in _delta_tables:
            raise ValueError(f"Delta table not found at {self._path}")

        pdf = _delta_tables[self._path]['versions'][-1].copy()

        if condition and set_values:
            _validate_eval_condition(condition, pdf)
            mask = pdf.eval(condition)
            for col_name, value in set_values.items():
                pdf.loc[mask, col_name] = value

        _delta_tables[self._path]['versions'].append(pdf)
        _delta_tables[self._path]['history'].append({
            'version': len(_delta_tables[self._path]['versions']) - 1,
            'operation': 'UPDATE',
            'timestamp': datetime.now().isoformat()
        })

    def vacuum(self, retention_hours=168):
        """Simulate vacuum - in browser, just record it happened"""
        _delta_tables[self._path]['history'].append({
            'version': len(_delta_tables[self._path]['versions']) - 1,
            'operation': 'VACUUM',
            'timestamp': datetime.now().isoformat()
        })
        return self

    def optimize(self):
        """Simulate optimize - in browser, just record it happened"""
        _delta_tables[self._path]['history'].append({
            'version': len(_delta_tables[self._path]['versions']) - 1,
            'operation': 'OPTIMIZE',
            'timestamp': datetime.now().isoformat()
        })
        return self


# ============ DELTA TABLE BUILDER ============
class DeltaTableBuilder:
    """Builder for creating Delta tables"""
    def __init__(self, spark):
        self._spark = spark
        self._path = None
        self._columns = []

    def tableName(self, name):
        self._path = f"/tables/{name}"
        return self

    def location(self, path):
        self._path = path
        return self

    def addColumn(self, name, dtype, nullable=True):
        self._columns.append({'name': name, 'type': dtype, 'nullable': nullable})
        return self

    def execute(self):
        global _delta_tables

        if not self._path:
            raise ValueError("Must specify tableName or location")

        # Create empty table with schema
        pdf = pd.DataFrame(columns=[c['name'] for c in self._columns])
        _delta_tables[self._path] = {
            'versions': [pdf],
            'history': [{
                'version': 0,
                'operation': 'CREATE TABLE',
                'timestamp': datetime.now().isoformat()
            }]
        }
        return DeltaTable(self._path)


# Add createIfNotExists and create to DeltaTable class
DeltaTable.createIfNotExists = staticmethod(lambda spark: DeltaTableBuilder(spark))
DeltaTable.create = staticmethod(lambda spark: DeltaTableBuilder(spark))


# ============ MODULE SETUP ============
# Create proper module structure for imports like: from delta.tables import DeltaTable
import sys
from types import ModuleType

delta_module = ModuleType('delta')
delta_tables_module = ModuleType('delta.tables')

delta_tables_module.DeltaTable = DeltaTable
delta_module.tables = delta_tables_module

sys.modules['delta'] = delta_module
sys.modules['delta.tables'] = delta_tables_module


# ============ MONKEY-PATCH PYSPARK SHIM ============
# Extend DataFrame with write property
@property
def _write_property(self):
    return DataFrameWriter(self)

DataFrame.write = _write_property


# Extend SparkSession with read property
@property
def _read_property(self):
    return DataFrameReader(self)

SparkSession.read = _read_property


print("✓ Delta Lake shim loaded successfully")
