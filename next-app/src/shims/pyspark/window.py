"""
Window functions for PySpark
"""

from .core import Column


class WindowSpec:
    """Window specification for window functions"""
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
        new_spec._partition_cols = [c if isinstance(c, str) else c.name for c in cols]
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
    """Window functions factory"""
    unboundedPreceding = "unboundedPreceding"
    unboundedFollowing = "unboundedFollowing"
    currentRow = "currentRow"

    @staticmethod
    def partitionBy(*cols):
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols):
        return WindowSpec().orderBy(*cols)


# ============ WINDOW FUNCTIONS ============
def row_number():
    """Assign sequential row numbers within partitions"""
    col = Column("row_number")
    col._window_func = 'row_number'
    col._is_window_func = True
    return col


def rank():
    """Assign ranks with gaps"""
    col = Column("rank")
    col._window_func = 'rank'
    col._is_window_func = True
    return col


def dense_rank():
    """Assign ranks without gaps"""
    col = Column("dense_rank")
    col._window_func = 'dense_rank'
    col._is_window_func = True
    return col


def lag(col_name, offset=1, default=None):
    """Access previous row value"""
    col = Column(col_name)
    col._window_func = 'lag'
    col._window_args = {'offset': offset, 'default': default}
    col._is_window_func = True
    return col


def lead(col_name, offset=1, default=None):
    """Access next row value"""
    col = Column(col_name)
    col._window_func = 'lead'
    col._window_args = {'offset': offset, 'default': default}
    col._is_window_func = True
    return col
