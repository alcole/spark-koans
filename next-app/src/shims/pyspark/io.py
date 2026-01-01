"""
I/O Module: DataFrameWriter and DataFrameReader
Handles saving and loading DataFrames to/from Unity Catalog
"""

import pandas as pd


class DataFrameWriter:
    """Writer interface for saving DataFrames to tables"""

    def __init__(self, df):
        self._df = df
        self._mode = "error"  # error, overwrite, append, ignore
        self._format = "delta"  # delta, parquet, etc. (mostly cosmetic)

    def mode(self, mode: str):
        """
        Set write mode:
        - error: Throw error if table exists (default)
        - overwrite: Overwrite existing table
        - append: Append to existing table
        - ignore: Silently ignore if table exists
        """
        self._mode = mode.lower()
        return self

    def format(self, source: str):
        """Set format (delta, parquet, etc.) - noop in shim"""
        self._format = source.lower()
        return self

    def saveAsTable(self, name: str):
        """
        Save DataFrame as table in Unity Catalog.
        Name can be:
        - 'table' -> saved to current catalog.schema.table
        - 'schema.table' -> saved to current catalog.schema.table
        - 'catalog.schema.table' -> saved with full path
        """
        # Import here to avoid circular dependency
        from .catalog import CatalogManager

        # Check if table exists
        try:
            existing_df = CatalogManager.get_table(name)
            table_exists = True
        except:
            table_exists = False

        # Handle mode logic
        if table_exists:
            if self._mode == "error":
                raise ValueError(f"Table {name} already exists. Use mode('overwrite') or mode('append')")
            elif self._mode == "ignore":
                return  # Do nothing
            elif self._mode == "append":
                # Append data to existing table
                import pandas as pd
                existing_pdf = existing_df._pdf
                combined_pdf = pd.concat([existing_pdf, self._df._pdf], ignore_index=True)
                from .core import DataFrame
                self._df = DataFrame(combined_pdf)
            # overwrite mode continues to register_table below

        # Register the table
        CatalogManager.register_table(name, self._df, is_managed=True)

    def save(self, path: str):
        """Alias for saveAsTable for compatibility"""
        return self.saveAsTable(path)


class DataFrameReader:
    """Reader interface for loading DataFrames from tables"""

    def __init__(self, spark_session):
        self._spark = spark_session
        self._format = "delta"

    def format(self, source: str):
        """Set format (delta, parquet, etc.) - noop in shim"""
        self._format = source.lower()
        return self

    def table(self, name: str):
        """
        Read table by name from Unity Catalog.
        Name can be:
        - 'table' -> reads from current catalog.schema.table
        - 'schema.table' -> reads from current catalog.schema.table
        - 'catalog.schema.table' -> reads with full path
        """
        from .catalog import CatalogManager
        return CatalogManager.get_table(name)

    def load(self, path: str):
        """Alias for table for compatibility"""
        return self.table(path)
