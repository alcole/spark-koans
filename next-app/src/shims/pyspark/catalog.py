"""
Unity Catalog Module: Metadata Management
Handles three-level namespace (catalog.schema.table) and permissions
"""

import pandas as pd
from datetime import datetime


# ============ GLOBAL METADATA REGISTRY ============

# Catalog registry: catalog_name -> catalog metadata
_CATALOG_REGISTRY = {}

# Table data storage: 'catalog.schema.table' -> DataFrame
_TABLE_REGISTRY = {}

# Temp views (session-scoped): view_name -> {'df': DataFrame, 'definition': str}
_TEMP_VIEWS = {}

# ACL registry: 'catalog.schema.table' -> [grant records]
_ACL_REGISTRY = {}

# Current context
_CURRENT_CATALOG = 'spark_catalog'
_CURRENT_SCHEMA = 'default'


# ============ INITIALIZATION ============

def _initialize_default_catalog():
    """Set up spark_catalog with default schema"""
    global _CATALOG_REGISTRY
    _CATALOG_REGISTRY['spark_catalog'] = {
        'name': 'spark_catalog',
        'comment': 'Default Spark catalog',
        'properties': {},
        'created_at': datetime.now().isoformat(),
        'owner': 'spark',
        'schemas': {
            'default': {
                'name': 'default',
                'comment': 'Default schema',
                'created_at': datetime.now().isoformat(),
                'owner': 'spark',
                'tables': {}
            }
        }
    }


# Initialize on module load
_initialize_default_catalog()


# ============ CONTEXT FUNCTIONS ============

def current_catalog():
    """Return current catalog name"""
    return _CURRENT_CATALOG


def current_database():
    """Return current schema/database name"""
    return _CURRENT_SCHEMA


def set_current_catalog(catalog_name: str):
    """Set current catalog context"""
    global _CURRENT_CATALOG
    if catalog_name not in _CATALOG_REGISTRY:
        raise ValueError(f"Catalog '{catalog_name}' does not exist")
    _CURRENT_CATALOG = catalog_name


def set_current_schema(schema_name: str):
    """Set current schema context (can be catalog.schema or just schema)"""
    global _CURRENT_CATALOG, _CURRENT_SCHEMA

    if '.' in schema_name:
        # Format: catalog.schema
        parts = schema_name.split('.')
        catalog, schema = parts[0], parts[1]
        if catalog not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{catalog}' does not exist")
        if schema not in _CATALOG_REGISTRY[catalog]['schemas']:
            raise ValueError(f"Schema '{schema}' does not exist in catalog '{catalog}'")
        _CURRENT_CATALOG = catalog
        _CURRENT_SCHEMA = schema
    else:
        # Format: schema (use current catalog)
        if schema_name not in _CATALOG_REGISTRY[_CURRENT_CATALOG]['schemas']:
            raise ValueError(f"Schema '{schema_name}' does not exist in catalog '{_CURRENT_CATALOG}'")
        _CURRENT_SCHEMA = schema_name


# ============ CATALOG MANAGER ============

class CatalogManager:
    """Manages Unity Catalog metadata and operations"""

    @staticmethod
    def create_catalog(name: str, comment: str = None, properties: dict = None, if_not_exists: bool = False):
        """Create a new catalog"""
        global _CATALOG_REGISTRY

        if name in _CATALOG_REGISTRY:
            if if_not_exists:
                return  # Silently succeed
            raise ValueError(f"Catalog '{name}' already exists")

        _CATALOG_REGISTRY[name] = {
            'name': name,
            'comment': comment,
            'properties': properties or {},
            'created_at': datetime.now().isoformat(),
            'owner': 'user',
            'schemas': {}
        }

    @staticmethod
    def create_schema(full_path: str, comment: str = None, if_not_exists: bool = False):
        """
        Create schema within catalog.
        full_path can be 'catalog.schema' or just 'schema' (uses current catalog)
        """
        global _CATALOG_REGISTRY

        if '.' in full_path:
            catalog, schema = full_path.split('.', 1)
        else:
            catalog, schema = _CURRENT_CATALOG, full_path

        if catalog not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{catalog}' does not exist")

        if schema in _CATALOG_REGISTRY[catalog]['schemas']:
            if if_not_exists:
                return  # Silently succeed
            raise ValueError(f"Schema '{schema}' already exists in catalog '{catalog}'")

        _CATALOG_REGISTRY[catalog]['schemas'][schema] = {
            'name': schema,
            'comment': comment,
            'created_at': datetime.now().isoformat(),
            'owner': 'user',
            'tables': {}
        }

    @staticmethod
    def register_table(name: str, df, is_managed: bool = True, location: str = None,
                      table_type: str = 'MANAGED', comment: str = None, properties: dict = None):
        """
        Register table with metadata.
        name can be 'table', 'schema.table', or 'catalog.schema.table'
        """
        global _TABLE_REGISTRY, _CATALOG_REGISTRY

        catalog, schema, table = CatalogManager.resolve_name(name)
        qualified_name = CatalogManager.qualified_name(catalog, schema, table)

        # Ensure catalog and schema exist
        if catalog not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{catalog}' does not exist")
        if schema not in _CATALOG_REGISTRY[catalog]['schemas']:
            raise ValueError(f"Schema '{schema}' does not exist in catalog '{catalog}'")

        # Infer column metadata from DataFrame
        columns = CatalogManager._infer_columns(df)

        # Store table metadata
        _CATALOG_REGISTRY[catalog]['schemas'][schema]['tables'][table] = {
            'name': table,
            'type': table_type,  # MANAGED, EXTERNAL, VIEW
            'columns': columns,
            'location': location,
            'properties': properties or {},
            'comment': comment,
            'view_text': None,
            'created_at': datetime.now().isoformat()
        }

        # Store actual DataFrame
        _TABLE_REGISTRY[qualified_name] = df

    @staticmethod
    def _infer_columns(df):
        """Extract column metadata from DataFrame"""
        columns = []
        pdf = df._pdf if hasattr(df, '_pdf') else df
        for i, (col_name, dtype) in enumerate(pdf.dtypes.items()):
            columns.append({
                'name': col_name,
                'type': CatalogManager._pandas_to_spark_type(dtype),
                'ordinal': i,
                'comment': None
            })
        return columns

    @staticmethod
    def _pandas_to_spark_type(dtype) -> str:
        """Map pandas dtypes to Spark SQL types"""
        dtype_str = str(dtype)
        mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'int16': 'SMALLINT',
            'int8': 'TINYINT',
            'float64': 'DOUBLE',
            'float32': 'FLOAT',
            'object': 'STRING',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'category': 'STRING'
        }
        return mapping.get(dtype_str, 'STRING')

    @staticmethod
    def resolve_name(name: str) -> tuple:
        """
        Resolve table name to (catalog, schema, table).

        Examples:
        - 'mytable' -> (current_catalog, current_schema, 'mytable')
        - 'myschema.mytable' -> (current_catalog, 'myschema', 'mytable')
        - 'catalog.schema.table' -> ('catalog', 'schema', 'table')
        """
        parts = name.split('.')

        if len(parts) == 1:
            return (_CURRENT_CATALOG, _CURRENT_SCHEMA, parts[0])
        elif len(parts) == 2:
            return (_CURRENT_CATALOG, parts[0], parts[1])
        elif len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"Invalid table name: {name}")

    @staticmethod
    def qualified_name(catalog: str, schema: str, table: str) -> str:
        """Generate fully qualified name"""
        return f"{catalog}.{schema}.{table}"

    @staticmethod
    def get_table(name: str):
        """Retrieve table by name (resolves relative names)"""
        # Check temp views first
        if name in _TEMP_VIEWS:
            return _TEMP_VIEWS[name]['df']

        catalog, schema, table = CatalogManager.resolve_name(name)
        qualified_name = CatalogManager.qualified_name(catalog, schema, table)

        if qualified_name not in _TABLE_REGISTRY:
            raise ValueError(f"Table '{qualified_name}' not found")

        return _TABLE_REGISTRY[qualified_name]

    @staticmethod
    def get_table_metadata(name: str) -> dict:
        """Get table metadata"""
        catalog, schema, table = CatalogManager.resolve_name(name)

        if catalog not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{catalog}' does not exist")
        if schema not in _CATALOG_REGISTRY[catalog]['schemas']:
            raise ValueError(f"Schema '{schema}' does not exist")
        if table not in _CATALOG_REGISTRY[catalog]['schemas'][schema]['tables']:
            raise ValueError(f"Table '{table}' does not exist")

        return _CATALOG_REGISTRY[catalog]['schemas'][schema]['tables'][table]

    @staticmethod
    def get_catalogs() -> list:
        """Get list of catalog names"""
        return list(_CATALOG_REGISTRY.keys())

    @staticmethod
    def get_schemas(catalog: str = None) -> list:
        """Get list of schema names in catalog"""
        cat = catalog or _CURRENT_CATALOG
        if cat not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{cat}' does not exist")
        return list(_CATALOG_REGISTRY[cat]['schemas'].keys())

    @staticmethod
    def get_tables(schema_path: str = None) -> list:
        """Get list of table names in schema"""
        if schema_path:
            if '.' in schema_path:
                catalog, schema = schema_path.split('.')
            else:
                catalog, schema = _CURRENT_CATALOG, schema_path
        else:
            catalog, schema = _CURRENT_CATALOG, _CURRENT_SCHEMA

        if catalog not in _CATALOG_REGISTRY:
            raise ValueError(f"Catalog '{catalog}' does not exist")
        if schema not in _CATALOG_REGISTRY[catalog]['schemas']:
            raise ValueError(f"Schema '{schema}' does not exist")

        return list(_CATALOG_REGISTRY[catalog]['schemas'][schema]['tables'].keys())

    # ============ ACL METHODS ============

    @staticmethod
    def grant_privilege(object_type: str, object_name: str, privilege: str, principal: str):
        """Grant privilege on object to principal (stored but not enforced)"""
        global _ACL_REGISTRY

        if object_name not in _ACL_REGISTRY:
            _ACL_REGISTRY[object_name] = []

        # Check if grant already exists
        for grant in _ACL_REGISTRY[object_name]:
            if grant['principal'] == principal and grant['privilege'] == privilege:
                return  # Already exists

        _ACL_REGISTRY[object_name].append({
            'principal': principal,
            'privilege': privilege.upper(),
            'object_type': object_type.upper(),
            'granted_at': datetime.now().isoformat(),
            'granted_by': 'user'
        })

    @staticmethod
    def revoke_privilege(object_type: str, object_name: str, privilege: str, principal: str):
        """Revoke privilege on object from principal"""
        global _ACL_REGISTRY

        if object_name not in _ACL_REGISTRY:
            return  # Nothing to revoke

        _ACL_REGISTRY[object_name] = [
            grant for grant in _ACL_REGISTRY[object_name]
            if not (grant['principal'] == principal and grant['privilege'] == privilege.upper())
        ]

    @staticmethod
    def show_grants(object_name: str) -> list:
        """Return list of grants for object"""
        return _ACL_REGISTRY.get(object_name, [])

    # ============ TEMP VIEWS ============

    @staticmethod
    def create_temp_view(name: str, df, definition: str = None):
        """Create temporary view (session-scoped)"""
        global _TEMP_VIEWS
        _TEMP_VIEWS[name] = {
            'df': df,
            'definition': definition
        }

    @staticmethod
    def drop_temp_view(name: str):
        """Drop temporary view"""
        global _TEMP_VIEWS
        if name in _TEMP_VIEWS:
            del _TEMP_VIEWS[name]
