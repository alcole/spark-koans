/**
 * Spark Methods Database
 * Contains common PySpark methods and their descriptions for error suggestions
 */

export const SPARK_METHODS = {
  // SparkSession methods
  'SparkSession': [
    { name: 'createDataFrame', description: 'Creates a DataFrame from Python data structures' },
    { name: 'read', description: 'Returns a DataFrameReader for reading data' },
    { name: 'sql', description: 'Executes a SQL query and returns the result as a DataFrame' },
    { name: 'table', description: 'Returns a DataFrame representing the specified table' },
    { name: 'range', description: 'Creates a DataFrame with a column containing a range of values' },
    { name: 'stop', description: 'Stops the SparkSession' },
  ],

  // DataFrame methods
  'DataFrame': [
    // Selection and filtering
    { name: 'select', description: 'Projects a set of expressions and returns a new DataFrame' },
    { name: 'filter', description: 'Filters rows using the given condition' },
    { name: 'where', description: 'Filters rows using the given condition (alias for filter)' },
    { name: 'drop', description: 'Removes columns from the DataFrame' },
    { name: 'dropDuplicates', description: 'Removes duplicate rows' },
    { name: 'distinct', description: 'Returns a new DataFrame with distinct rows' },

    // Transformations
    { name: 'withColumn', description: 'Adds or replaces a column in the DataFrame' },
    { name: 'withColumnRenamed', description: 'Renames a column in the DataFrame' },
    { name: 'withColumns', description: 'Adds or replaces multiple columns' },
    { name: 'cast', description: 'Converts a column to a different data type' },
    { name: 'alias', description: 'Renames a column' },

    // Aggregations
    { name: 'groupBy', description: 'Groups the DataFrame by specified columns' },
    { name: 'agg', description: 'Performs aggregation on the DataFrame' },
    { name: 'count', description: 'Returns the number of rows' },
    { name: 'sum', description: 'Computes the sum of numeric columns' },
    { name: 'avg', description: 'Computes the average of numeric columns' },
    { name: 'mean', description: 'Computes the mean of numeric columns' },
    { name: 'max', description: 'Computes the maximum value' },
    { name: 'min', description: 'Computes the minimum value' },

    // Joins and set operations
    { name: 'join', description: 'Joins two DataFrames together' },
    { name: 'union', description: 'Combines two DataFrames with the same schema vertically' },
    { name: 'unionAll', description: 'Combines two DataFrames (deprecated, use union)' },
    { name: 'unionByName', description: 'Combines two DataFrames by column names' },
    { name: 'intersect', description: 'Returns rows that exist in both DataFrames' },
    { name: 'subtract', description: 'Returns rows in this DataFrame but not in another' },
    { name: 'exceptAll', description: 'Returns rows in this DataFrame but not in another (with duplicates)' },

    // Ordering
    { name: 'orderBy', description: 'Sorts the DataFrame by specified columns' },
    { name: 'sort', description: 'Sorts the DataFrame by specified columns (alias for orderBy)' },

    // Actions
    { name: 'show', description: 'Displays the DataFrame in a tabular format' },
    { name: 'collect', description: 'Returns all rows as a list' },
    { name: 'take', description: 'Returns the first n rows' },
    { name: 'first', description: 'Returns the first row' },
    { name: 'head', description: 'Returns the first n rows' },

    // I/O operations
    { name: 'write', description: 'Returns a DataFrameWriter for writing data' },
    { name: 'printSchema', description: 'Prints the schema to the console' },
    { name: 'schema', description: 'Returns the schema of the DataFrame' },
    { name: 'columns', description: 'Returns all column names as a list' },
    { name: 'dtypes', description: 'Returns column names and types' },
  ],

  // Column methods
  'Column': [
    { name: 'alias', description: 'Renames the column' },
    { name: 'cast', description: 'Converts the column to a different data type' },
    { name: 'contains', description: 'Checks if the string column contains the specified substring' },
    { name: 'startswith', description: 'Checks if the string column starts with the specified prefix' },
    { name: 'endswith', description: 'Checks if the string column ends with the specified suffix' },
    { name: 'like', description: 'SQL LIKE expression for pattern matching' },
    { name: 'rlike', description: 'Regular expression pattern matching' },
    { name: 'isin', description: 'Checks if the column value is in a list of values' },
    { name: 'isNull', description: 'Checks if the column value is null' },
    { name: 'isNotNull', description: 'Checks if the column value is not null' },
    { name: 'between', description: 'Checks if the column value is between two values' },
    { name: 'when', description: 'Conditional expression (like SQL CASE WHEN)' },
    { name: 'otherwise', description: 'Default value for when() expression' },
    { name: 'substr', description: 'Extracts a substring from the column' },
    { name: 'lower', description: 'Converts string to lowercase' },
    { name: 'upper', description: 'Converts string to uppercase' },
  ],

  // GroupedData methods
  'GroupedData': [
    { name: 'agg', description: 'Performs aggregation using specified functions' },
    { name: 'count', description: 'Counts the number of records in each group' },
    { name: 'sum', description: 'Computes the sum for each numeric column in each group' },
    { name: 'avg', description: 'Computes the average for each numeric column in each group' },
    { name: 'mean', description: 'Computes the mean for each numeric column in each group' },
    { name: 'max', description: 'Computes the maximum value for each column in each group' },
    { name: 'min', description: 'Computes the minimum value for each column in each group' },
  ],
};

/**
 * Get method names for a specific object type
 * @param {string} objectType - The type of object (e.g., 'DataFrame', 'SparkSession')
 * @returns {string[]} - Array of method names
 */
export function getMethodNames(objectType) {
  const methods = SPARK_METHODS[objectType];
  return methods ? methods.map(m => m.name) : [];
}

/**
 * Get method description
 * @param {string} objectType - The type of object
 * @param {string} methodName - The name of the method
 * @returns {string|null} - Method description or null if not found
 */
export function getMethodDescription(objectType, methodName) {
  const methods = SPARK_METHODS[objectType];
  if (!methods) return null;

  const method = methods.find(m => m.name === methodName);
  return method ? method.description : null;
}

/**
 * Get all method names across all types (for general searching)
 * @returns {string[]} - Array of all method names
 */
export function getAllMethodNames() {
  const allMethods = [];
  for (const type in SPARK_METHODS) {
    allMethods.push(...SPARK_METHODS[type].map(m => m.name));
  }
  return [...new Set(allMethods)]; // Remove duplicates
}
