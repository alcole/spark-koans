# Koan Error Handling Improvement Plan

## Executive Summary

This document outlines a comprehensive plan to improve the error handling experience in the PySpark Koans learning platform. Currently, users encounter raw Python tracebacks when they make common mistakes (typos, syntax errors, wrong method names), which creates a poor learning experience. This plan proposes multiple improvements to provide contextual, helpful error messages that guide learners toward the correct solution.

---

## Problem Statement

### Current Behavior
When a learner makes an error in koan-028 (Union DataFrames):
- **Input mistake**: Using `df1.except(df2)` instead of `df1.union(df2)`
- **Current output**:
```
Error:
Traceback (most recent call last):
  File "/lib/python311.zip/_pyodide/_base.py", line 571, in eval_code_async
    await CodeRunner(
  File "/lib/python311.zip/_pyodide/_base.py", line 394, in run_async
    coroutine = eval(self.code, globals, locals)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<exec>", line 2, in <module>
AttributeError: 'SparkSession' object has no attribute 'creatDataFrame'

Tip: Check your syntax and make sure you replaced all ___ placeholders.
```

### Issues with Current Approach
1. **Overwhelming technical details**: Full Python stack traces are intimidating for beginners
2. **Generic tips**: The tip about syntax and placeholders doesn't help with method name typos
3. **No contextual help**: Doesn't suggest what the correct method might be
4. **Poor contrast with successful output**: Errors look technical; success looks friendly (with ✓ and 🎉)
5. **No partial credit**: If 2 out of 3 assertions pass, the user sees nothing about the successful parts

---

## Proposed Solutions

### Phase 1: Enhanced Error Parsing and Messaging (HIGH PRIORITY)

#### 1.1 Smart Error Detection and Classification

**Location**: `/next-app/pages/koans/[id].js` - `runCode()` function (lines 44-94)

**Implementation**: Create an error parsing utility that categorizes Python errors:

```javascript
// New file: /next-app/src/utils/errorParser.js

export function parseAndFormatError(error, koanContext) {
  const errorMessage = error.message || error.toString();

  // Extract error type and details from Python traceback
  const errorType = extractErrorType(errorMessage);
  const errorLine = extractErrorLine(errorMessage);
  const errorDetails = extractErrorDetails(errorMessage);

  switch (errorType) {
    case 'AttributeError':
      return formatAttributeError(errorDetails, koanContext);
    case 'SyntaxError':
      return formatSyntaxError(errorDetails, koanContext);
    case 'NameError':
      return formatNameError(errorDetails, koanContext);
    case 'TypeError':
      return formatTypeError(errorDetails, koanContext);
    case 'AssertionError':
      return formatAssertionError(errorDetails, koanContext);
    default:
      return formatGenericError(errorMessage);
  }
}
```

#### 1.2 Method Name Typo Detection

**Strategy**: Use fuzzy matching to suggest correct method names

```javascript
function formatAttributeError(details, koanContext) {
  // Extract the attribute that doesn't exist
  const match = details.match(/'(\w+)' object has no attribute '(\w+)'/);
  if (!match) return formatGenericError(details);

  const [_, objectType, wrongMethod] = match;

  // Common PySpark methods for fuzzy matching
  const sparkMethods = {
    'DataFrame': ['select', 'filter', 'groupBy', 'agg', 'join', 'union',
                  'drop', 'withColumn', 'show', 'count', 'collect'],
    'SparkSession': ['createDataFrame', 'read', 'sql', 'table'],
    'Column': ['alias', 'cast', 'contains', 'startswith', 'endswith']
  };

  const suggestion = findClosestMatch(wrongMethod, sparkMethods[objectType] || []);

  return {
    type: 'method-typo',
    title: '❌ Method Not Found',
    message: `The method '${wrongMethod}' doesn't exist on ${objectType}.`,
    suggestion: suggestion
      ? `Did you mean '${suggestion}'?`
      : `Check the spelling of your method name.`,
    hint: `Review the hints if you're unsure which method to use.`,
    showTraceback: false
  };
}
```

**Example output for `df1.except(df2)` mistake:**
```
❌ Method Not Found

The method 'except' doesn't exist on DataFrame.

💡 Did you mean 'union'?

The 'union' method combines two DataFrames with the same schema vertically.

Hint: Review the koan hints if you're unsure which method to use.
```

#### 1.3 Syntax Error Detection

```javascript
function formatSyntaxError(details, koanContext) {
  return {
    type: 'syntax',
    title: '❌ Syntax Error',
    message: 'Your code has a syntax problem that Python cannot parse.',
    details: cleanupSyntaxErrorMessage(details),
    suggestion: 'Common issues: missing parentheses, incorrect indentation, or typos in keywords.',
    hint: 'Try running the code step by step, or check the solution for the correct syntax.',
    showTraceback: false
  };
}
```

#### 1.4 Placeholder Detection

```javascript
function detectUnreplacedPlaceholders(code) {
  const placeholderPattern = /___/g;
  const matches = code.match(placeholderPattern);

  if (matches && matches.length > 0) {
    return {
      hasPlaceholders: true,
      count: matches.length,
      message: `⚠️  You have ${matches.length} unreplaced placeholder(s) (___) in your code.`
    };
  }
  return { hasPlaceholders: false };
}
```

---

### Phase 2: Enhanced Output Display (HIGH PRIORITY)

#### 2.1 Structured Error Component

**New file**: `/next-app/src/components/ErrorDisplay.jsx`

```jsx
export default function ErrorDisplay({ error }) {
  const { type, title, message, suggestion, hint, details, showTraceback } = error;

  return (
    <div className="bg-red-900/20 border border-red-800/50 rounded-lg overflow-hidden">
      {/* Error header */}
      <div className="px-4 py-3 bg-red-900/40 border-b border-red-800/50">
        <div className="text-red-400 font-semibold">{title}</div>
      </div>

      {/* Error content */}
      <div className="p-4 space-y-3">
        <div className="text-red-200">{message}</div>

        {suggestion && (
          <div className="bg-yellow-900/20 border border-yellow-700/50 rounded px-3 py-2">
            <div className="text-yellow-300 text-sm">{suggestion}</div>
          </div>
        )}

        {hint && (
          <div className="text-gray-400 text-sm italic">{hint}</div>
        )}

        {details && showTraceback && (
          <details className="mt-2">
            <summary className="cursor-pointer text-sm text-gray-500 hover:text-gray-400">
              Show technical details
            </summary>
            <pre className="mt-2 text-xs text-gray-500 bg-gray-950 p-2 rounded overflow-x-auto">
              {details}
            </pre>
          </details>
        )}
      </div>
    </div>
  );
}
```

#### 2.2 Update OutputPanel for Structured Errors

**File**: `/next-app/src/components/OutputPanel.jsx`

Modify to handle both string output and structured error objects:

```jsx
export default function OutputPanel({ output, error }) {
  if (error) {
    return <ErrorDisplay error={error} />;
  }

  // ... existing output rendering logic
}
```

---

### Phase 3: Partial Success Feedback (MEDIUM PRIORITY)

#### 3.1 Assertion Interception

**Problem**: If assertion #1 passes but assertion #2 fails, the user sees nothing about #1 succeeding.

**Solution**: Wrap each assertion in try-catch and collect all results:

**File**: `/next-app/pages/koans/[id].js` - Modify `runCode()` function

```javascript
// Inject assertion tracking wrapper
await pyodide.runPythonAsync(`
import sys
from io import StringIO

_assertion_results = []
_original_assert = __builtins__.assert

def _tracked_assert(condition, message=""):
    try:
        if not condition:
            raise AssertionError(message)
        _assertion_results.append(("pass", message))
    except AssertionError as e:
        _assertion_results.append(("fail", str(e)))
        raise
`);
```

Then after execution, check `_assertion_results` to show partial progress.

**Example output**:
```
✓ DataFrame created with correct row count
✓ DataFrame has correct number of columns
❌ Expected 4 rows, got 2

You're making progress! 2 out of 3 checks passed.
Hint: The union operation should combine rows from both DataFrames.
```

---

### Phase 4: Pre-execution Validation (LOW PRIORITY)

#### 4.1 Client-side Code Checks

Before sending code to Pyodide, run quick validation:

```javascript
function validateCodeBeforeRun(code, koan) {
  const issues = [];

  // Check for unreplaced placeholders
  if (code.includes('___')) {
    issues.push({
      type: 'warning',
      message: 'You still have ___ placeholders in your code. Replace them before running.'
    });
  }

  // Check for common typos based on the koan's solution
  const expectedMethods = extractExpectedMethods(koan.solution);
  const usedMethods = extractMethodCalls(code);

  for (const method of usedMethods) {
    if (!isValidSparkMethod(method) && !expectedMethods.includes(method)) {
      const suggestion = findClosestMatch(method, expectedMethods);
      if (suggestion) {
        issues.push({
          type: 'warning',
          message: `'${method}' doesn't look right. Did you mean '${suggestion}'?`
        });
      }
    }
  }

  return issues;
}
```

---

### Phase 5: Reference to Old Scala Koans Experience (INSPIRATION)

The old Scala koans had excellent error handling:

**Key features to emulate**:
1. **Contextual hints**: Errors included suggestions specific to the exercise
2. **Progressive disclosure**: Simple message first, technical details hidden
3. **Visual hierarchy**: Clear separation between "what went wrong" and "how to fix it"
4. **Partial success indicators**: Showed which assertions passed before the failure

**Example from Scala koans**:
```
Koan 15: About Lists - FAILED

Expected: List(1, 2, 3)
But got:  List(1, 3)

The flatMap operation should transform each element and flatten the results.
Try reviewing how flatMap differs from map.

[Show stack trace]
```

---

## Implementation Priorities

### Must-Have (Phase 1 & 2)
1. **Error parsing utility** (`/next-app/src/utils/errorParser.js`)
2. **Method typo detection** with fuzzy matching
3. **Structured error display component** (`/next-app/src/components/ErrorDisplay.jsx`)
4. **Update runCode() function** to use new error handling

**Estimated effort**: 1-2 days for experienced developer

### Should-Have (Phase 3)
5. **Partial success feedback** with assertion tracking
6. **Better assertion messages** in koan templates

**Estimated effort**: 1 day

### Nice-to-Have (Phase 4)
7. **Pre-execution validation**
8. **Common typo database** for auto-correction suggestions

**Estimated effort**: 0.5 day

---

## Technical Implementation Details

### Dependencies
- **No new dependencies required**
- Uses existing: React, Next.js, Pyodide
- May add: `fuzzysort` or similar library for fuzzy matching (optional, can implement simple Levenshtein distance)

### File Changes Summary

#### New Files
1. `/next-app/src/utils/errorParser.js` - Error parsing and formatting logic
2. `/next-app/src/utils/fuzzyMatch.js` - Fuzzy string matching for method suggestions
3. `/next-app/src/components/ErrorDisplay.jsx` - Structured error display component
4. `/next-app/src/data/sparkMethodsDatabase.js` - Database of valid PySpark methods for suggestions

#### Modified Files
1. `/next-app/pages/koans/[id].js`
   - Update `runCode()` function (lines 44-94)
   - Add error parsing before setting output
   - Add pre-validation checks

2. `/next-app/src/components/OutputPanel.jsx`
   - Support both string output and structured error objects
   - Update to use ErrorDisplay component when error object present

3. `/next-app/src/koans/pyspark/advanced/koan-028-union-dataframes.js` (and others)
   - Optional: Improve assertion messages to be more descriptive
   - Example: `assert result.count() == 4, f"Expected 4 rows (2 from each DataFrame), got {result.count()}"`

---

## Example User Flows

### Flow 1: Method Name Typo (creatDataFrame → createDataFrame)

**User enters**: `df = spark.creatDataFrame(data, columns)`

**Current experience**:
```
Error:
Traceback (most recent call last):
  ...
AttributeError: 'SparkSession' object has no attribute 'creatDataFrame'

Tip: Check your syntax and make sure you replaced all ___ placeholders.
```

**New experience**:
```
❌ Method Not Found

The method 'creatDataFrame' doesn't exist on SparkSession.

💡 Did you mean 'createDataFrame'?

The 'createDataFrame' method creates a DataFrame from Python data structures.

Hint: Review the koan hints if you're unsure which method to use.
```

### Flow 2: Wrong Method (except → union)

**User enters**: `result = df1.except(df2)`

**Current experience**:
```
Error:
AttributeError: 'DataFrame' object has no attribute 'except'

Tip: Check your syntax and make sure you replaced all ___ placeholders.
```

**New experience**:
```
❌ Method Not Found

The method 'except' doesn't exist on DataFrame.

💡 Did you mean 'union'?

The 'union' method combines two DataFrames with the same schema vertically.
Note: 'except' is a Python keyword, not a Spark method.

Hint: Read the koan description for clues about which operation to use.
```

### Flow 3: Partial Success (2 out of 3 assertions pass)

**User enters**: Correct code but with a logical error causing count to be wrong

**Current experience**:
```
Error:
AssertionError: Expected 4 rows, got 2
```

**New experience**:
```
✓ DataFrame union created successfully
✓ Contains names from both DataFrames
❌ Expected 4 rows, got 2

Progress: 2 out of 3 checks passed!

The issue: Your union operation isn't combining all rows.

💡 Hint: Check if you're filtering the data somewhere. Both DataFrames should contribute their rows.
```

---

## Testing Strategy

### Manual Testing Scenarios
1. **Typo in method name**: Test with common typos like `creatDataFrame`, `selct`, `groupby`
2. **Wrong method entirely**: Test with `except` instead of `union`, `add` instead of `withColumn`
3. **Syntax errors**: Test with missing parentheses, wrong indentation
4. **Unreplaced placeholders**: Test with `___` still in code
5. **Partial success**: Modify assertion to fail after some pass
6. **Correct solution**: Ensure success message still works

### Regression Testing
- Ensure all 39 koans (29 PySpark + 10 Delta Lake) still work correctly
- Test on both desktop and mobile viewports
- Test with slow network (Pyodide loading)

---

## Future Enhancements (Beyond Initial Scope)

1. **Interactive error recovery**: Click on error to auto-insert suggestion
2. **Code diff viewer**: Show difference between user code and solution
3. **Video hints**: Embedded video clips for complex concepts
4. **AI-powered hints**: Use LLM to generate contextual hints based on specific error
5. **Community solutions**: Show how other learners solved the same koan
6. **Error analytics**: Track common errors to improve koan design

---

## Success Metrics

After implementation, measure:
1. **Error clarity**: User survey asking "Did the error message help you fix the problem?"
2. **Completion rates**: Do more users complete koans without viewing solution?
3. **Hint usage**: Does hint usage decrease (indicating errors are self-explanatory)?
4. **Time to completion**: Does average time per koan decrease?

---

## Conclusion

This plan provides a comprehensive approach to improving the error handling experience in the PySpark Koans platform. By implementing **Phase 1 and 2** (error parsing and structured display), we can immediately provide a significantly better learning experience that matches the quality of the old Scala koans.

The modular approach allows for incremental implementation, with each phase providing independent value while building toward a complete solution.

**Recommended immediate action**: Implement Phase 1 and Phase 2 as they provide the most impact with reasonable effort.
