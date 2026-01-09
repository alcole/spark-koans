/**
 * Error Parser Utility
 * Parses Python errors and provides helpful, contextual error messages
 */

import { findClosestMatch } from './fuzzyMatch';
import { getMethodNames, getMethodDescription } from '../data/sparkMethodsDatabase';

/**
 * Extract error type from Python traceback
 * @param {string} errorMessage - Full error message
 * @returns {string|null} - Error type (e.g., 'AttributeError', 'SyntaxError')
 */
function extractErrorType(errorMessage) {
  const errorTypeMatch = errorMessage.match(/(\w+Error):/);
  return errorTypeMatch ? errorTypeMatch[1] : null;
}

/**
 * Extract the main error message (last line)
 * @param {string} errorMessage - Full error message
 * @returns {string} - Cleaned error message
 */
function extractErrorDetails(errorMessage) {
  const lines = errorMessage.split('\n');
  // Find the last non-empty line which usually contains the actual error
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i].trim();
    if (line) return line;
  }
  return errorMessage;
}

/**
 * Format AttributeError with method suggestions
 * @param {string} details - Error details
 * @param {object} koanContext - Koan context (optional)
 * @returns {object} - Formatted error object
 */
function formatAttributeError(details, koanContext) {
  // Match pattern: 'ObjectType' object has no attribute 'methodName'
  const match = details.match(/'(\w+)' object has no attribute '(\w+)'/);

  if (!match) {
    return formatGenericError(details);
  }

  const [, objectType, wrongMethod] = match;

  // Get valid methods for this object type
  const validMethods = getMethodNames(objectType);

  // Find closest match
  const suggestion = findClosestMatch(wrongMethod, validMethods, 0.4);

  // Get description for the suggested method
  const suggestionDescription = suggestion
    ? getMethodDescription(objectType, suggestion)
    : null;

  // Build the error object
  const errorObj = {
    type: 'method-typo',
    title: '❌ Method Not Found',
    message: `The method '${wrongMethod}' doesn't exist on ${objectType}.`,
    suggestion: null,
    hint: null,
    details: details,
    showTraceback: false
  };

  if (suggestion) {
    errorObj.suggestion = `💡 Did you mean '${suggestion}'?`;
    if (suggestionDescription) {
      errorObj.suggestion += `\n\nThe '${suggestion}' method ${suggestionDescription.toLowerCase()}`;
    }
  } else {
    errorObj.suggestion = `Check the spelling of your method name. Common ${objectType} methods include: ${validMethods.slice(0, 5).join(', ')}.`;
  }

  errorObj.hint = `Review the koan hints if you're unsure which method to use.`;

  // Special handling for common mistakes
  if (wrongMethod === 'except' && objectType === 'DataFrame') {
    errorObj.suggestion = `💡 Did you mean 'union' or 'subtract'?\n\n'except' is a Python keyword, not a Spark method. Use 'union' to combine rows or 'subtract' to get rows in one DataFrame but not another.`;
  }

  return errorObj;
}

/**
 * Format SyntaxError
 * @param {string} details - Error details
 * @param {object} koanContext - Koan context (optional)
 * @returns {object} - Formatted error object
 */
function formatSyntaxError(details, koanContext) {
  return {
    type: 'syntax',
    title: '❌ Syntax Error',
    message: 'Your code has a syntax problem that Python cannot parse.',
    details: cleanupSyntaxErrorMessage(details),
    suggestion: '💡 Common issues: missing parentheses, incorrect indentation, or typos in keywords.',
    hint: 'Try checking your code for matching brackets, quotes, and proper indentation.',
    showTraceback: false
  };
}

/**
 * Format NameError
 * @param {string} details - Error details
 * @param {object} koanContext - Koan context (optional)
 * @returns {object} - Formatted error object
 */
function formatNameError(details, koanContext) {
  const match = details.match(/name '(\w+)' is not defined/);
  const varName = match ? match[1] : 'variable';

  return {
    type: 'name-error',
    title: '❌ Name Not Defined',
    message: `The name '${varName}' is not defined.`,
    suggestion: `💡 Make sure you've defined '${varName}' before using it, or check for typos in the variable name.`,
    hint: 'Variables must be defined before they can be used.',
    details: details,
    showTraceback: false
  };
}

/**
 * Format TypeError
 * @param {string} details - Error details
 * @param {object} koanContext - Koan context (optional)
 * @returns {object} - Formatted error object
 */
function formatTypeError(details, koanContext) {
  return {
    type: 'type-error',
    title: '❌ Type Error',
    message: 'There\'s a type mismatch in your code.',
    details: details,
    suggestion: '💡 Check that you\'re using the right data types for operations and function arguments.',
    hint: 'Review the method signature and ensure you\'re passing the correct types.',
    showTraceback: false
  };
}

/**
 * Format AssertionError
 * @param {string} details - Error details
 * @param {object} koanContext - Koan context (optional)
 * @returns {object} - Formatted error object
 */
function formatAssertionError(details, koanContext) {
  // Clean up the assertion error message
  const cleanMessage = details.replace(/^AssertionError:\s*/, '');

  return {
    type: 'assertion-error',
    title: '❌ Assertion Failed',
    message: cleanMessage || 'The test assertion failed.',
    suggestion: '💡 Your code runs but doesn\'t produce the expected result.',
    hint: 'Review the koan description and hints to understand what the expected output should be.',
    details: details,
    showTraceback: false
  };
}

/**
 * Format generic error
 * @param {string} errorMessage - Error message
 * @returns {object} - Formatted error object
 */
function formatGenericError(errorMessage) {
  return {
    type: 'generic',
    title: '❌ Error',
    message: 'An error occurred while running your code.',
    details: errorMessage,
    suggestion: null,
    hint: 'Check your syntax and make sure you replaced all ___ placeholders.',
    showTraceback: true
  };
}

/**
 * Clean up syntax error message for better readability
 * @param {string} message - Raw syntax error message
 * @returns {string} - Cleaned message
 */
function cleanupSyntaxErrorMessage(message) {
  // Remove file paths and line numbers that aren't helpful
  return message
    .replace(/File ".*?", line \d+/g, '')
    .replace(/^\s*\^+\s*$/gm, '')
    .trim();
}

/**
 * Detect unreplaced placeholders in code
 * @param {string} code - User code
 * @returns {object|null} - Placeholder detection result
 */
export function detectUnreplacedPlaceholders(code) {
  const placeholderPattern = /___/g;
  const matches = code.match(placeholderPattern);

  if (matches && matches.length > 0) {
    return {
      hasPlaceholders: true,
      count: matches.length,
      error: {
        type: 'placeholder',
        title: '⚠️  Unreplaced Placeholders',
        message: `You have ${matches.length} unreplaced placeholder(s) (___) in your code.`,
        suggestion: '💡 Replace all ___ with the appropriate code before running.',
        hint: 'Placeholders are meant to be replaced with your solution.',
        details: null,
        showTraceback: false
      }
    };
  }

  return { hasPlaceholders: false };
}

/**
 * Main function to parse and format Python errors
 * @param {Error|string} error - JavaScript Error object or error string
 * @param {object} koanContext - Optional koan context
 * @returns {object} - Formatted error object
 */
export function parseAndFormatError(error, koanContext = null) {
  const errorMessage = error.message || error.toString();

  // Extract error type and details
  const errorType = extractErrorType(errorMessage);
  const errorDetails = extractErrorDetails(errorMessage);

  // Route to appropriate formatter
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
