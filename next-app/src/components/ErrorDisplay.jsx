/**
 * Error Display Component
 * Displays structured, helpful error messages instead of raw tracebacks
 */

export default function ErrorDisplay({ error }) {
  if (!error) return null;

  const { type, title, message, suggestion, hint, details, showTraceback } = error;

  return (
    <div className="bg-red-900/20 border border-red-800/50 rounded-lg overflow-hidden">
      {/* Error header */}
      <div className="px-4 py-3 bg-red-900/40 border-b border-red-800/50">
        <div className="text-red-400 font-semibold">{title}</div>
      </div>

      {/* Error content */}
      <div className="p-4 space-y-3">
        {/* Main error message */}
        <div className="text-red-200">{message}</div>

        {/* Suggestion section */}
        {suggestion && (
          <div className="bg-yellow-900/20 border border-yellow-700/50 rounded px-3 py-2">
            <div className="text-yellow-300 text-sm whitespace-pre-line">{suggestion}</div>
          </div>
        )}

        {/* Hint section */}
        {hint && (
          <div className="text-gray-400 text-sm italic">{hint}</div>
        )}

        {/* Technical details (collapsible) */}
        {details && showTraceback && (
          <details className="mt-2">
            <summary className="cursor-pointer text-sm text-gray-500 hover:text-gray-400 select-none">
              Show technical details
            </summary>
            <pre className="mt-2 text-xs text-gray-500 bg-gray-950 p-2 rounded overflow-x-auto whitespace-pre-wrap">
              {details}
            </pre>
          </details>
        )}
      </div>
    </div>
  );
}
