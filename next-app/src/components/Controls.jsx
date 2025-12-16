/**
 * Control Buttons Component
 */

export default function Controls({
  onRun,
  onHint,
  onSolution,
  onReset,
  isLoading,
  showHints,
  currentHint,
  totalHints,
  showingSolution
}) {
  return (
    <div className="flex gap-2 flex-wrap">
      <button
        onClick={onRun}
        disabled={isLoading}
        className="px-4 py-2 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-700 disabled:text-gray-500 rounded-lg font-medium transition-colors"
      >
        {isLoading ? 'Loading...' : 'Run Code'}
      </button>
      <button
        onClick={onHint}
        disabled={currentHint >= totalHints - 1 && showHints}
        className="px-4 py-2 bg-gray-700 hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 rounded-lg transition-colors"
      >
        {showHints ? 'Next Hint' : 'Hint'}
      </button>
      <button
        onClick={onSolution}
        className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
      >
        {showingSolution ? 'Hide Solution' : 'Show Solution'}
      </button>
      <button
        onClick={onReset}
        className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
      >
        Reset
      </button>
    </div>
  );
}
