/**
 * Hint Panel Component
 */

export default function HintPanel({ hint, currentHint, totalHints }) {
  return (
    <div className="bg-yellow-900/20 border border-yellow-800/50 rounded-lg p-4">
      <h3 className="text-sm font-medium text-yellow-500 mb-2">
        Hint {currentHint + 1}/{totalHints}
      </h3>
      <p className="text-yellow-200/80">{hint}</p>
    </div>
  );
}
