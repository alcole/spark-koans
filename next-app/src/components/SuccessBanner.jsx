/**
 * Success Banner Component
 * Appears after first-time koan completion to guide users to the next koan.
 */

export default function SuccessBanner({ nextKoanId, onNavigate }) {
  if (!nextKoanId) return null;

  return (
    <div className="bg-green-900/30 border border-green-700/50 rounded-lg p-4 flex items-center justify-between gap-4 animate-fade-in">
      <p className="text-green-300 text-sm font-medium">
        Nice work! Ready for the next challenge?
      </p>
      <button
        onClick={() => onNavigate(nextKoanId)}
        className="px-5 py-2 bg-orange-600 hover:bg-orange-500 text-white font-medium rounded-lg transition-colors whitespace-nowrap"
      >
        Continue to Next Koan →
      </button>
    </div>
  );
}
