/**
 * Koan Header Component
 */

export default function KoanHeader({ koan, isComplete }) {
  return (
    <div className="mb-6">
      <div className="flex items-center gap-2 mb-2">
        <span className={`text-xs px-2 py-1 rounded ${
          koan.category === 'Delta Lake'
            ? 'bg-blue-900 text-blue-300'
            : 'bg-gray-800 text-gray-400'
        }`}>
          {koan.category}
        </span>
        <span className="text-xs text-gray-600">Koan {koan.id}</span>
        {isComplete && (
          <span className="text-xs px-2 py-1 rounded bg-green-900 text-green-300">
            ✓ Completed
          </span>
        )}
      </div>
      <h2 className="text-2xl font-semibold text-white mb-2">{koan.title}</h2>
      <p className="text-gray-400">{koan.description}</p>
    </div>
  );
}
