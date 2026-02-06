/**
 * Completion Modal - Shows when all koans in a track are completed
 */

import Link from 'next/link';

export default function CompletionModal({ isOpen, onClose, totalKoans, badgePage = '/badge', trackName = 'PySpark Koans' }) {
  if (!isOpen) return null;

  const isAdvanced = badgePage.includes('advanced');
  const borderColor = isAdvanced ? 'border-purple-500' : 'border-orange-500';
  const headingColor = isAdvanced ? 'text-purple-500' : 'text-orange-500';
  const btnColor = isAdvanced ? 'bg-purple-600 hover:bg-purple-700' : 'bg-orange-600 hover:bg-orange-700';

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
      <div className={`bg-gray-900 border ${borderColor} rounded-lg p-8 max-w-md w-full shadow-2xl`}>
        <div className="text-center">
          <div className="text-6xl mb-4">🎉</div>
          <h2 className={`text-3xl font-bold ${headingColor} mb-4`}>
            Congratulations!
          </h2>
          <p className="text-gray-300 mb-2">
            You&apos;ve completed all {totalKoans} {trackName} koans!
          </p>
          <p className="text-gray-400 mb-6">
            Claim your achievement badge and share it with the world.
          </p>

          <div className="space-y-3">
            <Link
              href={badgePage}
              className={`block w-full ${btnColor} text-white font-semibold px-6 py-3 rounded-lg transition-colors`}
            >
              View Your Achievement Badge 🎓
            </Link>
            <button
              onClick={onClose}
              className="block w-full bg-gray-800 hover:bg-gray-700 text-white px-6 py-3 rounded-lg transition-colors"
            >
              Continue Practicing
            </button>
          </div>

          <div className="mt-6 pt-6 border-t border-gray-800">
            <p className="text-sm text-gray-500">
              Share your achievement on social media!
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
