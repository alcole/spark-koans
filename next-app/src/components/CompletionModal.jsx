/**
 * Completion Modal - Shows when all koans are completed
 */

import Link from 'next/link';

export default function CompletionModal({ isOpen, onClose, totalKoans }) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 border border-orange-500 rounded-lg p-8 max-w-md w-full shadow-2xl">
        <div className="text-center">
          <div className="text-6xl mb-4">🎉</div>
          <h2 className="text-3xl font-bold text-orange-500 mb-4">
            Congratulations!
          </h2>
          <p className="text-gray-300 mb-2">
            You've completed all {totalKoans} PySpark Koans!
          </p>
          <p className="text-gray-400 mb-6">
            You've mastered PySpark fundamentals and Delta Lake operations.
          </p>

          <div className="space-y-3">
            <Link
              href="/certificate"
              className="block w-full bg-orange-600 hover:bg-orange-700 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
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
