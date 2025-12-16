/**
 * Debug Utilities Page
 * For testing and development purposes
 */

import { useRouter } from 'next/router';
import { getAllKoanIds } from '../src/koans';
import useKoanProgress from '../src/hooks/useKoanProgress';

export default function Debug() {
  const router = useRouter();
  const { progress, resetProgress } = useKoanProgress();
  const allKoanIds = getAllKoanIds();

  const markAllComplete = () => {
    const allIds = getAllKoanIds();
    localStorage.setItem('pyspark-koans-progress', JSON.stringify(allIds));
    alert('All koans marked as complete! Refreshing...');
    window.location.reload();
  };

  const clearAllProgress = () => {
    resetProgress();
    alert('All progress cleared! Refreshing...');
    window.location.reload();
  };

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-2xl mx-auto">
        <h1 className="text-3xl font-bold text-orange-500 mb-8">Debug Utilities</h1>

        <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">Progress Status</h2>
          <div className="space-y-2 text-gray-400">
            <p>Current Progress: {progress.size} / {allKoanIds.length} koans</p>
            <p>Completed IDs: {Array.from(progress).sort((a, b) => a - b).join(', ') || 'None'}</p>
          </div>
        </div>

        <div className="space-y-4">
          <button
            onClick={markAllComplete}
            className="w-full bg-green-600 hover:bg-green-700 text-white font-semibold px-6 py-4 rounded-lg transition-colors"
          >
            ✓ Mark All Koans as Complete
          </button>

          <button
            onClick={clearAllProgress}
            className="w-full bg-red-600 hover:bg-red-700 text-white font-semibold px-6 py-4 rounded-lg transition-colors"
          >
            ✗ Clear All Progress
          </button>

          <button
            onClick={() => router.push('/')}
            className="w-full bg-gray-800 hover:bg-gray-700 text-white px-6 py-3 rounded-lg transition-colors"
          >
            ← Back to Home
          </button>
        </div>

        <div className="mt-8 p-4 bg-yellow-900/20 border border-yellow-800 rounded-lg">
          <p className="text-sm text-yellow-500">
            <strong>Note:</strong> These utilities modify localStorage directly. Use for testing purposes only.
          </p>
        </div>
      </div>
    </div>
  );
}
