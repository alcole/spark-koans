/**
 * Landing Page / Dashboard
 */

import Link from 'next/link';
import { getAllCategories, getKoanStats, getKoansByCategory } from '../src/koans';
import useKoanProgress from '../src/hooks/useKoanProgress';

export default function Home() {
  const stats = getKoanStats();
  const categories = getAllCategories();
  const { progress } = useKoanProgress();

  const completedCount = progress.size || 0;
  const progressPercentage = stats.total > 0
    ? Math.round((completedCount / stats.total) * 100)
    : 0;

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="max-w-4xl mx-auto px-6 py-12">
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold text-orange-500 mb-4">
            PySpark Koans
          </h1>
          <p className="text-xl text-gray-400">
            Learn PySpark and Delta Lake through interactive exercises
          </p>
        </div>

        <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 mb-8">
          <h2 className="text-2xl font-semibold mb-4">Progress</h2>
          <div className="grid grid-cols-3 gap-6 text-center">
            <div>
              <div className="text-3xl font-bold text-orange-500">{stats.total}</div>
              <div className="text-gray-400">Total Koans</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-green-500">{completedCount}</div>
              <div className="text-gray-400">Completed</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-blue-500">{progressPercentage}%</div>
              <div className="text-gray-400">Progress</div>
            </div>
          </div>
        </div>

        <div className="mb-8">
          <h2 className="text-2xl font-semibold mb-4">Categories</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {categories.map(category => {
              const categoryKoans = getKoansByCategory(category);
              const count = categoryKoans.length;
              const firstKoanId = categoryKoans.length > 0 ? categoryKoans[0].id : 1;

              return (
                <Link
                  key={category}
                  href={`/koans/${firstKoanId}`}
                  className="bg-gray-900 border border-gray-800 rounded-lg p-6 hover:border-orange-500 transition-colors"
                >
                  <h3 className="text-xl font-semibold mb-2">{category}</h3>
                  <p className="text-gray-400">{count} koans</p>
                </Link>
              );
            })}
          </div>
        </div>

        <div className="text-center space-y-4">
          {completedCount === stats.total && completedCount > 0 ? (
            <>
              <div className="text-2xl mb-4">🎉 Congratulations! All koans completed!</div>
              <Link
                href="/certificate"
                className="inline-block bg-green-600 hover:bg-green-700 text-white font-semibold px-8 py-4 rounded-lg text-lg transition-colors"
              >
                View Your Achievement Badge 🎓
              </Link>
              <div>
                <Link
                  href="/koans/1"
                  className="inline-block text-orange-500 hover:text-orange-400 underline"
                >
                  Continue practicing
                </Link>
              </div>
            </>
          ) : (
            <Link
              href="/koans/1"
              className="inline-block bg-orange-600 hover:bg-orange-700 text-white font-semibold px-8 py-4 rounded-lg text-lg transition-colors"
            >
              {completedCount > 0 ? 'Continue Learning →' : 'Start Learning →'}
            </Link>
          )}
        </div>
      </div>
    </div>
  );
}
