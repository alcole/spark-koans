/**
 * Landing Page / Dashboard - Track-focused design
 */

import Link from 'next/link';
import { getAllTracks, getTrackProgress } from '../src/tracks';
import { getKoanStats } from '../src/koans';
import useKoanProgress from '../src/hooks/useKoanProgress';

export default function Home() {
  const stats = getKoanStats();
  const tracks = getAllTracks();
  const { progress } = useKoanProgress();

  const completedCount = progress.size || 0;

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="max-w-6xl mx-auto px-6 py-12">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold text-orange-500 mb-4">
            PySpark Koans
          </h1>
          <p className="text-xl text-gray-400 mb-2">
            Master PySpark and Delta Lake through interactive exercises
          </p>
          <p className="text-gray-500">
            Choose your learning track and earn achievement badges
          </p>
        </div>

        {/* Overall Progress */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-6 mb-12">
          <div className="flex justify-between items-center">
            <div>
              <h2 className="text-xl font-semibold mb-1">Overall Progress</h2>
              <p className="text-gray-400 text-sm">Complete tracks to earn badges</p>
            </div>
            <div className="text-right">
              <div className="text-3xl font-bold text-orange-500">{completedCount} / {stats.total}</div>
              <div className="text-gray-400 text-sm">Koans Completed</div>
            </div>
          </div>
        </div>

        {/* Learning Tracks */}
        <div className="mb-12">
          <h2 className="text-3xl font-bold mb-6">Learning Tracks</h2>
          <div className="grid md:grid-cols-2 gap-8">
            {tracks.map(track => {
              const trackProgress = getTrackProgress(track.id, progress);
              const isComplete = trackProgress?.isComplete;
              const isStarted = (trackProgress?.completed || 0) > 0;

              return (
                <div
                  key={track.id}
                  className={`bg-gray-900 border-2 rounded-lg p-8 transition-all ${
                    isComplete
                      ? 'border-orange-500 shadow-lg shadow-orange-500/20'
                      : 'border-gray-800 hover:border-gray-700'
                  }`}
                >
                  {/* Track Header */}
                  <div className="flex items-start justify-between mb-6">
                    <div className="flex-1">
                      <h3 className="text-2xl font-bold mb-2">{track.name}</h3>
                      <p className="text-gray-400 text-sm mb-4">{track.description}</p>

                      {/* Track Metadata */}
                      <div className="flex gap-4 text-sm text-gray-500">
                        <div>
                          <span className="text-orange-500 font-semibold">{trackProgress?.total || 0}</span> koans
                        </div>
                        {isComplete && (
                          <div className="flex items-center gap-1 text-green-500">
                            <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                            </svg>
                            Completed
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Badge Preview */}
                    {isComplete && (
                      <div className="ml-4">
                        <img
                          src={track.badge.image}
                          alt={track.badge.title}
                          className="w-20 h-auto"
                        />
                      </div>
                    )}
                  </div>

                  {/* Progress Bar */}
                  <div className="mb-6">
                    <div className="flex justify-between text-sm mb-2">
                      <span className="text-gray-500">Progress</span>
                      <span className={isComplete ? 'text-orange-500 font-semibold' : 'text-gray-400'}>
                        {trackProgress?.completed || 0} / {trackProgress?.total || 0}
                      </span>
                    </div>
                    <div className="w-full bg-gray-800 rounded-full h-3">
                      <div
                        className={`h-3 rounded-full transition-all ${
                          isComplete ? 'bg-orange-500' : 'bg-orange-600'
                        }`}
                        style={{ width: `${trackProgress?.percentage || 0}%` }}
                      />
                    </div>
                  </div>

                  {/* Action Buttons */}
                  <div className="flex gap-3">
                    {isComplete ? (
                      <>
                        <Link
                          href={`/badge/${track.id}`}
                          className="flex-1 bg-orange-600 hover:bg-orange-700 text-white font-semibold px-6 py-3 rounded-lg text-center transition-colors"
                        >
                          View Badge 🎓
                        </Link>
                        <Link
                          href="/koans/1"
                          className="bg-gray-800 hover:bg-gray-700 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
                        >
                          Practice →
                        </Link>
                      </>
                    ) : (
                      <Link
                        href="/koans/1"
                        className="flex-1 bg-orange-600 hover:bg-orange-700 text-white font-semibold px-6 py-3 rounded-lg text-center transition-colors"
                      >
                        {isStarted ? 'Continue Track →' : 'Start Track →'}
                      </Link>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* View All Badges Link */}
        <div className="text-center">
          <Link
            href="/badges"
            className="inline-flex items-center gap-2 text-orange-500 hover:text-orange-400 transition-colors"
          >
            <span>View All Badges & Tracks</span>
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
            </svg>
          </Link>
        </div>
      </div>
    </div>
  );
}
