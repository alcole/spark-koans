/**
 * Badges Page - Shows all available learning tracks and earned badges
 */

import { useState, useEffect } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import { getAllTracks, getTrackProgress } from '../src/tracks';
import useKoanProgress from '../src/hooks/useKoanProgress';

export default function Badges() {
  const { progress } = useKoanProgress();
  const tracks = getAllTracks();

  return (
    <>
      <Head>
        <title>Achievement Badges - PySpark Koans</title>
        <meta name="description" content="View all available PySpark learning tracks and achievement badges" />
      </Head>

      <div className="min-h-screen bg-gray-950 text-gray-100 py-12 px-4">
        <div className="max-w-6xl mx-auto">
          <Link href="/" className="text-orange-500 hover:text-orange-400 mb-8 inline-block">
            ← Back to Home
          </Link>

          <div className="mb-12">
            <h1 className="text-4xl font-bold mb-4">Achievement Badges</h1>
            <p className="text-gray-400">
              Complete learning tracks to earn achievement badges you can share on LinkedIn and Twitter.
            </p>
          </div>

          {/* Track Cards */}
          <div className="grid md:grid-cols-2 gap-8">
            {tracks.map(track => {
              const trackProgress = getTrackProgress(track.id, progress);
              const isComplete = trackProgress?.isComplete;

              return (
                <div
                  key={track.id}
                  className={`bg-gray-900 border-2 rounded-lg p-8 transition-all ${
                    isComplete
                      ? 'border-orange-500 shadow-lg shadow-orange-500/20'
                      : 'border-gray-800 hover:border-gray-700'
                  }`}
                >
                  {/* Badge Preview */}
                  <div className="flex justify-center mb-6">
                    {isComplete ? (
                      <img
                        src={track.badge.image}
                        alt={track.badge.title}
                        className="w-48 h-auto drop-shadow-xl"
                      />
                    ) : (
                      <div className="w-48 h-60 bg-gray-800 rounded-lg flex items-center justify-center border-2 border-dashed border-gray-700">
                        <div className="text-center">
                          <div className="text-6xl mb-2 opacity-30">🔒</div>
                          <div className="text-sm text-gray-600">Locked</div>
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Track Info */}
                  <div className="text-center mb-6">
                    <h2 className="text-2xl font-bold mb-2">{track.name}</h2>
                    <p className="text-gray-400 text-sm mb-4">{track.description}</p>

                    {/* Progress Bar */}
                    <div className="mb-4">
                      <div className="flex justify-between text-sm mb-1">
                        <span className="text-gray-500">Progress</span>
                        <span className={isComplete ? 'text-orange-500 font-semibold' : 'text-gray-400'}>
                          {trackProgress?.completed || 0} / {trackProgress?.total || 0} koans
                        </span>
                      </div>
                      <div className="w-full bg-gray-800 rounded-full h-2">
                        <div
                          className={`h-2 rounded-full transition-all ${
                            isComplete ? 'bg-orange-500' : 'bg-orange-600'
                          }`}
                          style={{ width: `${trackProgress?.percentage || 0}%` }}
                        />
                      </div>
                    </div>
                  </div>

                  {/* Action Button */}
                  {isComplete ? (
                    <div className="space-y-2">
                      <Link
                        href={`/badge/${track.id}`}
                        className="block w-full bg-orange-600 hover:bg-orange-700 text-white font-semibold px-6 py-3 rounded-lg text-center transition-colors"
                      >
                        View Badge 🎓
                      </Link>
                      <div className="text-center">
                        <span className="text-green-500 text-sm">✓ Completed</span>
                      </div>
                    </div>
                  ) : (
                    <Link
                      href="/koans/1"
                      className="block w-full bg-gray-800 hover:bg-gray-700 text-white font-semibold px-6 py-3 rounded-lg text-center transition-colors"
                    >
                      Continue Learning →
                    </Link>
                  )}
                </div>
              );
            })}
          </div>

          {/* Future Tracks Teaser */}
          <div className="mt-12 p-8 bg-gray-900 border border-gray-800 rounded-lg text-center">
            <h3 className="text-xl font-bold mb-2">More Tracks Coming Soon</h3>
            <p className="text-gray-400">
              Additional learning tracks and badges will be added in the future.
            </p>
          </div>
        </div>
      </div>
    </>
  );
}
