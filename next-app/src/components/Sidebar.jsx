/**
 * Sidebar Navigation Component
 */

import { useState } from 'react';
import Link from 'next/link';
import { getAllKoanIds, getKoan, getAllCategories, getKoansByCategory, getTrackForKoan, getKoanIdsByTrack, getCategoriesByTrack, TRACKS } from '../koans';

export default function Sidebar({ currentKoanId, progress, onKoanSelect }) {
  const track = getTrackForKoan(currentKoanId);
  const trackDef = TRACKS[track];
  const koanIds = getKoanIdsByTrack(track);
  const categories = getCategoriesByTrack(track);
  const trackProgress = koanIds.filter(id => progress.has(id)).length;
  const [expandedCategories, setExpandedCategories] = useState(
    // All categories expanded by default
    Object.fromEntries(categories.map(cat => [cat, true]))
  );

  const isAdvanced = track === 'advanced';
  const accentColor = isAdvanced ? 'purple' : 'orange';

  const toggleCategory = (category) => {
    setExpandedCategories(prev => ({
      ...prev,
      [category]: !prev[category]
    }));
  };

  return (
    <div className="w-72 h-screen bg-gray-900 border-r border-gray-800 p-4 overflow-y-auto flex flex-col">
      <div className="mb-6">
        <Link href="/" className="block hover:opacity-80 transition-opacity">
          <h1 className={`text-2xl font-bold mb-1 cursor-pointer ${isAdvanced ? 'text-purple-500' : 'text-orange-500'}`}>PySpark Koans</h1>
        </Link>
        <p className={`text-sm font-medium ${isAdvanced ? 'text-purple-400' : 'text-orange-400'}`}>
          {trackDef.name}
        </p>
      </div>

      {/* Track Switcher */}
      <div className="mb-4 flex gap-2">
        <button
          onClick={() => onKoanSelect(TRACKS.standard.startKoan)}
          className={`flex-1 text-xs py-1.5 rounded-lg transition-colors ${
            !isAdvanced
              ? 'bg-orange-600 text-white'
              : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
          }`}
        >
          Fundamentals
        </button>
        <button
          onClick={() => onKoanSelect(TRACKS.advanced.startKoan)}
          className={`flex-1 text-xs py-1.5 rounded-lg transition-colors ${
            isAdvanced
              ? 'bg-purple-600 text-white'
              : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
          }`}
        >
          Advanced
        </button>
      </div>

      <div className="mb-4">
        <div className="flex justify-between text-sm text-gray-500 mb-1">
          <span>Progress</span>
          <span>{trackProgress}/{koanIds.length}</span>
        </div>
        <div className="w-full bg-gray-800 rounded-full h-2">
          <div
            className={`h-2 rounded-full transition-all ${isAdvanced ? 'bg-purple-600' : 'bg-orange-600'}`}
            style={{ width: `${(trackProgress / koanIds.length) * 100}%` }}
          />
        </div>
      </div>

      {/* Documentation Link */}
      <div className="mb-4">
        <Link href="/docs" className="flex items-center px-3 py-2 text-gray-300 hover:bg-gray-800 hover:text-white rounded-lg transition-colors">
          <svg className="w-5 h-5 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
          </svg>
          Documentation
        </Link>
      </div>

      <div className="space-y-2">
        {categories.map(category => {
          const categoryKoans = getKoansByCategory(category);
          const isExpanded = expandedCategories[category];
          const completedInCategory = categoryKoans.filter(k => progress.has(k.id)).length;

          return (
            <div key={category} className="border-b border-gray-800 pb-2">
              <button
                onClick={() => toggleCategory(category)}
                className="w-full text-left px-2 py-2 flex items-center justify-between text-sm font-medium text-gray-300 hover:text-white transition-colors"
              >
                <span className="flex items-center gap-2">
                  <span className={`transform transition-transform ${isExpanded ? 'rotate-90' : ''}`}>
                    ▶
                  </span>
                  {category}
                </span>
                <span className="text-xs text-gray-500">
                  {completedInCategory}/{categoryKoans.length}
                </span>
              </button>

              {isExpanded && (
                <div className="mt-1 space-y-1 ml-2">
                  {categoryKoans.map(koan => {
                    const isComplete = progress.has(koan.id);
                    const isCurrent = koan.id === currentKoanId;

                    return (
                      <button
                        key={koan.id}
                        onClick={() => onKoanSelect(koan.id)}
                        className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors flex items-center gap-2 ${
                          isCurrent
                            ? 'bg-gray-800 text-white'
                            : 'text-gray-400 hover:bg-gray-800/50'
                        }`}
                      >
                        <span
                          className={`w-5 h-5 rounded flex items-center justify-center text-xs ${
                            isComplete
                              ? 'bg-green-600 text-white'
                              : 'bg-gray-700 text-gray-400'
                          }`}
                        >
                          {isComplete ? '✓' : koan.id}
                        </span>
                        <span className="truncate">{koan.title}</span>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Copyright Notice */}
      <div className="mt-auto pt-4 border-t border-gray-800 text-sm text-gray-500 text-center">
        <p>&copy; 2025-2026 Alex Cole. All Rights Reserved.</p>
        <p className="mt-1">Spark Koans is an independent community tool.</p>
      </div>
    </div>
  );
}
