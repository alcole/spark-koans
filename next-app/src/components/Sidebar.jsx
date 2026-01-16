/**
 * Sidebar Navigation Component
 */

import { useState } from 'react';
import Link from 'next/link';
import { getAllKoanIds, getKoan, getAllCategories, getKoansByCategory } from '../koans';

export default function Sidebar({ currentKoanId, progress, onKoanSelect }) {
  const koanIds = getAllKoanIds();
  const categories = getAllCategories();
  const [expandedCategories, setExpandedCategories] = useState(
    // All categories expanded by default
    Object.fromEntries(categories.map(cat => [cat, true]))
  );

  const toggleCategory = (category) => {
    setExpandedCategories(prev => ({
      ...prev,
      [category]: !prev[category]
    }));
  };

  return (
    <div className="w-72 min-h-screen bg-gray-900 border-r border-gray-800 p-4 overflow-y-auto flex flex-col">
      <div className="mb-6">
        <Link href="/" className="block hover:opacity-80 transition-opacity">
          <h1 className="text-2xl font-bold text-orange-500 mb-1 cursor-pointer">PySpark Koans</h1>
        </Link>
        <p className="text-gray-500 text-sm">Learn by fixing tests</p>
      </div>

      <div className="mb-4">
        <div className="flex justify-between text-sm text-gray-500 mb-1">
          <span>Progress</span>
          <span>{progress.size || 0}/{koanIds.length}</span>
        </div>
        <div className="w-full bg-gray-800 rounded-full h-2">
          <div
            className="bg-orange-600 h-2 rounded-full transition-all"
            style={{ width: `${((progress.size || 0) / koanIds.length) * 100}%` }}
          />
        </div>
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
        <p>© 2025-2026 Alex Cole. All Rights Reserved.</p>
        <p className="mt-1">Spark Koans is an independent community tool.</p>
      </div>
    </div>
  );
}
