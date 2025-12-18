/**
 * Learning tracks configuration
 * Each track has its own completion requirements and badge
 */

import koansById, { getKoanIdsByCategories } from '../koans';

// Cache for resolved koan IDs per track
const trackKoanIdsCache = new Map();

/**
 * Resolve track definition to actual koan IDs
 * Supports both category-based and ID-based tracks
 */
function resolveTrackKoanIds(track) {
  // Category-based tracks
  if (track.requiredCategories && track.requiredCategories.length > 0) {
    return getKoanIdsByCategories(track.requiredCategories);
  }

  // Legacy: explicit koan IDs (for backward compatibility)
  if (track.requiredKoanIds && Array.isArray(track.requiredKoanIds)) {
    return track.requiredKoanIds;
  }

  return [];
}

/**
 * Get required koan IDs for a track (with caching)
 */
function getTrackKoanIds(trackId) {
  // Return from cache if available
  if (trackKoanIdsCache.has(trackId)) {
    return trackKoanIdsCache.get(trackId);
  }

  const track = tracks[trackId];
  if (!track) return [];

  const koanIds = resolveTrackKoanIds(track);
  trackKoanIdsCache.set(trackId, koanIds);

  return koanIds;
}

export const tracks = {
  'pyspark-fundamentals': {
    id: 'pyspark-fundamentals',
    name: 'PySpark Fundamentals',
    description: 'Master PySpark core concepts and operations',
    requiredCategories: [
      'Basics',
      'Column Operations',
      'String Functions',
      'Aggregations',
      'Joins',
      'Window Functions',
      'Null Handling',
      'Advanced'
    ],
    badge: {
      title: 'PySpark Koans Master',
      image: '/assets/badges/pyspark-fundamentals.png',
      ogImage: '/api/og-badge?track=pyspark-fundamentals'
    },
    order: 1,
  },

  'pyspark-advanced': {
    id: 'pyspark-advanced',
    name: 'PySpark Advanced',
    description: 'Complete mastery including Delta Lake',
    requiredCategories: [
      'Basics',
      'Column Operations',
      'String Functions',
      'Aggregations',
      'Joins',
      'Window Functions',
      'Null Handling',
      'Advanced',
      'Delta Lake'
    ],
    badge: {
      title: 'PySpark Advanced Master',
      image: '/assets/badges/pyspark-advanced.png',
      ogImage: '/api/og-badge?track=pyspark-advanced'
    },
    order: 2,
  },
};

/**
 * Get all tracks in display order
 */
export function getAllTracks() {
  return Object.values(tracks).sort((a, b) => a.order - b.order);
}

/**
 * Get a specific track by ID
 */
export function getTrack(trackId) {
  return tracks[trackId] || null;
}

/**
 * Check if a track is completed based on user progress
 * @param {string} trackId - Track ID
 * @param {Set} completedKoanIds - Set of completed koan IDs
 * @returns {boolean}
 */
export function isTrackCompleted(trackId, completedKoanIds) {
  const track = tracks[trackId];
  if (!track) return false;

  const requiredIds = getTrackKoanIds(trackId);
  return requiredIds.every(koanId => completedKoanIds.has(koanId));
}

/**
 * Get track progress
 * @param {string} trackId - Track ID
 * @param {Set} completedKoanIds - Set of completed koan IDs
 * @returns {Object} Progress info
 */
export function getTrackProgress(trackId, completedKoanIds) {
  const track = tracks[trackId];
  if (!track) return null;

  const requiredIds = getTrackKoanIds(trackId);
  const completed = requiredIds.filter(id => completedKoanIds.has(id)).length;
  const total = requiredIds.length;
  const percentage = total > 0 ? Math.round((completed / total) * 100) : 0;

  return {
    completed,
    total,
    percentage,
    isComplete: completed === total,
  };
}

/**
 * Get all completed tracks for a user
 * @param {Set} completedKoanIds - Set of completed koan IDs
 * @returns {Array} Array of completed track IDs
 */
export function getCompletedTracks(completedKoanIds) {
  return Object.keys(tracks).filter(trackId =>
    isTrackCompleted(trackId, completedKoanIds)
  );
}
