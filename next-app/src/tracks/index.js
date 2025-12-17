/**
 * Learning tracks configuration
 * Each track has its own completion requirements and badge
 */

export const tracks = {
  'pyspark-fundamentals': {
    id: 'pyspark-fundamentals',
    name: 'PySpark Fundamentals',
    description: 'Master PySpark core concepts and operations',
    // Include all koans except Delta Lake (101-110)
    requiredKoanIds: [
      1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
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
    // All koans including Delta Lake
    requiredKoanIds: [
      1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      101, 102, 103, 104, 105, 106, 107, 108, 109, 110
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

  return track.requiredKoanIds.every(koanId => completedKoanIds.has(koanId));
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

  const completed = track.requiredKoanIds.filter(id => completedKoanIds.has(id)).length;
  const total = track.requiredKoanIds.length;
  const percentage = Math.round((completed / total) * 100);

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
