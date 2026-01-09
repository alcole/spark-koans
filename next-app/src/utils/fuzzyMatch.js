/**
 * Fuzzy Matching Utility
 * Provides string similarity matching using Levenshtein distance
 */

/**
 * Calculate Levenshtein distance between two strings
 * @param {string} str1 - First string
 * @param {string} str2 - Second string
 * @returns {number} - Edit distance
 */
function levenshteinDistance(str1, str2) {
  const len1 = str1.length;
  const len2 = str2.length;
  const matrix = [];

  // Initialize matrix
  for (let i = 0; i <= len1; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= len2; j++) {
    matrix[0][j] = j;
  }

  // Fill matrix
  for (let i = 1; i <= len1; i++) {
    for (let j = 1; j <= len2; j++) {
      const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,      // deletion
        matrix[i][j - 1] + 1,      // insertion
        matrix[i - 1][j - 1] + cost // substitution
      );
    }
  }

  return matrix[len1][len2];
}

/**
 * Calculate similarity score between two strings (0-1, where 1 is identical)
 * @param {string} str1 - First string
 * @param {string} str2 - Second string
 * @returns {number} - Similarity score
 */
function similarityScore(str1, str2) {
  const distance = levenshteinDistance(str1.toLowerCase(), str2.toLowerCase());
  const maxLen = Math.max(str1.length, str2.length);
  return maxLen === 0 ? 1 : 1 - distance / maxLen;
}

/**
 * Find the closest match from a list of candidates
 * @param {string} target - The string to match
 * @param {string[]} candidates - Array of possible matches
 * @param {number} threshold - Minimum similarity score (0-1, default 0.5)
 * @returns {string|null} - Best match or null if no good match found
 */
export function findClosestMatch(target, candidates, threshold = 0.5) {
  if (!target || !candidates || candidates.length === 0) {
    return null;
  }

  let bestMatch = null;
  let bestScore = threshold;

  for (const candidate of candidates) {
    const score = similarityScore(target, candidate);
    if (score > bestScore) {
      bestScore = score;
      bestMatch = candidate;
    }
  }

  return bestMatch;
}

/**
 * Find multiple close matches from a list of candidates
 * @param {string} target - The string to match
 * @param {string[]} candidates - Array of possible matches
 * @param {number} maxResults - Maximum number of results to return
 * @param {number} threshold - Minimum similarity score (0-1, default 0.4)
 * @returns {Array<{match: string, score: number}>} - Array of matches with scores
 */
export function findClosestMatches(target, candidates, maxResults = 3, threshold = 0.4) {
  if (!target || !candidates || candidates.length === 0) {
    return [];
  }

  const matches = candidates
    .map(candidate => ({
      match: candidate,
      score: similarityScore(target, candidate)
    }))
    .filter(item => item.score >= threshold)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults);

  return matches;
}
