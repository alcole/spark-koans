/**
 * Koan Progress Hook
 * Tracks completed koans using localStorage
 */

import { useState, useEffect } from 'react';

const STORAGE_KEY = 'pyspark-koans-progress';

export default function useKoanProgress() {
  const [progress, setProgress] = useState(new Set());

  // Load progress from localStorage on mount
  useEffect(() => {
    if (typeof window !== 'undefined') {
      try {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) {
          setProgress(new Set(JSON.parse(saved)));
        }
      } catch (err) {
        console.error('Failed to load progress:', err);
      }
    }
  }, []);

  // Save progress to localStorage whenever it changes
  useEffect(() => {
    if (typeof window !== 'undefined' && progress.size > 0) {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify([...progress]));
      } catch (err) {
        console.error('Failed to save progress:', err);
      }
    }
  }, [progress]);

  const markComplete = (koanId) => {
    setProgress(prev => new Set([...prev, koanId]));
  };

  const markIncomplete = (koanId) => {
    setProgress(prev => {
      const newSet = new Set(prev);
      newSet.delete(koanId);
      return newSet;
    });
  };

  const isComplete = (koanId) => {
    return progress.has(koanId);
  };

  const resetProgress = () => {
    setProgress(new Set());
    if (typeof window !== 'undefined') {
      localStorage.removeItem(STORAGE_KEY);
    }
  };

  const getProgress = () => {
    return {
      completed: progress.size,
      total: 100, // Update this as you add more koans
      percentage: Math.round((progress.size / 100) * 100),
    };
  };

  return {
    progress,
    markComplete,
    markIncomplete,
    isComplete,
    resetProgress,
    getProgress,
  };
}
