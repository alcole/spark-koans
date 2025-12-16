/**
 * Pyodide Hook
 * Manages Pyodide initialization and shim loading
 */

import { useState, useEffect } from 'react';

// Import shims as raw strings (will need to set up proper loading)
// For now, this is a placeholder - actual shim loading will be done via fetch
const PYSPARK_SHIM_URL = '/shims/pyspark-shim.py';
const DELTA_SHIM_URL = '/shims/delta-shim.py';

export default function usePyodide() {
  const [pyodide, setPyodide] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [shimsLoaded, setShimsLoaded] = useState({
    pyspark: false,
    delta: false,
  });

  useEffect(() => {
    async function initPyodide() {
      setIsLoading(true);
      try {
        // Load Pyodide
        const pyodideInstance = await window.loadPyodide({
          indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/'
        });

        // Load pandas
        await pyodideInstance.loadPackage(['pandas']);

        // Load PySpark shim
        const pysparkShimResponse = await fetch(PYSPARK_SHIM_URL);
        const pysparkShim = await pysparkShimResponse.text();
        await pyodideInstance.runPythonAsync(pysparkShim);

        setShimsLoaded(prev => ({ ...prev, pyspark: true }));
        setPyodide(pyodideInstance);

        console.log('✓ Pyodide initialized with PySpark shim');
      } catch (err) {
        console.error('Failed to initialize Pyodide:', err);
        setError(err);
      }
      setIsLoading(false);
    }

    // Only run in browser
    if (typeof window !== 'undefined') {
      if (!window.loadPyodide) {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js';
        script.onload = initPyodide;
        script.onerror = () => setError(new Error('Failed to load Pyodide script'));
        document.head.appendChild(script);
      } else {
        initPyodide();
      }
    }
  }, []);

  /**
   * Load Delta Lake shim on demand
   */
  const loadDeltaShim = async () => {
    if (!pyodide || shimsLoaded.delta) return;

    try {
      const deltaShimResponse = await fetch(DELTA_SHIM_URL);
      const deltaShim = await deltaShimResponse.text();
      await pyodide.runPythonAsync(deltaShim);
      setShimsLoaded(prev => ({ ...prev, delta: true }));
      console.log('✓ Delta Lake shim loaded');
    } catch (err) {
      console.error('Failed to load Delta shim:', err);
      throw err;
    }
  };

  return {
    pyodide,
    isLoading,
    error,
    shimsLoaded,
    loadDeltaShim,
  };
}
