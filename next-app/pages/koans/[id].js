/**
 * Individual Koan Page
 * Dynamic route: /koans/1, /koans/2, etc.
 */

import Head from 'next/head';
import { useRouter } from 'next/router';
import { useState, useEffect } from 'react';
import { getKoan, getAllKoanIds, getKoanStats } from '../../src/koans';
import KoanEditor from '../../src/components/KoanEditor';
import OutputPanel from '../../src/components/OutputPanel';
import KoanHeader from '../../src/components/KoanHeader';
import HintPanel from '../../src/components/HintPanel';
import Controls from '../../src/components/Controls';
import Sidebar from '../../src/components/Sidebar';
import CompletionModal from '../../src/components/CompletionModal';
import usePyodide from '../../src/hooks/usePyodide';
import useKoanProgress from '../../src/hooks/useKoanProgress';
import { parseAndFormatError, detectUnreplacedPlaceholders } from '../../src/utils/errorParser';

const BASE_URL = 'https://spark-koans.com';

export default function KoanPage({ koan }) {
  const router = useRouter();
  const [code, setCode] = useState('');
  const [output, setOutput] = useState('');
  const [error, setError] = useState(null);
  const [showHints, setShowHints] = useState(false);
  const [currentHint, setCurrentHint] = useState(0);
  const [showSolution, setShowSolution] = useState(false);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [showCompletionModal, setShowCompletionModal] = useState(false);

  const { pyodide, isLoading, error: pyodideError, loadDeltaShim, shimsLoaded } = usePyodide();
  const { markComplete, isComplete, progress } = useKoanProgress();
  const stats = getKoanStats();

  // Initialize code when koan changes
  useEffect(() => {
    if (koan) {
      setCode(koan.template);
      setOutput('');
      setError(null);
      setShowHints(false);
      setCurrentHint(0);
      setShowSolution(false);
    }
  }, [koan]);

  const runCode = async () => {
    if (!pyodide) {
      setOutput('❌ Python environment not ready');
      setError(null);
      return;
    }

    // Check for unreplaced placeholders before running
    const placeholderCheck = detectUnreplacedPlaceholders(code);
    if (placeholderCheck.hasPlaceholders) {
      setError(placeholderCheck.error);
      setOutput('');
      return;
    }

    setOutput('Running...\\n');
    setError(null);

    try {
      // Load Delta shim for Delta Lake koans
      if (koan.category === 'Delta Lake' && !shimsLoaded.delta) {
        await loadDeltaShim();
      }

      // Run setup code
      await pyodide.runPythonAsync(koan.setup);

      // Capture stdout
      await pyodide.runPythonAsync(`
import sys
from io import StringIO
_stdout_capture = StringIO()
sys.stdout = _stdout_capture
`);

      // Run user code
      await pyodide.runPythonAsync(code);

      // Get captured output
      const capturedOutput = await pyodide.runPythonAsync(`
sys.stdout = sys.__stdout__
_stdout_capture.getvalue()
`);

      setOutput(capturedOutput);
      setError(null);

      // Check if koan is complete
      if (capturedOutput.includes('🎉 Koan complete!')) {
        const wasAlreadyComplete = isComplete(koan.id);
        markComplete(koan.id);

        // Check if this completion means ALL koans are now complete
        if (!wasAlreadyComplete) {
          // Need to check with the new progress count
          setTimeout(() => {
            const newCompletedCount = progress.size + 1;
            if (newCompletedCount === stats.total) {
              setShowCompletionModal(true);
            }
          }, 1000); // Small delay for celebration effect
        }
      }
    } catch (err) {
      // Parse and format the error
      const formattedError = parseAndFormatError(err, { koan });
      setError(formattedError);
      setOutput('');
    }
  };

  const nextHint = () => {
    if (!showHints) {
      // First click: just show hints, don't increment
      setShowHints(true);
    } else if (currentHint < koan.hints.length - 1) {
      // Subsequent clicks: increment to next hint
      setCurrentHint(prev => prev + 1);
    } else {
      // At last hint: cycle back to first hint
      setCurrentHint(0);
    }
  };

  const resetCode = () => {
    setCode(koan.template);
    setOutput('');
    setError(null);
  };

  const navigateToKoan = (newId) => {
    router.push(`/koans/${newId}`);
  };

  // Get next/previous koan IDs
  const allIds = getAllKoanIds();
  const currentIndex = allIds.indexOf(koan.id);
  const prevKoanId = currentIndex > 0 ? allIds[currentIndex - 1] : null;
  const nextKoanId = currentIndex < allIds.length - 1 ? allIds[currentIndex + 1] : null;

  const difficulty = koan.difficulty || 'beginner';
  const ogImageUrl = `${BASE_URL}/api/og-koan?${new URLSearchParams({ title: koan.title, category: koan.category, difficulty })}`;

  if (!koan) {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="text-gray-400">Koan not found</div>
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>{`${koan.title} - PySpark Koans`}</title>
        <meta name="description" content={koan.description || `An interactive ${koan.category} exercise for learning PySpark.`} />
        <meta property="og:site_name" content="PySpark Koans" />
        <meta property="og:title" content={koan.title} />
        <meta property="og:description" content={koan.description || `An interactive ${koan.category} exercise for learning PySpark.`} />
        <meta property="og:type" content="website" />
        <meta property="og:url" content={`${BASE_URL}/koans/${koan.id}`} />
        <meta property="og:image" content={ogImageUrl} />
        <meta property="og:image:width" content="1200" />
        <meta property="og:image:height" content="630" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={koan.title} />
        <meta name="twitter:description" content={koan.description || `An interactive ${koan.category} exercise for learning PySpark.`} />
        <meta name="twitter:image" content={ogImageUrl} />
        <link rel="canonical" href={`${BASE_URL}/koans/${koan.id}`} />
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify({
          "@context": "https://schema.org",
          "@type": "LearningResource",
          "name": koan.title,
          "description": koan.description || `An interactive ${koan.category} exercise for learning PySpark.`,
          "url": `${BASE_URL}/koans/${koan.id}`,
          "learningResourceType": "exercise",
          "educationalLevel": difficulty.charAt(0).toUpperCase() + difficulty.slice(1),
          "teaches": koan.category,
          "provider": {
            "@type": "Organization",
            "name": "PySpark Koans",
            "url": BASE_URL
          }
        }) }} />
      </Head>

      <div className="h-screen bg-gray-950 text-gray-100 overflow-hidden">
      {/* Completion Modal */}
      <CompletionModal
        isOpen={showCompletionModal}
        onClose={() => setShowCompletionModal(false)}
        totalKoans={stats.total}
      />

      {/* Mobile menu button */}
      <button
        onClick={() => setIsSidebarOpen(!isSidebarOpen)}
        className="lg:hidden fixed top-4 left-4 z-50 p-2 bg-gray-800 rounded-lg border border-gray-700 hover:bg-gray-700 transition-colors"
      >
        <svg
          className="w-6 h-6"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          {isSidebarOpen ? (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          ) : (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
          )}
        </svg>
      </button>

      {/* Backdrop for mobile */}
      {isSidebarOpen && (
        <div
          className="lg:hidden fixed inset-0 bg-black/50 z-30"
          onClick={() => setIsSidebarOpen(false)}
        />
      )}

      <div className="flex h-full">
        {/* Sidebar */}
        <div className={`
          fixed lg:static inset-y-0 left-0 z-40
          transform transition-transform duration-300 ease-in-out
          ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
        `}>
          <Sidebar
            currentKoanId={koan.id}
            progress={progress}
            onKoanSelect={(id) => {
              navigateToKoan(id);
              setIsSidebarOpen(false);
            }}
          />
        </div>

        {/* Main content */}
        <div className="flex-1 p-6 overflow-y-auto lg:ml-0 h-full">
          <div className="max-w-6xl mx-auto">
            <KoanHeader koan={koan} isComplete={isComplete(koan.id)} />

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mt-6">
              {/* Left column */}
              <div className="space-y-4">
                {/* Setup code (read-only) */}
                <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
                  <div className="px-4 py-2 bg-gray-800 border-b border-gray-700">
                    <span className="text-sm text-gray-400">Setup (read-only)</span>
                  </div>
                  <pre className="p-4 text-sm text-gray-400 font-mono whitespace-pre-wrap overflow-x-auto">
                    {koan.setup.trim()}
                  </pre>
                </div>

                {showHints && (
                  <HintPanel
                    hint={koan.hints[currentHint]}
                    currentHint={currentHint}
                    totalHints={koan.hints.length}
                  />
                )}

                {showSolution && (
                  <div className="bg-green-900/20 border border-green-800/50 rounded-lg p-4 overflow-hidden">
                    <h3 className="text-sm font-medium text-green-500 mb-2">Solution</h3>
                    <pre className="text-sm text-green-200/80 font-mono whitespace-pre-wrap break-all overflow-x-auto">
                      {koan.solution}
                    </pre>
                  </div>
                )}
              </div>

              {/* Right column */}
              <div className="space-y-4">
                <KoanEditor
                  code={code}
                  onChange={setCode}
                  onRun={runCode}
                  isLoading={isLoading}
                />

                <Controls
                  onRun={runCode}
                  onHint={nextHint}
                  onSolution={() => setShowSolution(!showSolution)}
                  onReset={resetCode}
                  isLoading={isLoading}
                  showHints={showHints}
                  currentHint={currentHint}
                  totalHints={koan.hints.length}
                  showingSolution={showSolution}
                />

                <OutputPanel output={output} error={error} />

                {/* Navigation */}
                <div className="flex justify-between">
                  <button
                    onClick={() => navigateToKoan(prevKoanId)}
                    disabled={!prevKoanId}
                    className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
                  >
                    ← Previous
                  </button>
                  <button
                    onClick={() => navigateToKoan(nextKoanId)}
                    disabled={!nextKoanId}
                    className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:bg-gray-900 disabled:text-gray-700 rounded-lg transition-colors"
                  >
                    Next →
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    </>
  );
}

// Generate static paths for all koans at build time
export async function getStaticPaths() {
  const ids = getAllKoanIds();
  return {
    paths: ids.map(id => ({
      params: { id: String(id) }
    })),
    fallback: false
  };
}

// Load koan data at build time
export async function getStaticProps({ params }) {
  const koan = getKoan(params.id);
  return {
    props: { koan }
  };
}
