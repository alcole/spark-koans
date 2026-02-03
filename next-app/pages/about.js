/**
 * About Page
 */

import Head from 'next/head';
import Link from 'next/link';

export default function About() {
  return (
    <>
      <Head>
        <title>About - PySpark Koans</title>
        <meta name="description" content="About PySpark Koans - Learn PySpark and Delta Lake through interactive exercises" />
        <meta property="og:site_name" content="PySpark Koans" />
        <meta property="og:title" content="About - PySpark Koans" />
        <meta property="og:description" content="About PySpark Koans - Learn PySpark and Delta Lake through interactive exercises" />
        <meta property="og:type" content="website" />
        <meta property="og:url" content="https://spark-koans.vercel.app/about" />
        <meta name="twitter:card" content="summary" />
        <meta name="twitter:title" content="About - PySpark Koans" />
        <meta name="twitter:description" content="About PySpark Koans - Learn PySpark and Delta Lake through interactive exercises" />
      </Head>

      <div className="bg-gray-950 text-gray-100 min-h-screen">
        <div className="max-w-4xl mx-auto px-6 py-12">
          {/* Header */}
          <div className="mb-8">
            <Link href="/" className="inline-block text-orange-500 hover:text-orange-400 transition-colors mb-4">
              ← Back to Home
            </Link>
            <h1 className="text-5xl font-bold text-orange-500 mb-4">About PySpark Koans</h1>
          </div>

          {/* Main Content */}
          <div className="space-y-8 text-gray-300">
            <section>
              <h2 className="text-2xl font-semibold text-white mb-3">What is PySpark Koans?</h2>
              <p className="text-lg leading-relaxed">
                PySpark Koans is a browser-based, test-driven learning environment for PySpark and Delta Lake.
                It runs entirely in your browser using Pyodide (Python in WebAssembly) with a pandas-backed
                PySpark shim that emulates Spark APIs without requiring a real Spark cluster.
              </p>
            </section>

            <section>
              <h2 className="text-2xl font-semibold text-white mb-3">How It Works</h2>
              <p className="text-lg leading-relaxed mb-4">
                Each &quot;koan&quot; is a small exercise where you fill in the blanks to make tests pass.
                You&apos;ll learn PySpark concepts by fixing failing tests, guided by hints and immediate feedback.
              </p>
              <ul className="list-disc list-inside space-y-2 text-gray-400 ml-4">
                <li>No installation required - runs entirely in your browser</li>
                <li>Progressive difficulty from beginner to advanced</li>
                <li>Covers PySpark DataFrames, SQL operations, and Delta Lake</li>
                <li>Earn achievement badges as you complete learning tracks</li>
              </ul>
            </section>

            <section>
              <h2 className="text-2xl font-semibold text-white mb-3">What&apos;s Covered</h2>
              <div className="grid md:grid-cols-2 gap-6">
                <div className="bg-gray-900 border border-gray-800 rounded-lg p-6">
                  <h3 className="text-xl font-semibold text-orange-400 mb-2">PySpark</h3>
                  <p className="text-gray-400">
                    DataFrame basics, column operations, string functions, aggregations, joins,
                    window functions, null handling, and advanced operations.
                  </p>
                </div>
                <div className="bg-gray-900 border border-gray-800 rounded-lg p-6">
                  <h3 className="text-xl font-semibold text-orange-400 mb-2">Delta Lake</h3>
                  <p className="text-gray-400">
                    Delta Lake features including time travel, merge operations, optimization,
                    and transaction history.
                  </p>
                </div>
              </div>
            </section>
          </div>

          {/* Connect Section */}
          <section className="mt-12 pt-8 border-t border-gray-800">
            <h2 className="text-2xl font-semibold text-white mb-6 text-center">Connect</h2>
            <div className="flex justify-center gap-6 mb-8">
              <a href="https://www.linkedin.com/in/alexcole01/" target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-orange-400 transition-colors">
                <svg className="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
                </svg>
              </a>
              <a href="https://github.com/alcole/" target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-orange-400 transition-colors">
                <svg className="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
              </a>
              <a href="https://www.alexcole.net/" target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-orange-400 transition-colors">
                <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"/>
                </svg>
              </a>
            </div>

            <h3 className="text-lg font-semibold text-white mb-4 text-center">Other Projects</h3>
            <div className="grid md:grid-cols-2 gap-4 max-w-2xl mx-auto">
              <a href="https://databricksreleasehub.com" target="_blank" rel="noopener noreferrer" className="bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-orange-500 transition-colors block">
                <h4 className="text-orange-400 font-semibold">Databricks Release Hub</h4>
                <p className="text-gray-500 text-sm">Track Databricks platform releases</p>
              </a>
              <a href="https://www.databricksnavigator.com/" target="_blank" rel="noopener noreferrer" className="bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-orange-500 transition-colors block">
                <h4 className="text-orange-400 font-semibold">Databricks Navigator</h4>
                <p className="text-gray-500 text-sm">Navigate the Databricks ecosystem</p>
              </a>
            </div>
          </section>

          {/* Copyright Footer */}
          <footer className="mt-12 pt-8 border-t border-gray-800 text-center">
            <p className="text-gray-400">© 2025-2026 Alex Cole. All Rights Reserved.</p>
            <p className="text-gray-500 mt-2">Spark Koans is an independent community tool.</p>
          </footer>
        </div>
      </div>
    </>
  );
}
