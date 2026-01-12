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
              <h2 className="text-2xl font-semibold text-white mb-3">Learning Paths</h2>
              <div className="grid md:grid-cols-2 gap-6">
                <div className="bg-gray-900 border border-gray-800 rounded-lg p-6">
                  <h3 className="text-xl font-semibold text-orange-400 mb-2">PySpark Fundamentals</h3>
                  <p className="text-gray-400">
                    Learn DataFrame basics, column operations, string functions, aggregations, joins,
                    window functions, null handling, and advanced operations.
                  </p>
                </div>
                <div className="bg-gray-900 border border-gray-800 rounded-lg p-6">
                  <h3 className="text-xl font-semibold text-orange-400 mb-2">Delta Lake</h3>
                  <p className="text-gray-400">
                    Master Delta Lake features including time travel, merge operations, optimization,
                    and transaction history.
                  </p>
                </div>
              </div>
            </section>

            <section>
              <h2 className="text-2xl font-semibold text-white mb-3">Exam Preparation</h2>
              <p className="text-lg leading-relaxed">
                Koans are tagged for Databricks certification relevance, helping you prepare for:
              </p>
              <ul className="list-disc list-inside space-y-2 text-gray-400 ml-4 mt-3">
                <li>Data Engineer Associate (DEA)</li>
                <li>Data Engineer Professional (DEP)</li>
                <li>Data Analyst Associate (DAA)</li>
                <li>Machine Learning Associate (MLA)</li>
              </ul>
            </section>
          </div>

          {/* Copyright Footer */}
          <footer className="mt-16 pt-8 border-t border-gray-800 text-center">
            <p className="text-gray-400">© 2025-2026 Alex Cole. All Rights Reserved.</p>
            <p className="text-gray-500 mt-2">Spark Koans is an independent community tool.</p>
          </footer>
        </div>
      </div>
    </>
  );
}
