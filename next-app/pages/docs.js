/**
 * Documentation Links Page
 */

import Link from 'next/link';
import Head from 'next/head';

export default function DocsPage() {
  const docLinks = [
    {
      title: "PySpark Getting Started Guide",
      url: "https://spark.apache.org/docs/latest/api/python/getting_started/index.html",
      description: "Official getting started guide for PySpark with examples and tutorials"
    },
    {
      title: "PySpark SQL Reference",
      url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html",
      description: "Complete API reference for PySpark SQL module including DataFrame, Column, and functions"
    },
    {
      title: "PySpark API Documentation",
      url: "https://spark.apache.org/docs/latest/api/python/index.html",
      description: "Full PySpark Python API documentation covering all modules"
    },
    {
      title: "Delta Lake Python API",
      url: "https://docs.delta.io/api/latest/python/spark/",
      description: "Delta Lake Python API documentation for data lake operations"
    }
  ];

  return (
    <>
      <Head>
        <title>Documentation Links - PySpark Koans</title>
        <meta name="description" content="Essential documentation links for PySpark and Delta Lake" />
      </Head>

      <div className="min-h-screen bg-gray-950 text-gray-100 py-8">
        <div className="max-w-4xl mx-auto px-4">
          {/* Navigation back to main site */}
          <div className="mb-8">
            <Link href="/" className="inline-flex items-center text-orange-500 hover:text-orange-400 transition-colors">
              <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
              Back to PySpark Koans
            </Link>
          </div>

          {/* Page header */}
          <div className="mb-8">
            <h1 className="text-4xl font-bold text-orange-500 mb-4">Documentation Links</h1>
            <p className="text-xl text-gray-300">
              Essential documentation and references for PySpark and Delta Lake development
            </p>
          </div>

          {/* Documentation links grid */}
          <div className="grid gap-6 md:grid-cols-2">
            {docLinks.map((link, index) => (
              <div key={index} className="bg-gray-900 border border-gray-800 rounded-lg p-6 hover:border-orange-500 transition-colors">
                <h3 className="text-xl font-semibold text-orange-400 mb-3">
                  {link.title}
                </h3>
                <p className="text-gray-400 mb-4">
                  {link.description}
                </p>
                <a
                  href={link.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center px-4 py-2 bg-orange-600 text-white rounded-md hover:bg-orange-500 transition-colors"
                >
                  Open Documentation
                  <svg className="w-4 h-4 ml-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                  </svg>
                </a>
              </div>
            ))}
          </div>

          {/* Additional resources section */}
          <div className="mt-12 bg-gray-900 border border-gray-800 rounded-lg p-6">
            <h2 className="text-2xl font-semibold text-orange-400 mb-4">Additional Resources</h2>
            <ul className="space-y-2 text-gray-400">
              <li>• <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 underline">Spark SQL Guide</a> - Learn about Spark SQL concepts and syntax</li>
              <li>• <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 underline">DataFrame API</a> - Comprehensive guide to DataFrame operations</li>
              <li>• <a href="https://docs.delta.io/latest/index.html" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 underline">Delta Lake Documentation</a> - Complete guide to Delta Lake features</li>
              <li>• <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 underline">Performance Tuning</a> - Best practices for optimizing Spark applications</li>
            </ul>
          </div>
        </div>
      </div>
    </>
  );
}
