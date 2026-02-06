/**
 * Landing Page for spark-koans.com
 */

import Head from 'next/head';
import Link from 'next/link';
import { TRACKS, getTrackStats } from '../src/koans';

export default function Home() {
  const standardStats = getTrackStats('standard');
  const advancedStats = getTrackStats('advanced');

  return (
    <>
      <Head>
        <title>PySpark Koans - Master PySpark Through Practice</title>
        <meta name="description" content="Master PySpark and Delta Lake through interactive exercises. Learn by doing with hands-on koans." />
        <meta property="og:site_name" content="PySpark Koans" />
        <meta property="og:title" content="PySpark Koans - Master PySpark Through Practice" />
        <meta property="og:description" content="Master PySpark and Delta Lake through interactive exercises. Learn by doing with hands-on koans." />
        <meta property="og:type" content="website" />
        <meta property="og:url" content="https://spark-koans.com/" />
        <meta property="og:image" content="https://spark-koans.com/api/og-homepage" />
        <meta property="og:image:width" content="1200" />
        <meta property="og:image:height" content="630" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="PySpark Koans - Master PySpark Through Practice" />
        <meta name="twitter:description" content="Master PySpark and Delta Lake through interactive exercises. Learn by doing with hands-on koans." />
        <meta name="twitter:image" content="https://spark-koans.com/api/og-homepage" />
        <link rel="canonical" href="https://spark-koans.com/" />
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify({
          "@context": "https://schema.org",
          "@type": "WebSite",
          "name": "PySpark Koans",
          "url": "https://spark-koans.com",
          "description": "Master PySpark and Delta Lake through interactive exercises. Learn by doing with hands-on koans.",
          "publisher": {
            "@type": "Person",
            "name": "Alex Cole"
          }
        }) }} />
      </Head>

      <style jsx>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: translateY(0); }
        }

        @keyframes pulse-glow {
          0%, 100% { box-shadow: 0 0 40px rgba(249, 115, 22, 0.3); }
          50% { box-shadow: 0 0 60px rgba(249, 115, 22, 0.5); }
        }

        .animate-fadeIn {
          animation: fadeIn 1s ease-out forwards;
        }

        .animate-pulse-glow {
          animation: pulse-glow 3s ease-in-out infinite;
        }

        .delay-200 { animation-delay: 0.2s; opacity: 0; }
        .delay-400 { animation-delay: 0.4s; opacity: 0; }
        .delay-600 { animation-delay: 0.6s; opacity: 0; }
        .delay-800 { animation-delay: 0.8s; opacity: 0; }
      `}</style>

      <div className="bg-gray-950 text-gray-100 min-h-screen flex items-center justify-center p-6">
        <div className="max-w-5xl w-full text-center">
          {/* Main Heading */}
          <h1 className="text-6xl md:text-7xl font-bold text-orange-500 mb-6 animate-fadeIn">
            PySpark Koans
          </h1>

          {/* Subheading */}
          <p className="text-2xl md:text-3xl text-gray-300 mb-4 animate-fadeIn delay-200">
            Master PySpark Through Practice
          </p>

          {/* Description */}
          <p className="text-lg text-gray-400 mb-12 max-w-2xl mx-auto animate-fadeIn delay-400">
            Interactive exercises to learn PySpark and Delta Lake.
            Choose a learning track, complete koans, and earn achievement badges.
          </p>

          {/* Track Selection */}
          <div className="grid md:grid-cols-2 gap-8 mb-12 animate-fadeIn delay-600">
            {/* Standard Track */}
            <Link href="/koans/1" className="group block bg-gray-900 border-2 border-gray-800 rounded-xl p-8 hover:border-orange-500 transition-all hover:shadow-lg hover:shadow-orange-500/10">
              <div className="flex justify-center mb-6">
                <div className="group-hover:animate-pulse-glow rounded-full p-2">
                  <img
                    src={TRACKS.standard.badge}
                    alt="PySpark Fundamentals Badge"
                    className="w-40 h-auto drop-shadow-xl"
                  />
                </div>
              </div>
              <h2 className="text-2xl font-bold text-orange-400 mb-2">{TRACKS.standard.name}</h2>
              <p className="text-gray-400 mb-4">{TRACKS.standard.description}</p>
              <div className="text-sm text-gray-500 mb-4">
                {standardStats.total} koans &middot; {Object.keys(standardStats.byCategory).length} categories
              </div>
              <div className="flex flex-wrap justify-center gap-2 mb-6">
                {Object.keys(standardStats.byCategory).map(cat => (
                  <span key={cat} className="text-xs bg-gray-800 text-gray-400 px-2 py-1 rounded">
                    {cat}
                  </span>
                ))}
              </div>
              <span className="inline-block bg-orange-600 group-hover:bg-orange-500 text-white px-6 py-3 rounded-lg font-semibold transition-colors">
                Start Fundamentals
              </span>
            </Link>

            {/* Advanced Track */}
            <Link href="/koans/201" className="group block bg-gray-900 border-2 border-gray-800 rounded-xl p-8 hover:border-purple-500 transition-all hover:shadow-lg hover:shadow-purple-500/10">
              <div className="flex justify-center mb-6">
                <div className="rounded-full p-2">
                  <img
                    src={TRACKS.advanced.badge}
                    alt="PySpark Advanced Badge"
                    className="w-40 h-auto drop-shadow-xl"
                  />
                </div>
              </div>
              <h2 className="text-2xl font-bold text-purple-400 mb-2">{TRACKS.advanced.name}</h2>
              <p className="text-gray-400 mb-4">{TRACKS.advanced.description}</p>
              <div className="text-sm text-gray-500 mb-4">
                {advancedStats.total} koans &middot; {Object.keys(advancedStats.byCategory).length} categories
              </div>
              <div className="flex flex-wrap justify-center gap-2 mb-6">
                {Object.keys(advancedStats.byCategory).map(cat => (
                  <span key={cat} className="text-xs bg-gray-800 text-gray-400 px-2 py-1 rounded">
                    {cat}
                  </span>
                ))}
              </div>
              <span className="inline-block bg-purple-600 group-hover:bg-purple-500 text-white px-6 py-3 rounded-lg font-semibold transition-colors">
                Start Advanced
              </span>
            </Link>
          </div>

          {/* Secondary Links */}
          <div className="flex justify-center gap-4 mb-12 animate-fadeIn delay-800">
            <Link href="/docs" className="inline-flex items-center px-6 py-3 border border-gray-700 text-base font-medium rounded-lg text-gray-300 bg-gray-900 hover:bg-gray-800 hover:border-orange-500 transition-colors">
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
              </svg>
              Documentation
            </Link>
            <Link href="/about" className="inline-flex items-center px-6 py-3 border border-gray-700 text-base font-medium rounded-lg text-gray-300 bg-gray-900 hover:bg-gray-800 hover:border-orange-500 transition-colors">
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              About
            </Link>
          </div>

          {/* Copyright Notice */}
          <footer className="mt-8 py-6 border-t border-gray-800 text-center text-xs text-gray-500">
            <p>&copy; 2025-2026 Alex Cole. All Rights Reserved.</p>
            <p className="mt-1">Spark Koans is an independent community learning tool.</p>
          </footer>
        </div>
      </div>
    </>
  );
}
