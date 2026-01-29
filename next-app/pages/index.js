/**
 * Coming Soon Landing Page for spark-koans.com
 */

import Head from 'next/head';
import Link from 'next/link';

export default function ComingSoon() {
  return (
    <>
      <Head>
        <title>PySpark Koans - Coming Soon</title>
        <meta name="description" content="Master PySpark and Delta Lake through interactive exercises. Coming soon to spark-koans.com" />
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
        <div className="max-w-4xl w-full text-center">
          {/* Badge Image */}
          <div className="flex justify-center mb-8 animate-fadeIn">
            <div className="animate-pulse-glow rounded-full p-4">
              <img
                src="/assets/badges/pyspark-fundamentals.png"
                alt="PySpark Koans Badge"
                className="w-64 h-auto drop-shadow-2xl"
              />
            </div>
          </div>

          {/* Main Heading */}
          <h1 className="text-6xl md:text-7xl font-bold text-orange-500 mb-6 animate-fadeIn delay-200">
            PySpark Koans
          </h1>

          {/* Subheading */}
          <p className="text-2xl md:text-3xl text-gray-300 mb-4 animate-fadeIn delay-400">
            Master PySpark Through Practice
          </p>

          {/* Description */}
          <p className="text-lg text-gray-400 mb-12 max-w-2xl mx-auto animate-fadeIn delay-600">
            Interactive exercises to learn PySpark and Delta Lake.
            Complete koans, earn achievement badges, and become a data engineering expert.
          </p>

          {/* Coming Soon Badge */}
          <div className="inline-block bg-orange-600 text-white px-8 py-4 rounded-lg text-xl font-semibold mb-8 animate-fadeIn delay-800">
            Coming Soon
          </div>

          {/* Documentation Link */}
          <div className="mb-12 animate-fadeIn delay-800">
            <Link href="/docs" className="inline-flex items-center px-6 py-3 border border-gray-700 text-base font-medium rounded-lg text-gray-300 bg-gray-900 hover:bg-gray-800 hover:border-orange-500 transition-colors">
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
              </svg>
              Documentation Links
            </Link>
          </div>

          {/* Features Grid */}
          <div className="grid md:grid-cols-3 gap-8 mt-16 animate-fadeIn delay-800">
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 hover:border-orange-500 transition-colors">
              <div className="text-4xl mb-4">🎯</div>
              <h3 className="text-xl font-semibold mb-2 text-orange-400">Interactive Learning</h3>
              <p className="text-gray-400">Hands-on exercises that teach by doing</p>
            </div>

            <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 hover:border-orange-500 transition-colors">
              <div className="text-4xl mb-4">🏆</div>
              <h3 className="text-xl font-semibold mb-2 text-orange-400">Achievement Badges</h3>
              <p className="text-gray-400">Earn badges as you complete tracks</p>
            </div>

            <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 hover:border-orange-500 transition-colors">
              <div className="text-4xl mb-4">📊</div>
              <h3 className="text-xl font-semibold mb-2 text-orange-400">Track Your Progress</h3>
              <p className="text-gray-400">Multiple learning paths for all levels</p>
            </div>
          </div>

          {/* Footer */}
          <div className="mt-16 text-gray-500 text-sm">
            <p>Preparing an amazing learning experience for you</p>
          </div>

          {/* Copyright Notice */}
          <footer className="mt-8 py-6 border-t border-gray-800 text-center text-xs text-gray-500">
            <p>© 2025-2026 Alex Cole. All Rights Reserved.</p>
            <p className="mt-1">Spark Koans is an independent community tool.</p>
            <p className="mt-2">
              <Link href="/about" className="text-orange-500 hover:text-orange-400 transition-colors">
                Learn More
              </Link>
            </p>
          </footer>
        </div>
      </div>
    </>
  );
}
