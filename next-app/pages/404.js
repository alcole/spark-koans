/**
 * Custom 404 Page
 */

import Head from 'next/head';
import Link from 'next/link';

export default function Custom404() {
  return (
    <>
      <Head>
        <title>Page Not Found - PySpark Koans</title>
      </Head>

      <div className="min-h-screen bg-gray-950 text-gray-100 flex items-center justify-center p-6">
        <div className="max-w-md w-full text-center">
          <h1 className="text-9xl font-bold text-orange-500 mb-4">404</h1>
          <h2 className="text-2xl font-semibold text-gray-300 mb-4">Page Not Found</h2>
          <p className="text-gray-400 mb-8">
            Looks like this page doesn&apos;t exist. Maybe it moved or was never here.
          </p>
          <Link
            href="/"
            className="inline-block bg-orange-600 hover:bg-orange-500 text-white px-6 py-3 rounded-lg transition-colors"
          >
            Back to Home
          </Link>
        </div>
      </div>
    </>
  );
}
