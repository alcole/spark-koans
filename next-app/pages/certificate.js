/**
 * Certificate Page - Shown when all koans are completed
 */

import { useEffect, useState } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import { getKoanStats } from '../src/koans';
import useKoanProgress from '../src/hooks/useKoanProgress';

export default function Certificate() {
  const stats = getKoanStats();
  const { progress } = useKoanProgress();
  const [userName, setUserName] = useState('');
  const [showNameInput, setShowNameInput] = useState(true);
  const [completionDate, setCompletionDate] = useState('');
  const [isDownloading, setIsDownloading] = useState(false);

  useEffect(() => {
    // Load saved name from localStorage
    const savedName = localStorage.getItem('pyspark-koans-username');
    if (savedName) {
      setUserName(savedName);
      setShowNameInput(false);
    }

    // Set completion date
    const today = new Date().toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
    setCompletionDate(today);
  }, []);

  const saveName = () => {
    if (userName.trim()) {
      localStorage.setItem('pyspark-koans-username', userName.trim());
      setShowNameInput(false);
    }
  };

  const isFullyComplete = progress.size === stats.total;

  const shareText = `🎉 I just completed all ${stats.total} PySpark Koans! Master your PySpark skills through interactive exercises. #PySpark #DataEngineering #Learning`;
  const shareUrl = typeof window !== 'undefined' ? window.location.origin : '';

  // Generate OG image URL with parameters (use production URL for OG tags)
  const baseUrl = process.env.NEXT_PUBLIC_VERCEL_URL
    ? `https://${process.env.NEXT_PUBLIC_VERCEL_URL}`
    : (typeof window !== 'undefined' ? window.location.origin : 'https://your-app.vercel.app');

  const ogImageUrl = `${baseUrl}/api/og-certificate?name=${encodeURIComponent(userName || 'A PySpark Learner')}&koans=${stats.total}&date=${encodeURIComponent(completionDate || 'Recently')}`;

  const shareOnTwitter = () => {
    const url = `https://twitter.com/intent/tweet?text=${encodeURIComponent(shareText)}&url=${encodeURIComponent(shareUrl)}`;
    window.open(url, '_blank', 'width=550,height=420');
  };

  const shareOnLinkedIn = () => {
    const url = `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(shareUrl)}`;
    window.open(url, '_blank', 'width=550,height=500');
  };

  const downloadCertificate = async () => {
    setIsDownloading(true);
    try {
      const html2canvas = (await import('html2canvas')).default;
      const certificate = document.getElementById('certificate');

      if (!certificate) {
        alert('Certificate element not found');
        return;
      }

      const canvas = await html2canvas(certificate, {
        backgroundColor: '#111827',
        scale: 2, // Higher quality
        logging: false,
      });

      // Convert to blob and download
      canvas.toBlob((blob) => {
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.download = `pyspark-koans-certificate-${userName.replace(/\s+/g, '-').toLowerCase() || 'completion'}.png`;
        link.href = url;
        link.click();
        URL.revokeObjectURL(url);
      });
    } catch (error) {
      console.error('Failed to download certificate:', error);
      alert('Failed to download certificate. Please try again.');
    } finally {
      setIsDownloading(false);
    }
  };

  return (
    <>
      <Head>
        <title>PySpark Koans Certificate - {userName || 'Achievement Unlocked'}</title>
        <meta name="description" content={`Completed all ${stats.total} PySpark Koans - Master PySpark through interactive exercises`} />

        {/* Open Graph meta tags for social sharing */}
        <meta property="og:title" content={`${userName || 'I'} completed all PySpark Koans!`} />
        <meta property="og:description" content={`Successfully completed all ${stats.total} PySpark and Delta Lake exercises. Master your data engineering skills!`} />
        <meta property="og:type" content="website" />
        <meta property="og:url" content={typeof window !== 'undefined' ? window.location.href : ''} />
        <meta property="og:image" content={ogImageUrl} />
        <meta property="og:image:width" content="1200" />
        <meta property="og:image:height" content="630" />

        {/* Twitter Card meta tags */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={`${userName || 'I'} completed all PySpark Koans!`} />
        <meta name="twitter:description" content={`Successfully completed all ${stats.total} PySpark and Delta Lake exercises.`} />
        <meta name="twitter:image" content={ogImageUrl} />
      </Head>

      <div className="min-h-screen bg-gray-950 text-gray-100 py-12 px-4">
        <div className="max-w-4xl mx-auto">
          <Link href="/" className="text-orange-500 hover:text-orange-400 mb-8 inline-block">
            ← Back to Home
          </Link>

        {!isFullyComplete ? (
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-12 text-center">
            <h1 className="text-3xl font-bold mb-4">Keep Going!</h1>
            <p className="text-gray-400 mb-4">
              You've completed {progress.size} out of {stats.total} koans.
            </p>
            <p className="text-gray-400 mb-8">
              Complete all koans to earn your certificate!
            </p>
            <Link
              href="/koans/1"
              className="inline-block bg-orange-600 hover:bg-orange-700 text-white px-6 py-3 rounded-lg transition-colors"
            >
              Continue Learning
            </Link>
          </div>
        ) : (
          <>
            {/* Certificate */}
            <div
              id="certificate"
              className="bg-gradient-to-br from-gray-900 to-gray-800 border-4 border-orange-500 rounded-lg p-12 mb-8 shadow-2xl"
            >
              <div className="text-center">
                <div className="mb-6">
                  <div className="text-6xl mb-4">🎓</div>
                  <h1 className="text-4xl font-bold text-orange-500 mb-2">
                    Certificate of Achievement
                  </h1>
                  <div className="w-32 h-1 bg-orange-500 mx-auto"></div>
                </div>

                <div className="my-8">
                  <p className="text-gray-400 mb-4">This certifies that</p>
                  {showNameInput ? (
                    <div className="flex items-center justify-center gap-2 mb-4">
                      <input
                        type="text"
                        value={userName}
                        onChange={(e) => setUserName(e.target.value)}
                        placeholder="Enter your name"
                        className="px-4 py-2 bg-gray-800 border border-gray-700 rounded-lg text-white text-xl text-center focus:outline-none focus:border-orange-500"
                        onKeyPress={(e) => e.key === 'Enter' && saveName()}
                      />
                      <button
                        onClick={saveName}
                        className="px-4 py-2 bg-orange-600 hover:bg-orange-700 rounded-lg transition-colors"
                      >
                        Save
                      </button>
                    </div>
                  ) : (
                    <h2 className="text-3xl font-bold text-white mb-4">
                      {userName}
                      <button
                        onClick={() => setShowNameInput(true)}
                        className="ml-2 text-sm text-gray-500 hover:text-gray-400"
                      >
                        (edit)
                      </button>
                    </h2>
                  )}
                  <p className="text-gray-400 mb-6">has successfully completed</p>
                  <h3 className="text-2xl font-semibold text-orange-400 mb-6">
                    PySpark Koans
                  </h3>
                  <p className="text-gray-400 mb-2">
                    Mastering PySpark through {stats.total} interactive exercises
                  </p>
                  <p className="text-gray-500 text-sm">
                    Completed on {completionDate}
                  </p>
                </div>

                <div className="mt-8 pt-8 border-t border-gray-700">
                  <div className="flex justify-center items-center gap-8 text-sm text-gray-500">
                    <div>
                      <div className="text-2xl font-bold text-orange-500">{stats.total}</div>
                      <div>Koans Completed</div>
                    </div>
                    <div className="w-px h-12 bg-gray-700"></div>
                    <div>
                      <div className="text-2xl font-bold text-orange-500">
                        {Object.keys(stats.byCategory).length}
                      </div>
                      <div>Categories Mastered</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Download & Share Section */}
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-8 text-center">
              <h2 className="text-2xl font-bold mb-4">Download & Share</h2>
              <p className="text-gray-400 mb-6">
                Download your certificate or share your achievement
              </p>

              {/* Download Button */}
              <div className="mb-6">
                <button
                  onClick={downloadCertificate}
                  disabled={isDownloading}
                  className="inline-flex items-center gap-2 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-700 text-white font-semibold px-8 py-3 rounded-lg transition-colors"
                >
                  {isDownloading ? (
                    <>
                      <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Generating...
                    </>
                  ) : (
                    <>
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                      </svg>
                      Download Certificate (PNG)
                    </>
                  )}
                </button>
                <p className="text-xs text-gray-500 mt-2">
                  Download to share on LinkedIn, Twitter, or other platforms
                </p>
              </div>

              {/* Share Buttons */}
              <p className="text-sm text-gray-500 mb-4">Or share directly:</p>
              <div className="flex flex-wrap justify-center gap-4">
                <button
                  onClick={shareOnTwitter}
                  className="flex items-center gap-2 bg-blue-500 hover:bg-blue-600 text-white px-6 py-3 rounded-lg transition-colors"
                >
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M23.953 4.57a10 10 0 01-2.825.775 4.958 4.958 0 002.163-2.723c-.951.555-2.005.959-3.127 1.184a4.92 4.92 0 00-8.384 4.482C7.69 8.095 4.067 6.13 1.64 3.162a4.822 4.822 0 00-.666 2.475c0 1.71.87 3.213 2.188 4.096a4.904 4.904 0 01-2.228-.616v.06a4.923 4.923 0 003.946 4.827 4.996 4.996 0 01-2.212.085 4.936 4.936 0 004.604 3.417 9.867 9.867 0 01-6.102 2.105c-.39 0-.779-.023-1.17-.067a13.995 13.995 0 007.557 2.209c9.053 0 13.998-7.496 13.998-13.985 0-.21 0-.42-.015-.63A9.935 9.935 0 0024 4.59z"/>
                  </svg>
                  Share on Twitter
                </button>
                <button
                  onClick={shareOnLinkedIn}
                  className="flex items-center gap-2 bg-blue-700 hover:bg-blue-800 text-white px-6 py-3 rounded-lg transition-colors"
                >
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
                  </svg>
                  Share on LinkedIn
                </button>
              </div>
            </div>
          </>
        )}
        </div>
      </div>
    </>
  );
}
