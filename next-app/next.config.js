/** @type {import('next').NextConfig} */
const nextConfig = {
  // Security headers
  // Note: 'unsafe-eval' is required by Pyodide for WebAssembly compilation.
  // 'unsafe-inline' is required by Next.js for hydration scripts and Tailwind styles.
  headers: () => [
    {
      source: '/(.*)',
      headers: [
        {
          key: 'Content-Security-Policy',
          value: [
            "default-src 'self'",
            "script-src 'self' 'unsafe-eval' 'unsafe-inline' https://cdn.jsdelivr.net https://static.cloudflareinsights.com https://vercel.live",
            "style-src 'self' 'unsafe-inline'",
            "img-src 'self' data: blob:",
            "connect-src 'self' https://cdn.jsdelivr.net https://static.cloudflareinsights.com https://*.vercel-analytics.com https://vercel.live",
            "worker-src blob: https://cdn.jsdelivr.net",
            "frame-ancestors 'none'",
            "base-uri 'self'",
            "form-action 'self'",
          ].join('; '),
        },
        {
          key: 'X-Frame-Options',
          value: 'DENY',
        },
        {
          key: 'X-Content-Type-Options',
          value: 'nosniff',
        },
      ],
    },
  ],

  // Note: Removed 'output: export' to enable API routes for OG image generation
  // This requires deployment to a platform that supports serverless functions (e.g., Vercel)

  // Base path for GitHub Pages deployment (update if needed)
  // basePath: '/pyspark-koans',

  // Image optimization disabled for compatibility
  images: {
    unoptimized: true,
  },

  // Webpack configuration for Pyodide/WebAssembly
  webpack: (config, { isServer }) => {
    // Pyodide requires these fallbacks for browser environment
    if (!isServer) {
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        path: false,
        crypto: false,
      };
    }

    // Enable WebAssembly support
    config.experiments = {
      ...config.experiments,
      asyncWebAssembly: true,
    };

    return config;
  },

  // Strict mode for better development experience
  reactStrictMode: true,
};

module.exports = nextConfig;
