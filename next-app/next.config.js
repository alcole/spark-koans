/** @type {import('next').NextConfig} */
const nextConfig = {
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
