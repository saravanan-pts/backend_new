/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Enable standalone output for Docker deployment
  output: 'standalone',
  // Mark packages that should not be bundled for server components
  serverComponentsExternalPackages: ['surrealdb.js'],
  webpack: (config, { isServer }) => {
    // Only allow Node.js modules on the server side
    if (!isServer) {
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        path: false,
        crypto: false,
        net: false,
        tls: false,
        ws: false,
        bufferutil: false,
        'utf-8-validate': false,
      };
    } else {
      // For server-side, externalize native dependencies that ws uses
      // These need to be loaded as actual Node.js modules, not bundled
      config.externals = config.externals || [];
      
      // Externalize native dependencies - they must be loaded at runtime
      // This prevents webpack from trying to bundle them incorrectly
      config.externals.push(function ({ request }, callback) {
        if (request === 'bufferutil' || request === 'utf-8-validate') {
          return callback(null, `commonjs ${request}`);
        }
        callback();
      });
    }
    return config;
  },
};

module.exports = nextConfig;
