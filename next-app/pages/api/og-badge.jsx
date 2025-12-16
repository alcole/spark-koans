import { readFileSync } from 'fs';
import { join } from 'path';
import { Resvg } from '@resvg/resvg-js';

export const config = {
  runtime: 'nodejs',
};

export default async function handler(req, res) {
  try {
    // Read the SVG badge file
    const svgPath = join(process.cwd(), 'public', 'assets', 'badge.svg');
    const badgeSvg = readFileSync(svgPath, 'utf-8');

    // Create an OG image (1200x630) with the badge centered
    // Badge will be scaled to fit nicely within the frame
    const ogImage = `
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">
        <defs>
          <!-- Background gradient -->
          <linearGradient id="bgGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#1e293b;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#0f172a;stop-opacity:1" />
          </linearGradient>
        </defs>

        <!-- Background -->
        <rect width="1200" height="630" fill="url(#bgGradient)"/>

        <!-- Centered badge - scaled to 500px height to fit nicely -->
        <g transform="translate(600, 315)">
          <g transform="scale(1.04) translate(-200, -240)">
            ${badgeSvg.replace(/<\?xml[^>]*\?>/, '').replace(/<svg[^>]*>/, '').replace(/<\/svg>/, '')}
          </g>
        </g>

        <!-- Optional: Add subtle text at bottom -->
        <text x="600" y="605"
              font-family="Arial, Helvetica, sans-serif"
              font-size="14"
              fill="#6b7280"
              text-anchor="middle"
              opacity="0.6">spark-koans.vercel.app</text>
      </svg>
    `;

    // Convert the composed SVG to PNG
    const resvg = new Resvg(ogImage, {
      fitTo: {
        mode: 'width',
        value: 1200,
      },
    });

    const pngData = resvg.render();
    const pngBuffer = pngData.asPng();

    // Set headers for PNG image
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, immutable, no-transform, max-age=31536000');

    res.send(pngBuffer);
  } catch (error) {
    console.error('Error generating badge:', error);
    res.status(500).json({ error: 'Failed to generate badge image', details: error.message });
  }
}
