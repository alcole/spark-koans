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
    let badgeSvg = readFileSync(svgPath, 'utf-8');

    // Rename gradient/filter IDs in the badge to avoid conflicts
    badgeSvg = badgeSvg
      .replace(/id="borderGradient"/g, 'id="badge-borderGradient"')
      .replace(/url\(#borderGradient\)/g, 'url(#badge-borderGradient)')
      .replace(/id="bgGradient"/g, 'id="badge-bgGradient"')
      .replace(/url\(#bgGradient\)/g, 'url(#badge-bgGradient)')
      .replace(/id="innerShadow"/g, 'id="badge-innerShadow"')
      .replace(/url\(#innerShadow\)/g, 'url(#badge-innerShadow)')
      .replace(/id="dropShadow"/g, 'id="badge-dropShadow"')
      .replace(/url\(#dropShadow\)/g, 'url(#badge-dropShadow)');

    // Extract badge content (remove xml declaration and outer svg tags)
    const badgeContent = badgeSvg
      .replace(/<\?xml[^>]*\?>/, '')
      .replace(/<svg[^>]*>/, '')
      .replace(/<\/svg>/, '');

    // Create an OG image (1200x630) with the badge centered
    const ogImage = `
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">
        <defs>
          <!-- Background gradient for canvas -->
          <linearGradient id="canvas-bgGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#1e293b;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#0f172a;stop-opacity:1" />
          </linearGradient>
        </defs>

        <!-- Background -->
        <rect width="1200" height="630" fill="url(#canvas-bgGradient)"/>

        <!-- Centered badge - scaled larger for readable text -->
        <g transform="translate(600, 315)">
          <g transform="scale(1.25) translate(-200, -240)">
            ${badgeContent}
          </g>
        </g>

        <!-- Branding text at bottom -->
        <text x="600" y="605"
              font-family="Arial, Helvetica, sans-serif"
              font-size="14"
              fill="#6b7280"
              text-anchor="middle"
              opacity="0.6">spark-koans.vercel.app</text>
      </svg>
    `;

    // Convert the composed SVG to PNG with font configuration
    const resvg = new Resvg(ogImage, {
      fitTo: {
        mode: 'width',
        value: 1200,
      },
      font: {
        loadSystemFonts: true, // Load system fonts
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
