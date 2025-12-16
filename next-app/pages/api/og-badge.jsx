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

    // For testing: Try rendering the badge directly to see if text appears
    // If this works, the issue is with composition. If not, it's a font/text rendering issue.

    // Convert directly - badge is 400x480, scale to fit 1200x630 with padding
    const resvg = new Resvg(badgeSvg, {
      fitTo: {
        mode: 'width',
        value: 500, // Reasonable size to fit in OG image
      },
      font: {
        loadSystemFonts: true,
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
