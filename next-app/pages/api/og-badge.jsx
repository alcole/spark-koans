import { readFileSync } from 'fs';
import { join } from 'path';
import { Resvg } from '@resvg/resvg-js';

export const config = {
  runtime: 'nodejs',
};

export default async function handler(req, res) {
  try {
    // Read the SVG file
    const svgPath = join(process.cwd(), 'public', 'assets', 'badge.svg');
    const svg = readFileSync(svgPath, 'utf-8');

    // Convert SVG to PNG using resvg
    const resvg = new Resvg(svg, {
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
    res.status(500).json({ error: 'Failed to generate badge image' });
  }
}
