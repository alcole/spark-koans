import { readFileSync } from 'fs';
import { join } from 'path';

export const config = {
  runtime: 'nodejs',
};

export default async function handler(req, res) {
  try {
    // Serve the pre-rendered PNG badge
    const pngPath = join(process.cwd(), 'public', 'assets', 'badge.png');
    const pngBuffer = readFileSync(pngPath);

    // Set headers for PNG image
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, immutable, no-transform, max-age=31536000');

    res.send(pngBuffer);
  } catch (error) {
    console.error('Error serving badge:', error);
    res.status(500).json({ error: 'Failed to serve badge image' });
  }
}
