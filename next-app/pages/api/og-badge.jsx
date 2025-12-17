import { readFileSync } from 'fs';
import { join } from 'path';
import sharp from 'sharp';

export const config = {
  runtime: 'nodejs',
};

export default async function handler(req, res) {
  try {
    // Get track from query parameter (default to pyspark-fundamentals)
    const { track = 'pyspark-fundamentals' } = req.query;

    // Read the pre-rendered PNG badge for the specified track
    const badgePath = join(process.cwd(), 'public', 'assets', 'badges', `${track}.png`);
    const badgeBuffer = readFileSync(badgePath);

    // Get badge metadata to calculate sizing
    const badgeMetadata = await sharp(badgeBuffer).metadata();

    // Target height for the badge in the OG image (adjust this to make badge larger/smaller)
    const targetHeight = 500; // Reasonable size for LinkedIn preview
    const targetWidth = Math.round((badgeMetadata.width / badgeMetadata.height) * targetHeight);

    // Resize the badge
    const resizedBadge = await sharp(badgeBuffer)
      .resize(targetWidth, targetHeight, {
        fit: 'contain',
        background: { r: 0, g: 0, b: 0, alpha: 0 }
      })
      .toBuffer();

    // Create OG image (1200x630) with gradient background
    const ogImage = await sharp({
      create: {
        width: 1200,
        height: 630,
        channels: 4,
        background: { r: 15, g: 23, b: 42, alpha: 1 } // Dark blue background
      }
    })
    .composite([
      {
        input: resizedBadge,
        // Center the badge
        left: Math.round((1200 - targetWidth) / 2),
        top: Math.round((630 - targetHeight) / 2)
      }
    ])
    .png()
    .toBuffer();

    // Set headers for PNG image
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, immutable, no-transform, max-age=31536000');

    res.send(ogImage);
  } catch (error) {
    console.error('Error generating OG badge:', error);
    res.status(500).json({ error: 'Failed to generate badge image', details: error.message });
  }
}
